// This source file is part of the EdgeDB open source project.
//
// Copyright 2020-present EdgeDB Inc. and the EdgeDB authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package edgedb

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/edgedb/edgedb-go/internal"
	"github.com/edgedb/edgedb-go/internal/buff"
	"github.com/edgedb/edgedb-go/internal/codecs"
	"github.com/edgedb/edgedb-go/internal/descriptor"
)

const defaultIdleConnectionTimeout = 30 * time.Second

var (
	defaultConcurrency = max(4, runtime.NumCPU())
)

func max(a, b int) int {
	if a > b {
		return a
	}

	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// Client is a connection pool and is safe for concurrent use.
type Client struct {
	pollEpoch *int64

	isClosed      *bool
	isClosedMutex *sync.RWMutex // locks isClosed

	// Guard both freeConns and potentialConns assignments.
	connectionPoolMutex *sync.Mutex

	// A buffered channel of connections that are ready for use.
	freeConns chan *transactableConn

	// A buffered channel of structs representing unconnected capacity.
	// This field remains nil until the first connection is acquired.
	potentialConns chan struct{}

	concurrency int

	txOpts    TxOptions
	retryOpts RetryOptions

	cfg *connConfig
	cacheCollection
}

// CreateClient returns a new client. The client connects lazily. Call
// Client.EnsureConnected() to force a connection.
func CreateClient(ctx context.Context, opts Options) (*Client, error) { // nolint:gocritic,lll
	return CreateClientDSN(ctx, "", opts)
}

// CreateClientDSN returns a new client. See also CreateClient.
//
// dsn is either an instance name
// https://www.edgedb.com/docs/clients/connection
// or it specifies a single string in the following format:
//
//     edgedb://user:password@host:port/database?option=value.
//
// The following options are recognized: host, port, user, database, password.
func CreateClientDSN(_ context.Context, dsn string, opts Options) (*Client, error) { // nolint:gocritic,lll
	cfg, err := parseConnectDSNAndArgs(dsn, &opts, newCfgPaths())
	if err != nil {
		return nil, err
	}

	False := false
	Zero := int64(0)
	p := &Client{
		pollEpoch:           &Zero,
		isClosed:            &False,
		isClosedMutex:       &sync.RWMutex{},
		cfg:                 cfg,
		txOpts:              NewTxOptions(),
		concurrency:         int(opts.Concurrency),
		connectionPoolMutex: &sync.Mutex{},
		retryOpts: RetryOptions{
			txConflict: RetryRule{attempts: 3, backoff: defaultBackoff},
			network:    RetryRule{attempts: 3, backoff: defaultBackoff},
		},
		cacheCollection: cacheCollection{
			serverSettings:    &cfg.serverSettings,
			typeIDCache:       &sync.Map{},
			inCodecCache:      &sync.Map{},
			outCodecCache:     &sync.Map{},
			capabilitiesCache: &sync.Map{},
		},
	}

	return p, nil
}

func (p *Client) newConn(ctx context.Context) (*transactableConn, error) {
	conn := transactableConn{
		txOpts:    p.txOpts,
		retryOpts: p.retryOpts,
		reconnectingConn: &reconnectingConn{
			cfg:             p.cfg,
			cacheCollection: p.cacheCollection,
		},
	}

	if err := conn.reconnect(ctx, false); err != nil {
		return nil, err
	}

	return &conn, nil
}

func (p *Client) connectionPoolMaintenance() {
	for !*p.isClosed {
		// Bump the epoch ahead of sleeping. p.release should use the _next_
		//  epoch in order for us to skip the connections in the first loop.
		epoch := atomic.AddInt64(p.pollEpoch, 1)
		timeout := p.serverSettings.GetIdleConnectionTimeout()

		// In the _worst case_ scenario (sudden stop of all activity 1ns
		//  before a cleanup run), the effective idle time for connections is
		//  `timeout + (min(10s, timeout / 2) - 1ns) + N*1ms`:
		// Timeout plus one idle cycle plus N times 1ms polling on idle conns.
		time.Sleep(minDuration(10*time.Second, timeout/2))

		p.processIdleConnections(epoch, timeout)
	}
}

func (p *Client) processIdleConnections(epoch int64, timeout time.Duration) {
	for i := 0; true; i++ {
		var conn *transactableConn
		select {
		case conn = <-p.freeConns:
		default:
			// Empty connection pool.
			return
		}
		if conn.pollEpoch == epoch {
			// We've seen all the idle connections.
			p.freeConns <- conn
			return
		}
		conn.pollEpoch = epoch
		if err := conn.conn.pollBackgroundMessages(); err != nil {
			log.Println("error polling idle connection:", err)
			if conn.conn.soc.Closed() {
				p.potentialConns <- struct{}{}
				continue
			}
		}
		// A timeout of 0 or less disables the idle timeout
		if timeout > 0 && conn.idleSince.Add(timeout).After(time.Now()) {
			if err := conn.Close(); err != nil {
				log.Println("error while closing idle connection:", err)
			}
			p.potentialConns <- struct{}{}
			continue
		}
		p.freeConns <- conn
	}
}

func (p *Client) acquire(ctx context.Context) (*transactableConn, error) {
	p.isClosedMutex.RLock()
	defer p.isClosedMutex.RUnlock()

	if *p.isClosed {
		return nil, &interfaceError{msg: "client closed"}
	}

	if p.potentialConns == nil {
		// Create connection pools guard under lock.
		p.connectionPoolMutex.Lock()
		if p.potentialConns == nil {
			conn, err := p.newConn(ctx)
			if err != nil {
				p.connectionPoolMutex.Unlock()
				return nil, err
			}

			if p.concurrency == 0 {
				// The user did not set Concurrency in provided Options.
				// See if the server sends a suggested max size.
				p.concurrency = conn.cfg.serverSettings.GetPoolCurrency()
			}

			p.freeConns = make(chan *transactableConn, p.concurrency)
			p.potentialConns = make(chan struct{}, p.concurrency)
			for i := 0; i < p.concurrency-1; i++ {
				p.potentialConns <- struct{}{}
			}
			go p.connectionPoolMaintenance()

			p.connectionPoolMutex.Unlock()
			return conn, nil
		}
		p.connectionPoolMutex.Unlock()
	}

	// force do nothing if context is expired
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("edgedb: %w", ctx.Err())
	default:
	}

	// force using an existing connection over connecting a new socket.
	select {
	case conn := <-p.freeConns:
		return conn, nil
	default:
	}

	select {
	case conn := <-p.freeConns:
		return conn, nil
	case <-p.potentialConns:
		conn, err := p.newConn(ctx)
		if err != nil {
			p.potentialConns <- struct{}{}
			return nil, err
		}
		return conn, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("edgedb: %w", ctx.Err())
	}
}

type systemConfig struct {
	ID                 UUID     `edgedb:"id"`
	SessionIdleTimeout Duration `edgedb:"session_idle_timeout"`
}

func parseSystemConfig(
	b []byte,
	version internal.ProtocolVersion,
) (systemConfig, error) {
	r := buff.SimpleReader(b)
	u := r.PopSlice(r.PopUint32())
	u.PopUUID()
	dsc, err := descriptor.Pop(u, version)
	if err != nil {
		return systemConfig{}, err
	}

	var cfg systemConfig
	typ := reflect.TypeOf(cfg)
	dec, err := codecs.BuildDecoder(dsc, typ, codecs.Path("system_config"))
	if err != nil {
		return systemConfig{}, err
	}

	err = dec.Decode(r.PopSlice(r.PopUint32()), unsafe.Pointer(&cfg))
	if err != nil {
		return systemConfig{}, err
	}

	if len(r.Buf) != 0 {
		return systemConfig{}, fmt.Errorf(
			"%v bytes left in buffer", len(r.Buf))
	}

	return cfg, nil
}

func (p *Client) release(conn *transactableConn, err error) error {
	if err != nil && isClientConnectionError(err) {
		p.potentialConns <- struct{}{}
		return conn.Close()
	}
	conn.idleSince = time.Now()
	conn.pollEpoch = atomic.LoadInt64(p.pollEpoch)
	p.freeConns <- conn
	return nil
}

// EnsureConnected forces the client to connect if it hasn't already.
func (p *Client) EnsureConnected(ctx context.Context) error {
	conn, err := p.acquire(ctx)
	if err != nil {
		return err
	}

	return p.release(conn, nil)
}

// Close closes all connections in the pool.
// Calling close blocks until all acquired connections have been released,
// and returns an error if called more than once.
func (p *Client) Close() error {
	p.isClosedMutex.Lock()
	defer p.isClosedMutex.Unlock()

	if *p.isClosed {
		return &interfaceError{msg: "client closed"}
	}
	*p.isClosed = true

	p.connectionPoolMutex.Lock()
	if p.potentialConns == nil {
		// The client never made any connections.
		p.connectionPoolMutex.Unlock()
		return nil
	}
	p.connectionPoolMutex.Unlock()

	wg := sync.WaitGroup{}
	errs := make([]error, p.concurrency)
	for i := 0; i < p.concurrency; i++ {
		select {
		case conn := <-p.freeConns:
			wg.Add(1)
			go func(i int) {
				errs[i] = conn.Close()
				wg.Done()
			}(i)
		case <-p.potentialConns:
		}
	}

	wg.Wait()
	return wrapAll(errs...)
}

// Execute an EdgeQL command (or commands).
func (p *Client) Execute(ctx context.Context, cmd string) error {
	if tx, hasTx := TxFromContext(ctx); hasTx {
		if subTx, hasSubtx := SubtxFromContext(ctx); hasSubtx {
			return subTx.Execute(ctx, cmd)
		}
		return tx.Execute(ctx, cmd)
	}

	conn, err := p.acquire(ctx)
	if err != nil {
		return err
	}

	q := sfQuery{
		cmd:     cmd,
		headers: conn.headers(),
	}

	defer conn.conn.handleCtxCancel(ctx)()
	err = conn.scriptFlow(ctx, q)
	return firstError(err, p.release(conn, err))
}

// Query runs a query and returns the results.
func (p *Client) Query(
	ctx context.Context,
	cmd string,
	out interface{},
	args ...interface{},
) error {
	if tx, hasTx := TxFromContext(ctx); hasTx {
		if subTx, hasSubtx := SubtxFromContext(ctx); hasSubtx {
			return subTx.Query(ctx, cmd, out, args...)
		}
		return tx.Query(ctx, cmd, out, args...)
	}

	conn, err := p.acquire(ctx)
	if err != nil {
		return err
	}

	err = runQuery(ctx, conn, "Query", cmd, out, args)
	return firstError(err, p.release(conn, err))
}

// QuerySingle runs a singleton-returning query and returns its element.
// If the query executes successfully but doesn't return a result
// a NoDataError is returned.
func (p *Client) QuerySingle(
	ctx context.Context,
	cmd string,
	out interface{},
	args ...interface{},
) error {
	if tx, hasTx := TxFromContext(ctx); hasTx {
		if subTx, hasSubtx := SubtxFromContext(ctx); hasSubtx {
			return subTx.QuerySingle(ctx, cmd, out, args...)
		}
		return tx.QuerySingle(ctx, cmd, out, args...)
	}

	conn, err := p.acquire(ctx)
	if err != nil {
		return err
	}

	err = runQuery(ctx, conn, "QuerySingle", cmd, out, args)
	return firstError(err, p.release(conn, err))
}

// QueryJSON runs a query and return the results as JSON.
func (p *Client) QueryJSON(
	ctx context.Context,
	cmd string,
	out *[]byte,
	args ...interface{},
) error {
	if tx, hasTx := TxFromContext(ctx); hasTx {
		if subTx, hasSubtx := SubtxFromContext(ctx); hasSubtx {
			return subTx.QueryJSON(ctx, cmd, out, args...)
		}
		return tx.QueryJSON(ctx, cmd, out, args...)
	}

	conn, err := p.acquire(ctx)
	if err != nil {
		return err
	}

	err = runQuery(ctx, conn, "QueryJSON", cmd, out, args)
	return firstError(err, p.release(conn, err))
}

// QuerySingleJSON runs a singleton-returning query.
// If the query executes successfully but doesn't have a result
// a NoDataError is returned.
func (p *Client) QuerySingleJSON(
	ctx context.Context,
	cmd string,
	out interface{},
	args ...interface{},
) error {
	if tx, hasTx := TxFromContext(ctx); hasTx {
		if subTx, hasSubtx := SubtxFromContext(ctx); hasSubtx {
			return subTx.QuerySingleJSON(ctx, cmd, out, args...)
		}
		return tx.QuerySingleJSON(ctx, cmd, out, args...)
	}

	conn, err := p.acquire(ctx)
	if err != nil {
		return err
	}

	err = runQuery(ctx, conn, "QuerySingleJSON", cmd, out, args)
	return firstError(err, p.release(conn, err))
}

// Tx runs an action in a transaction retrying failed actions
// if they might succeed on a subsequent attempt.
//
// Retries are governed by retry rules.
// The default rule can be set with WithRetryRule().
// For more fine grained control a retry rule can be set
// for each defined RetryCondition using WithRetryCondition().
// When a transaction fails but is retryable
// the rule for the failure condition is used to determine if the transaction
// should be tried again based on RetryRule.Attempts and the amount of time
// to wait before retrying is determined by RetryRule.Backoff.
// If either field is unset (see RetryRule) then the default rule is used.
// If the object's default is unset the fall back is 3 attempts
// and exponential backoff.
func (p *Client) Tx(ctx context.Context, action TxBlock) error {
	conn, err := p.acquire(ctx)
	if err != nil {
		return err
	}

	err = conn.Tx(ctx, action)
	return firstError(err, p.release(conn, err))
}
