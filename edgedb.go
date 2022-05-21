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
	"sync"
	"time"

	"github.com/edgedb/edgedb-go/internal"
	"github.com/edgedb/edgedb-go/internal/buff"
)

type cacheCollection struct {
	serverSettings    *serverSettings
	typeIDCache       *sync.Map
	inCodecCache      *sync.Map
	outCodecCache     *sync.Map
	capabilitiesCache *sync.Map // nolint:structcheck
}

type protocolConnection struct {
	soc *autoClosingSocket
	cr  *buff.ConnReader
	w   *buff.Writer

	protocolVersion internal.ProtocolVersion
	cacheCollection
}

// connectWithTimeout makes a single attempt to connect to `addr`.
func connectWithTimeout(
	ctx context.Context,
	cfg *connConfig,
	caches cacheCollection,
) (*protocolConnection, error) {
	socket, err := connectAutoClosingSocket(ctx, cfg)
	if err != nil {
		return nil, err
	}

	conn := &protocolConnection{
		soc:             socket,
		cr:              buff.NewReader(socket),
		w:               buff.NewWriter(),
		cacheCollection: caches,
	}

	defer conn.handleCtxCancel(ctx)()

	err = conn.connect(cfg)
	if err != nil {
		_ = socket.Close()
		return nil, err
	}
	return conn, nil
}

func (c *protocolConnection) pollBackgroundMessages() error {
	if c.soc.Closed() {
		return nil
	}
	if err := c.soc.SetDeadline(time.Now().Add(time.Millisecond)); err != nil {
		return err
	}
	for c.cr.Poll() {
		if err := c.fallThrough(); err != nil {
			_ = c.soc.Close()
			return err
		}
	}

	if err := c.cr.Err; err != nil {
		return err
	}

	if err := c.soc.SetDeadline(time.Time{}); err != nil {
		return err
	}
	return nil
}

// Close the db connection
func (c *protocolConnection) close() error {
	err := c.terminate()
	if err != nil {
		return err
	}

	return c.soc.Close()
}

func (c *protocolConnection) scriptFlow(q sfQuery) error {
	return c.execScriptFlow(q)
}

func (c *protocolConnection) granularFlow(
	ctx context.Context,
	q *gfQuery,
) error {
	defer c.handleCtxCancel(ctx)()
	return c.execGranularFlow(q)
}

func (c *protocolConnection) handleCtxCancel(ctx context.Context) func() {
	waitForDefer, cancel := context.WithCancel(ctx)
	aborted := true
	go func() {
		<-waitForDefer.Done()
		if aborted {
			// Unblock reads and writes.
			_ = c.soc.Close()
		}
	}()
	return func() {
		aborted = false
		cancel()
	}
}
