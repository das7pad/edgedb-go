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
	"log"
	"sync"
	"time"

	"github.com/edgedb/edgedb-go/internal"
	"github.com/edgedb/edgedb-go/internal/buff"
	"github.com/edgedb/edgedb-go/internal/soc"
)

type cacheCollection struct {
	serverSettings    *serverSettings
	typeIDCache       *sync.Map
	inCodecCache      *sync.Map
	outCodecCache     *sync.Map
	capabilitiesCache *sync.Map // nolint:structcheck
}

type protocolConnection struct {
	soc         *autoClosingSocket
	writeMemory [1024]byte
	r           *buff.Reader

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

	deadline, _ := ctx.Deadline()
	err = socket.SetDeadline(deadline)
	if err != nil {
		_ = socket.Close()
		return nil, err
	}

	toBeDeserialized := make(chan *soc.Data, 2)
	conn := &protocolConnection{
		soc:             socket,
		r:               buff.NewReader(toBeDeserialized),
		cacheCollection: caches,
	}

	go soc.Read(socket, soc.NewMemPool(4, 256*1024), toBeDeserialized)

	err = conn.connect(cfg)
	if err != nil {
		_ = socket.Close()
		return nil, err
	}

	if err = conn.resetSocketDeadLine(); err != nil {
		_ = socket.Close()
		return nil, err
	}
	return conn, nil
}

func (c *protocolConnection) resetSocketDeadLine() error {
	return c.soc.SetDeadline(time.Time{})
}

func (c *protocolConnection) pollBackgroundMessages() {
	if c.soc.Closed() {
		return
	}
	waitForMore := false
	for c.r.Next(&waitForMore) {
		if err := c.fallThrough(); err != nil {
			log.Println(err)
			_ = c.soc.Close()
		}
	}
}

// Close the db connection
func (c *protocolConnection) close() error {
	err := c.terminate()
	if err != nil {
		return err
	}

	return c.soc.Close()
}

func (c *protocolConnection) scriptFlow(ctx context.Context, q sfQuery) error {
	deadline, _ := ctx.Deadline()
	err := c.soc.SetDeadline(deadline)
	if err != nil {
		return err
	}
	return firstError(c.execScriptFlow(q), c.resetSocketDeadLine())
}

func (c *protocolConnection) granularFlow(
	ctx context.Context,
	q *gfQuery,
) error {
	deadline, _ := ctx.Deadline()
	err := c.soc.SetDeadline(deadline)
	if err != nil {
		return err
	}
	return firstError(c.execGranularFlow(q), c.resetSocketDeadLine())
}
