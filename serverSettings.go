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
	"fmt"
	"strconv"
	"time"

	"github.com/edgedb/edgedb-go/internal"
)

type serverSettings struct {
	suggestedPoolConcurrency *int
	idleConnectionTimeout    *time.Duration
}

func (s *serverSettings) GetPoolCurrency() int {
	if concurrency := s.suggestedPoolConcurrency; concurrency != nil {
		return *concurrency
	}
	return defaultConcurrency
}

func (s *serverSettings) GetIdleConnectionTimeout() time.Duration {
	if d := s.idleConnectionTimeout; d != nil {
		return *d
	}
	return defaultIdleConnectionTimeout
}

// Set sets the value for key.
func (s *serverSettings) Set(
	key string, blob []byte, v internal.ProtocolVersion,
) error {
	switch key {
	case "suggested_pool_concurrency":
		c, err := strconv.Atoi(string(blob))
		if err != nil {
			return fmt.Errorf("invalid suggested_pool_concurrency: %w", err)
		}
		s.suggestedPoolConcurrency = &c
	case "system_config":
		c, err := parseSystemConfig(blob, v)
		if err != nil {
			return fmt.Errorf("invalid system_config: %w", err)
		}
		d := c.SessionIdleTimeout.ToStdDuration()
		s.idleConnectionTimeout = &d
	}
	return nil
}
