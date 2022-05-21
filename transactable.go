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
	"errors"
	"time"
)

type txContextKey struct{}

// TxFromContext returns the last Tx from the context chain, if any.
// The context.Context argument from a TxBlock will have one stored.
func TxFromContext(ctx context.Context) (*Tx, bool) {
	tx, ok := ctx.Value(txContextKey{}).(*Tx)
	return tx, ok
}

type transactableConn struct {
	*reconnectingConn
	txOpts    TxOptions
	retryOpts RetryOptions
	idleSince time.Time
	pollEpoch int64
}

func (c *transactableConn) granularFlow(
	ctx context.Context,
	q *gfQuery,
) error {
	var err error
	if err = c.reconnectingConn.granularFlow(ctx, q); err == nil {
		return nil
	}

	var edbErr Error
	i := 0
	for {
		i++
		// q is a read only query if it has no capabilities
		// i.e. capabilities == 0. Read only queries are always
		// retryable, mutation queries are retryable if the
		// error explicitly indicates a transaction conflict.
		capabilities, ok := c.getCachedCapabilities(q)
		if ok &&
			errors.As(err, &edbErr) &&
			edbErr.HasTag(ShouldRetry) &&
			(capabilities == 0 || edbErr.Category(TransactionConflictError)) {
			rule, e := c.retryOpts.ruleForException(edbErr)
			if e != nil {
				return e
			}

			if i >= rule.attempts {
				return err
			}

			time.Sleep(rule.backoff(i))
		} else {
			return err
		}

		if c.conn.soc.Closed() {
			if err = c.reconnect(ctx, true); err != nil {
				continue
			}
		}

		if err = c.reconnectingConn.granularFlow(ctx, q); err == nil {
			return nil
		}
	}
}

// Tx runs an action in a transaction retrying failed actions if they might
// succeed on a subsequent attempt.
func (c *transactableConn) Tx(
	ctx context.Context,
	action TxBlock,
) (err error) {
	conn, err := c.borrow("transaction")
	if err != nil {
		return err
	}
	defer func() { err = firstError(err, c.unborrow()) }()

	var edbErr Error
	for i := 1; true; i++ {
		if err != nil && errors.As(err, &edbErr) && c.conn.soc.Closed() {
			err = c.reconnect(ctx, true)
			if err != nil {
				goto Error
			}
			// get the newly connected protocolConnection
			conn = c.conn
		}

		{
			cancel := conn.handleCtxCancel(ctx)
			tx := &Tx{
				borrowableConn: borrowableConn{conn: conn},
				txState:        &txState{},
				options:        c.txOpts,
			}
			err = tx.start()
			if err != nil {
				cancel()
				goto Error
			}

			err = action(context.WithValue(ctx, txContextKey{}, tx), tx)
			if err == nil {
				err = tx.commit()
				cancel()
				if err != nil && errors.As(err, &edbErr) &&
					edbErr.Category(TransactionError) &&
					edbErr.HasTag(ShouldRetry) {
					goto Error
				}
				return err
			} else if isClientConnectionError(err) {
				cancel()
				goto Error
			}

			if e := tx.rollback(); e != nil && !errors.As(e, &edbErr) {
				cancel()
				return e
			}
			cancel()
		}

	Error:
		if errors.As(err, &edbErr) && edbErr.HasTag(ShouldRetry) {
			rule, e := c.retryOpts.ruleForException(edbErr)
			if e != nil {
				return e
			}

			if i >= rule.attempts {
				return err
			}

			time.Sleep(rule.backoff(i))
			continue
		}

		return err
	}

	return &clientError{msg: "unreachable"}
}
