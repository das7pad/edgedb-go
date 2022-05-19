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

package buff

import (
	"encoding/binary"
	"fmt"
	"io"

	types "github.com/edgedb/edgedb-go/internal/edgedbtypes"
)

const (
	slabSize = 512 * 1024
)

// Reader is a buffer reader.
type Reader struct {
	conn   io.Reader
	slab   []byte
	offset int

	Err     error
	Buf     []byte
	MsgType uint8
}

// NewReader returns a new Reader.
func NewReader(conn io.Reader) *Reader {
	return &Reader{conn: conn, slab: make([]byte, 0, slabSize)}
}

// SimpleReader creates a new reader that operates on a single []byte.
func SimpleReader(buf []byte) *Reader {
	r := &Reader{Buf: buf[:len(buf):len(buf)]}
	return r
}

// Next advances the reader to the next message.
// Next returns false when the reader doesn't own any socket data
//  and waitForMore is false, or an error is encountered while reading.
//
// Callers must continue to call Next until it returns false.
//
// If the previous message was not fully read Next() panics.
//
// Next() panics if called on a reader created with SimpleReader().
func (r *Reader) Next(waitForMore bool) bool {
	if r.conn == nil {
		panic("called next on a simple reader")
	}

	if len(r.Buf) > 0 {
		r.Err = fmt.Errorf(
			"cannot finish: unread data in buffer (message type: 0x%x)",
			r.MsgType,
		)
		return false
	}

	r.MsgType = 0

	if len(r.slab) == 0 && !waitForMore {
		return false
	}

	// put message type and length into r.Buf
	r.Err = r.feed(5)
	if r.Err != nil {
		return false
	}

	r.MsgType = r.PopUint8()
	msgLen := int(r.PopUint32()) - 4

	r.Err = r.feed(msgLen)
	if r.Err != nil {
		return false
	}

	r.Buf = r.Buf[:msgLen:msgLen]
	return true
}

func min(x, y int) int {
	if x < y {
		return x
	}

	return y
}

func (r *Reader) feed(n int) error {
	if n == 0 {
		return nil
	}

	m := min(n, len(r.slab)-r.offset)
	isOwnSlice := false
	if n+r.offset > slabSize {
		r.Buf = make([]byte, n)
		copy(r.Buf, r.slab[r.offset:r.offset+m])
		isOwnSlice = true
	} else {
		r.Buf = r.slab[r.offset : r.offset+n]
	}
	n -= m
	r.offset += m

	if n == 0 {
		if r.offset == len(r.slab) {
			r.slab = r.slab[:0]
			r.offset = 0
		}
		return nil
	}

	for n > 0 {
		if isOwnSlice && r.offset == len(r.slab) {
			r.slab = r.slab[:0]
			r.offset = 0
		}
		nn, err := r.conn.Read(r.slab[r.offset:slabSize])
		if err != nil {
			return err
		}
		r.slab = r.slab[:r.offset+nn]
		overlap := min(n, nn)
		if isOwnSlice {
			copy(r.Buf[m:], r.slab[r.offset:r.offset+overlap])
			m += overlap
		}
		n -= overlap
		r.offset += overlap
	}

	if r.offset == len(r.slab) {
		r.slab = r.slab[:0]
		r.offset = 0
	}
	return nil
}

// Discard skips n bytes.
func (r *Reader) Discard(n int) {
	r.Buf = r.Buf[n:]
}

// DiscardMessage discards all remaining bytes in the current message.
func (r *Reader) DiscardMessage() {
	r.Buf = nil
}

// PopSlice returns a SimpleReader
// populated with the first n bytes from the buffer
// and discards those bytes.
func (r *Reader) PopSlice(n uint32) *Reader {
	s := SimpleReader(r.Buf[:n])
	r.Buf = r.Buf[n:]
	return s
}

// PopUint8 returns the next byte and advances the buffer.
func (r *Reader) PopUint8() uint8 {
	val := r.Buf[0]
	r.Buf = r.Buf[1:]
	return val
}

// PopUint16 reads a uint16 and advances the buffer.
func (r *Reader) PopUint16() uint16 {
	val := binary.BigEndian.Uint16(r.Buf[:2])
	r.Buf = r.Buf[2:]
	return val
}

// PopUint32 reads a uint32 and advances the buffer.
func (r *Reader) PopUint32() uint32 {
	val := binary.BigEndian.Uint32(r.Buf[:4])
	r.Buf = r.Buf[4:]
	return val
}

// PopUint64 reads a uint64 and advances the buffer.
func (r *Reader) PopUint64() uint64 {
	val := binary.BigEndian.Uint64(r.Buf[:8])
	r.Buf = r.Buf[8:]
	return val
}

// PopUUID reads a types.UUID and advances the buffer.
func (r *Reader) PopUUID() types.UUID {
	var id types.UUID
	copy(id[:], r.Buf[:16])
	r.Buf = r.Buf[16:]
	return id
}

// PopBytes reads a []byte and advances the buffer.
// The returned slice is owned by the buffer.
func (r *Reader) PopBytes() []byte {
	n := int(r.PopUint32())
	val := r.Buf[:n]
	r.Buf = r.Buf[n:]
	return val
}

// PopString reads a string and advances the buffer.
func (r *Reader) PopString() string {
	n := int(r.PopUint32())
	val := string(r.Buf[:n])
	r.Buf = r.Buf[n:]
	return val
}
