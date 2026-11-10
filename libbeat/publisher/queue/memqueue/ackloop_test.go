// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package memqueue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProcessACKDoesNotBlockAfterQueueClosed(t *testing.T) {
	// Once the queue is closed, runLoop no longer reads deleteChan. An ACK
	// that is being processed at that moment must not block the ackLoop
	// forever.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b := &broker[int]{
		ctx:        ctx,
		buf:        make([]queueEntry[int], 4),
		deleteChan: make(chan int), // unbuffered and never read
	}

	acked := 0
	producer := &ackProducer[int]{
		broker: b,
		state:  produceState{cb: func(count int) { acked += count }},
	}
	for i := range b.buf {
		b.buf[i].producer = producer
		b.buf[i].producerID = producerID(i + 1)
	}
	var acks batchList[int]
	acks.append(&batch[int]{queue: b, start: 0, count: len(b.buf)})

	done := make(chan struct{})
	go func() {
		newACKLoop(b).processACK(acks, len(b.buf))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("processACK blocked on deleteChan after the queue was closed")
	}
	assert.Zero(t, acked, "producer ACK callbacks should not run after the queue is closed")
}
