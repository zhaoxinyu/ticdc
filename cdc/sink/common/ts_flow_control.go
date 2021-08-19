// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

// ReactiveTsFlowControl is a reactive flow control, based on timestamp.
// It can be shared with multiple flows. All methods are threadsafe.
type ReactiveTsFlowControl struct {
	upperBound uint64
	isAborted  uint32

	mu   sync.Mutex
	cond *sync.Cond
}

// NewReactiveTsFlowControl creates a new ReactiveTsFlowControl
func NewReactiveTsFlowControl(upperBound uint64) *ReactiveTsFlowControl {
	ret := &ReactiveTsFlowControl{
		upperBound: upperBound,
		mu:         sync.Mutex{},
	}

	ret.cond = sync.NewCond(&ret.mu)
	return ret
}

// ConsumeWithBlocking is called when a hard-limit is needed. The method will
// block until enough memory has been freed up by Release.
// blockCallBack will be called if the function will block.
// Should be used with care to prevent deadlock.
func (c *ReactiveTsFlowControl) ConsumeWithBlocking(ts uint64, onBlock func() error) error {
	if ts > atomic.LoadUint64(&c.upperBound) {
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for {
		if atomic.LoadUint32(&c.isAborted) == 1 {
			return cerrors.ErrFlowControllerAborted.GenWithStackByArgs()
		}

		if ts <= atomic.LoadUint64(&c.upperBound) {
			// It's within the upper bound.
			break
		} else if onBlock != nil {
			onBlock()
			onBlock = nil
		}
		c.cond.Wait()
	}

	return nil
}

// ForceConsume is called when blocking is not acceptable and the limit can be violated
// for the sake of avoid deadlock. It merely records the increased memory consumption.
func (c *ReactiveTsFlowControl) ForceConsume(nBytes uint64) error {
	// no op
	return nil
}

// Request is called when the caller can advance its progress.
func (c *ReactiveTsFlowControl) Request(ts uint64) {
	upper := atomic.LoadUint64(&c.upperBound)
	if upper > ts {
		log.Warn("Reactive ts flow control regression",
			zap.Uint64("old", upper), zap.Uint64("new", ts))
		return
	}
	atomic.CompareAndSwapUint64(&c.upperBound, upper, ts)
	c.cond.Broadcast()
}

// Abort interrupts any ongoing ConsumeWithBlocking call
func (c *ReactiveTsFlowControl) Abort() {
	atomic.StoreUint32(&c.isAborted, 1)
	c.cond.Broadcast()
}

// GetConsumption returns the current memory consumption
func (c *ReactiveTsFlowControl) GetConsumption() uint64 {
	return atomic.LoadUint64(&c.upperBound)
}

// TableTsFlowController provides a convenient interface to control the memory consumption of a per table event stream
type TableTsFlowController struct {
	tsFlowControl *ReactiveTsFlowControl

	lastCommitTs uint64
}

// NewTableTsFlowController creates a new TableTsFlowController
func NewTableTsFlowController(tsFlowControl *ReactiveTsFlowControl) *TableTsFlowController {
	return &TableTsFlowController{
		tsFlowControl: tsFlowControl,
	}
}

// Consume is called when an event has arrived for being processed by the sink.
// It will handle transaction boundaries automatically, and will not block intra-transaction.
func (c *TableTsFlowController) Consume(commitTs uint64, size uint64, onBlock func() error) error {
	lastCommitTs := atomic.LoadUint64(&c.lastCommitTs)

	if commitTs < lastCommitTs {
		log.Panic("commitTs regressed, report a bug",
			zap.Uint64("commitTs", commitTs),
			zap.Uint64("lastCommitTs", c.lastCommitTs))
	}

	if commitTs > lastCommitTs {
		atomic.StoreUint64(&c.lastCommitTs, commitTs)
	}
	err := c.tsFlowControl.ConsumeWithBlocking(commitTs, onBlock)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// GetConsumption returns the current memory consumption
func (c *TableTsFlowController) GetConsumption() uint64 {
	return c.tsFlowControl.GetConsumption()
}
