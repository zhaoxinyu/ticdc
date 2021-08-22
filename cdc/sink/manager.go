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

package sink

import (
	"context"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/common"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
)

const (
	defaultMetricInterval = time.Second * 15
)

// Manager manages table sinks, maintains the relationship between table sinks and backendSink
type Manager struct {
	bsink        *bufferSink
	checkpointTs model.Ts
	tableSinks   map[model.TableID]*tableSink
	tableSinksMu sync.Mutex

	flushMu  sync.Mutex
	flushing int64

	drawbackChan chan drawbackMsg
}

// NewManager creates a new Sink manager
func NewManager(ctx context.Context, backendSink Sink, errCh chan error, checkpointTs model.Ts) (*Manager, *common.ReactiveTsFlowControl) {
	drawbackChan := make(chan drawbackMsg, 16)

	const tsFlowControlStep = time.Minute
	flowControl := common.NewReactiveTsFlowControl(checkpointTs)
	newCheckpointTime := oracle.GetTimeFromTS(checkpointTs)
	upperBoundTime := newCheckpointTime.Add(tsFlowControlStep)
	flowControl.Request(oracle.EncodeTSO(upperBoundTime.Unix() * 1000))
	bsink := newBufferSink(ctx, backendSink, checkpointTs, drawbackChan, flowControl)
	go bsink.run(ctx, errCh, tsFlowControlStep)

	return &Manager{
		bsink:        bsink,
		checkpointTs: checkpointTs,
		tableSinks:   make(map[model.TableID]*tableSink),
		drawbackChan: drawbackChan,
	}, flowControl
}

// CreateTableSink creates a table sink
func (m *Manager) CreateTableSink(tableID model.TableID, checkpointTs model.Ts) Sink {
	m.tableSinksMu.Lock()
	defer m.tableSinksMu.Unlock()
	if _, exist := m.tableSinks[tableID]; exist {
		log.Panic("the table sink already exists", zap.Uint64("tableID", uint64(tableID)))
	}
	sink := &tableSink{
		tableID:   tableID,
		manager:   m,
		buffer:    make([]*model.RowChangedEvent, 0, 128),
		emittedTs: checkpointTs,
	}
	m.tableSinks[tableID] = sink
	return sink
}

// Close closes the Sink manager and backend Sink, this method can be reentrantly called
func (m *Manager) Close(ctx context.Context) error {
	return m.bsink.Close(ctx)
}

func (m *Manager) getMinEmittedTs() (model.Ts, uint64) {
	m.tableSinksMu.Lock()
	defer m.tableSinksMu.Unlock()
	if len(m.tableSinks) == 0 {
		return m.getCheckpointTs(), 0
	}
	minTs := model.Ts(math.MaxUint64)
	minTable := int64(0)
	for tableID, tableSink := range m.tableSinks {
		emittedTs := tableSink.getEmittedTs()
		if minTs > emittedTs {
			minTs = emittedTs
			minTable = tableID
		}
	}
	return minTs, uint64(minTable)
}

func (m *Manager) flushBackendSink(ctx context.Context, tableID uint64) (model.Ts, error) {
	if !atomic.CompareAndSwapInt64(&m.flushing, 0, 1) {
		return atomic.LoadUint64(&m.checkpointTs), nil
	}
	m.flushMu.Lock()
	defer func() {
		m.flushMu.Unlock()
		atomic.StoreInt64(&m.flushing, 0)
	}()
	minEmittedTs, minTableID := m.getMinEmittedTs()
	log.Info("manager flushBackendSink",
		zap.Uint64("resolvedTs", minEmittedTs),
		zap.Uint64("tableID", tableID),
		zap.Uint64("minTableID", uint64(minTableID)))
	checkpointTs, err := m.bsink.FlushRowChangedEvents(ctx, minEmittedTs)
	if err != nil {
		return m.getCheckpointTs(), errors.Trace(err)
	}
	atomic.StoreUint64(&m.checkpointTs, checkpointTs)
	return checkpointTs, nil
}

func (m *Manager) destroyTableSink(ctx context.Context, tableID model.TableID) error {
	m.tableSinksMu.Lock()
	delete(m.tableSinks, tableID)
	m.tableSinksMu.Unlock()
	callback := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.drawbackChan <- drawbackMsg{tableID: tableID, callback: callback}:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-callback:
	}
	return m.bsink.Barrier(ctx)
}

func (m *Manager) getCheckpointTs() uint64 {
	return atomic.LoadUint64(&m.checkpointTs)
}

type tableSink struct {
	tableID model.TableID
	manager *Manager
	buffer  []*model.RowChangedEvent
	// emittedTs means all of events which of commitTs less than or equal to emittedTs is sent to backendSink
	emittedTs model.Ts
}

func (t *tableSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	// do nothing
	return nil
}

func (t *tableSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	t.buffer = append(t.buffer, rows...)
	return nil
}

func (t *tableSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	// the table sink doesn't receive the DDL event
	return nil
}

func (t *tableSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	i := sort.Search(len(t.buffer), func(i int) bool {
		return t.buffer[i].CommitTs > resolvedTs
	})
	if i == 0 {
		atomic.StoreUint64(&t.emittedTs, resolvedTs)
		return t.manager.flushBackendSink(ctx, uint64(t.tableID))
	}
	resolvedRows := t.buffer[:i]
	t.buffer = append(make([]*model.RowChangedEvent, 0, len(t.buffer[i:])), t.buffer[i:]...)

	err := t.manager.bsink.EmitRowChangedEvents(ctx, resolvedRows...)
	if err != nil {
		return t.manager.getCheckpointTs(), errors.Trace(err)
	}
	atomic.StoreUint64(&t.emittedTs, resolvedTs)
	return t.manager.flushBackendSink(ctx, uint64(t.tableID))
}

func (t *tableSink) getEmittedTs() uint64 {
	return atomic.LoadUint64(&t.emittedTs)
}

func (t *tableSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	// the table sink doesn't receive the checkpoint event
	return nil
}

// Note once the Close is called, no more events can be written to this table sink
func (t *tableSink) Close(ctx context.Context) error {
	return t.manager.destroyTableSink(ctx, t.tableID)
}

// Barrier is not used in table sink
func (t *tableSink) Barrier(ctx context.Context) error {
	return nil
}

type drawbackMsg struct {
	tableID  model.TableID
	callback chan struct{}
}

type bufferSink struct {
	Sink
	checkpointTs uint64
	buffer       map[model.TableID][]*model.RowChangedEvent
	bufferMu     sync.Mutex
	flushTsChan  chan uint64
	drawbackChan chan drawbackMsg
	flowControl  *common.ReactiveTsFlowControl
}

func newBufferSink(
	ctx context.Context,
	backendSink Sink,
	checkpointTs model.Ts,
	drawbackChan chan drawbackMsg,
	flowControl *common.ReactiveTsFlowControl,
) *bufferSink {
	return &bufferSink{
		Sink: backendSink,
		// buffer shares the same flow control with table sink
		buffer:       make(map[model.TableID][]*model.RowChangedEvent),
		checkpointTs: checkpointTs,
		flushTsChan:  make(chan uint64, 128),
		drawbackChan: drawbackChan,
		flowControl:  flowControl,
	}
}

func (b *bufferSink) run(ctx context.Context, errCh chan error, flowControlStep time.Duration) {
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	advertiseAddr := util.CaptureAddrFromCtx(ctx)
	metricFlushDuration := flushRowChangedDuration.WithLabelValues(advertiseAddr, changefeedID, "Flush")
	metricEmitRowDuration := flushRowChangedDuration.WithLabelValues(advertiseAddr, changefeedID, "EmitRow")
	metricBufferSize := bufferChanSizeGauge.WithLabelValues(advertiseAddr, changefeedID)
	metricsTimer := time.NewTimer(defaultMetricInterval)
	defer metricsTimer.Stop()
	fcTimer := time.NewTimer(flowControlStep / 10)
	defer fcTimer.Stop()

	for {
		select {
		case <-metricsTimer.C:
			metricBufferSize.Set(float64(len(b.buffer)))
			metricsTimer.Reset(defaultMetricInterval)
		default:
		}
		flushDuration, emitRowDuration, err := b.runOnce(ctx, fcTimer, errCh, flowControlStep)
		if err != nil && errors.Cause(err) != context.Canceled {
			errCh <- err
			return
		}
		if flushDuration != 0 {
			metricFlushDuration.Observe(flushDuration.Seconds())
		}
		if emitRowDuration != 0 {
			metricEmitRowDuration.Observe(emitRowDuration.Seconds())
		}
	}
}

func (b *bufferSink) runOnce(
	ctx context.Context, fcTimer *time.Timer, errCh chan error, flowControlStep time.Duration,
) (
	flushDuration, emitRowDuration time.Duration, err error,
) {
	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	case drawback := <-b.drawbackChan:
		b.bufferMu.Lock()
		delete(b.buffer, drawback.tableID)
		b.bufferMu.Unlock()
		close(drawback.callback)
	case <-fcTimer.C:
		checkpointTs := atomic.LoadUint64(&b.checkpointTs)
		upperBoundTime := oracle.GetTimeFromTS(checkpointTs).Add(flowControlStep)
		b.flowControl.Request(oracle.EncodeTSO(upperBoundTime.Unix() * 1000))
		fcTimer.Reset(flowControlStep / 10)
		log.Info("bufferSink fcTimer",
			zap.Uint64("checkpointTs", checkpointTs),
			zap.Uint64("upperBound", b.flowControl.GetConsumption()))
	case resolvedTs := <-b.flushTsChan:
		b.bufferMu.Lock()
		// find all rows before resolvedTs and emit to backend sink
		for tableID, rows := range b.buffer {
			i := sort.Search(len(rows), func(i int) bool {
				return rows[i].CommitTs > resolvedTs
			})

			start := time.Now()
			err = b.Sink.EmitRowChangedEvents(ctx, rows[:i]...)
			if err != nil {
				b.bufferMu.Unlock()
				return
			}
			emitRowDuration = time.Since(start)

			// put remaining rows back to buffer
			// append to a new, fixed slice to avoid lazy GC
			b.buffer[tableID] = append(make([]*model.RowChangedEvent, 0, len(rows[i:])), rows[i:]...)
		}
		b.bufferMu.Unlock()

		start := time.Now()
		var checkpointTs uint64
		checkpointTs, err = b.Sink.FlushRowChangedEvents(ctx, resolvedTs)
		if err != nil {
			return
		}
		oldCheckpointTs := atomic.SwapUint64(&b.checkpointTs, checkpointTs)
		if oldCheckpointTs > checkpointTs {
			if checkpointTs != 0 {
				log.Panic("checkpoint regression",
					zap.Uint64("old", oldCheckpointTs),
					zap.Uint64("new", checkpointTs))
			}
			checkpointTs = oldCheckpointTs
		}
		oldCheckpointTime := oracle.GetTimeFromTS(oldCheckpointTs)
		newCheckpointTime := oracle.GetTimeFromTS(checkpointTs)
		oldUpperBound := b.flowControl.GetConsumption()
		if newCheckpointTime.Sub(oldCheckpointTime) >= 0 {
			upperBoundTime := newCheckpointTime.Add(flowControlStep)
			b.flowControl.Request(oracle.EncodeTSO(upperBoundTime.Unix() * 1000))
		}

		log.Info("bufferSink resolvedTs",
			zap.Uint64("oldCheckpointTs", oldCheckpointTs),
			zap.Uint64("checkpointTs", checkpointTs),
			zap.Uint64("oldUpperBound", oldUpperBound),
			zap.Uint64("upperBound", b.flowControl.GetConsumption()))

		flushDuration = time.Since(start)
		if flushDuration > 3*time.Second {
			log.Warn("flush row changed events too slow",
				zap.Duration("duration", flushDuration), util.ZapFieldChangefeed(ctx))
		}

	}
	return
}

func (b *bufferSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		if len(rows) == 0 {
			return nil
		}
		tableID := rows[0].Table.TableID
		b.bufferMu.Lock()
		b.buffer[tableID] = append(b.buffer[tableID], rows...)
		b.bufferMu.Unlock()
	}
	return nil
}

func (b *bufferSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	select {
	case <-ctx.Done():
		return atomic.LoadUint64(&b.checkpointTs), ctx.Err()
	case b.flushTsChan <- resolvedTs:
	}
	return atomic.LoadUint64(&b.checkpointTs), nil
}
