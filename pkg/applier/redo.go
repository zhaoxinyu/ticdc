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

package applier

import (
	"context"
	"net/url"
	"time"

	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/reader"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink"
	ddlfactory "github.com/pingcap/tiflow/cdc/sink/ddlsink/factory"
	dmlfactory "github.com/pingcap/tiflow/cdc/sink/dmlsink/factory"
	"github.com/pingcap/tiflow/cdc/sink/tablesink"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	applierChangefeed = "redo-applier"
	warnDuration      = 3 * time.Minute
	flushWaitDuration = 200 * time.Millisecond
)

var (
	// In the boundary case, non-idempotent DDLs will not be executed.
	// TODO(CharlesCheung96): fix this
	unsupportedDDL = map[timodel.ActionType]struct{}{
		timodel.ActionExchangeTablePartition: {},
	}
	errApplyFinished = errors.New("apply finished, can exit safely")
)

// RedoApplierConfig is the configuration used by a redo log applier
type RedoApplierConfig struct {
	SinkURI string
	Storage string
	Dir     string
}

// RedoApplier implements a redo log applier
type RedoApplier struct {
	cfg *RedoApplierConfig
	rd  reader.RedoLogReader

	ddlSink         ddlsink.Sink
	appliedDDLCount uint64

	// sinkFactory is used to create table sinks.
	sinkFactory *dmlfactory.SinkFactory
	// tableSinks is a map from tableID to table sink.
	// We create it when we need it, and close it after we finish applying the redo logs.
	tableSinks         map[model.TableID]tablesink.TableSink
	tableResolvedTsMap map[model.TableID]model.ResolvedTs
	appliedLogCount    uint64

	errCh chan error
}

// NewRedoApplier creates a new RedoApplier instance
func NewRedoApplier(cfg *RedoApplierConfig) *RedoApplier {
	return &RedoApplier{
		cfg:   cfg,
		errCh: make(chan error, 1024),
	}
}

// toLogReaderConfig is an adapter to translate from applier config to redo reader config
// returns storageType, *reader.toLogReaderConfig and error
func (rac *RedoApplierConfig) toLogReaderConfig() (string, *reader.LogReaderConfig, error) {
	uri, err := url.Parse(rac.Storage)
	if err != nil {
		return "", nil, errors.WrapError(errors.ErrConsistentStorage, err)
	}
	cfg := &reader.LogReaderConfig{
		Dir:                uri.Path,
		UseExternalStorage: redo.IsExternalStorage(uri.Scheme),
	}
	if cfg.UseExternalStorage {
		cfg.URI = *uri
		// If use external storage as backend, applier will download redo logs to local dir.
		cfg.Dir = rac.Dir
	}
	return uri.Scheme, cfg, nil
}

func (ra *RedoApplier) catchError(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-ra.errCh:
			return err
		}
	}
}

func (ra *RedoApplier) initSink(ctx context.Context) (err error) {
	replicaConfig := config.GetDefaultReplicaConfig()
	ra.sinkFactory, err = dmlfactory.New(ctx, ra.cfg.SinkURI, replicaConfig, ra.errCh)
	if err != nil {
		return err
	}
	ra.ddlSink, err = ddlfactory.New(ctx, ra.cfg.SinkURI, replicaConfig)
	if err != nil {
		return err
	}

	ra.tableSinks = make(map[model.TableID]tablesink.TableSink)
	ra.tableResolvedTsMap = make(map[model.TableID]model.ResolvedTs)
	return nil
}

func (ra *RedoApplier) consumeLogs(ctx context.Context) error {
	checkpointTs, resolvedTs, err := ra.rd.ReadMeta(ctx)
	if err != nil {
		return err
	}
	log.Info("apply redo log starts",
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Uint64("resolvedTs", resolvedTs))
	if err := ra.initSink(ctx); err != nil {
		return err
	}
	defer ra.sinkFactory.Close()

	shouldApplyDDL := func(row *model.RowChangedEvent, ddl *model.DDLEvent) bool {
		if ddl == nil {
			return false
		} else if row == nil {
			// no more rows to apply
			return true
		}
		// If all rows before the DDL (which means row.CommitTs <= ddl.CommitTs)
		// are applied, we should apply this DDL.
		return row.CommitTs > ddl.CommitTs
	}

	row, err := ra.rd.ReadNextRow(ctx)
	if err != nil {
		return err
	}
	ddl, err := ra.rd.ReadNextDDL(ctx)
	if err != nil {
		return err
	}
	for {
		if row == nil && ddl == nil {
			break
		}
		if shouldApplyDDL(row, ddl) {
			if err := ra.applyDDL(ctx, ddl, checkpointTs, resolvedTs); err != nil {
				return err
			}
			if ddl, err = ra.rd.ReadNextDDL(ctx); err != nil {
				return err
			}
		} else {
			if err := ra.applyRow(row, checkpointTs); err != nil {
				return err
			}
			if row, err = ra.rd.ReadNextRow(ctx); err != nil {
				return err
			}
		}
	}
	// wait all tables to flush data
	for tableID := range ra.tableResolvedTsMap {
		if err := ra.waitTableFlush(ctx, tableID, resolvedTs); err != nil {
			return err
		}
		ra.tableSinks[tableID].Close(ctx)
	}

	log.Info("apply redo log finishes",
		zap.Uint64("appliedLogCount", ra.appliedLogCount),
		zap.Uint64("appliedDDLCount", ra.appliedDDLCount),
		zap.Uint64("currentCheckpoint", resolvedTs))
	return errApplyFinished
}

func (ra *RedoApplier) applyDDL(
	ctx context.Context, ddl *model.DDLEvent, checkpointTs, resolvedTs uint64,
) error {
	shouldSkip := func() bool {
		if ddl.CommitTs == checkpointTs {
			if _, ok := unsupportedDDL[ddl.Type]; ok {
				log.Error("ignore unsupported DDL", zap.Any("ddl", ddl))
				return true
			}
		}
		if ddl.TableInfo == nil {
			// Note this could omly happen when using old version of cdc, and the commit ts
			// of the DDL should be equal to checkpoint ts or resolved ts.
			log.Warn("ignore DDL without table info", zap.Any("ddl", ddl))
			return true
		}
		return false
	}
	if ddl.CommitTs != checkpointTs && ddl.CommitTs != resolvedTs {
		// TODO: move this panic to shouldSkip after redo log supports cross DDL events.
		log.Panic("ddl commit ts is not equal to checkpoint ts or resolved ts")
	}
	if shouldSkip() {
		return nil
	}
	log.Warn("apply DDL", zap.Any("ddl", ddl))
	// Wait all tables to flush data before applying DDL.
	// TODO: only block tables that are affected by this DDL.
	for tableID := range ra.tableSinks {
		if err := ra.waitTableFlush(ctx, tableID, ddl.CommitTs); err != nil {
			return err
		}
	}
	if err := ra.ddlSink.WriteDDLEvent(ctx, ddl); err != nil {
		return err
	}
	ra.appliedDDLCount++
	return nil
}

func (ra *RedoApplier) applyRow(
	row *model.RowChangedEvent, checkpointTs model.Ts,
) error {
	tableID := row.Table.TableID
	if _, ok := ra.tableSinks[tableID]; !ok {
		tableSink := ra.sinkFactory.CreateTableSink(
			model.DefaultChangeFeedID(applierChangefeed),
			spanz.TableIDToComparableSpan(tableID),
			prometheus.NewCounter(prometheus.CounterOpts{}),
		)
		ra.tableSinks[tableID] = tableSink
	}
	if _, ok := ra.tableResolvedTsMap[tableID]; !ok {
		ra.tableResolvedTsMap[tableID] = model.NewResolvedTs(checkpointTs)
	}
	ra.tableSinks[tableID].AppendRowChangedEvents(row)
	if row.CommitTs > ra.tableResolvedTsMap[tableID].Ts {
		// Use batch resolvedTs to flush data as quickly as possible.
		ra.tableResolvedTsMap[tableID] = model.ResolvedTs{
			Mode:    model.BatchResolvedMode,
			Ts:      row.CommitTs,
			BatchID: 1,
		}
	} else if row.CommitTs < ra.tableResolvedTsMap[tableID].Ts {
		log.Panic("commit ts of redo log regressed",
			zap.Int64("tableID", tableID),
			zap.Uint64("commitTs", row.CommitTs),
			zap.Any("resolvedTs", ra.tableResolvedTsMap[tableID]))
	}

	ra.appliedLogCount++
	if ra.appliedLogCount%mysql.DefaultMaxTxnRow == 0 {
		for tableID, tableResolvedTs := range ra.tableResolvedTsMap {
			if err := ra.tableSinks[tableID].UpdateResolvedTs(tableResolvedTs); err != nil {
				return err
			}
			if tableResolvedTs.IsBatchMode() {
				ra.tableResolvedTsMap[tableID] = tableResolvedTs.AdvanceBatch()
			}
		}
	}
	return nil
}

func (ra *RedoApplier) waitTableFlush(
	ctx context.Context, tableID model.TableID, rts model.Ts,
) error {
	ticker := time.NewTicker(warnDuration)
	defer ticker.Stop()

	resolvedTs := model.NewResolvedTs(rts)
	ra.tableResolvedTsMap[tableID] = resolvedTs
	if err := ra.tableSinks[tableID].UpdateResolvedTs(resolvedTs); err != nil {
		return err
	}
	// Make sure all events are flushed to downstream.
	for !ra.tableSinks[tableID].GetCheckpointTs().EqualOrGreater(resolvedTs) {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			log.Warn(
				"Table sink is not catching up with resolved ts for a long time",
				zap.Int64("tableID", tableID),
				zap.Any("resolvedTs", resolvedTs),
				zap.Any("checkpointTs", ra.tableSinks[tableID].GetCheckpointTs()),
			)
		default:
			time.Sleep(flushWaitDuration)
		}
	}
	return nil
}

var createRedoReader = createRedoReaderImpl

func createRedoReaderImpl(ctx context.Context, cfg *RedoApplierConfig) (reader.RedoLogReader, error) {
	storageType, readerCfg, err := cfg.toLogReaderConfig()
	if err != nil {
		return nil, err
	}
	return reader.NewRedoLogReader(ctx, storageType, readerCfg)
}

// ReadMeta creates a new redo applier and read meta from reader
func (ra *RedoApplier) ReadMeta(ctx context.Context) (checkpointTs uint64, resolvedTs uint64, err error) {
	rd, err := createRedoReader(ctx, ra.cfg)
	if err != nil {
		return 0, 0, err
	}
	return rd.ReadMeta(ctx)
}

// Apply applies redo log to given target
func (ra *RedoApplier) Apply(egCtx context.Context) (err error) {
	eg, egCtx := errgroup.WithContext(egCtx)
	egCtx = contextutil.PutRoleInCtx(egCtx, util.RoleRedoLogApplier)
	if ra.rd, err = createRedoReader(egCtx, ra.cfg); err != nil {
		return err
	}

	eg.Go(func() error {
		return ra.rd.Run(egCtx)
	})
	eg.Go(func() error {
		return ra.consumeLogs(egCtx)
	})
	eg.Go(func() error {
		return ra.catchError(egCtx)
	})

	err = eg.Wait()
	if errors.Cause(err) != errApplyFinished {
		return err
	}
	return nil
}
