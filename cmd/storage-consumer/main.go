// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	rcommon "github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/sink"
	"github.com/pingcap/tiflow/cdc/sink/codec/csv"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/quotes"
	psink "github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"go.uber.org/zap"
)

var (
	upstreamURIStr   string
	upstreamURI      *url.URL
	downstreamURIStr string
	configFile       string
	logFile          string
	logLevel         string
	flushInterval    time.Duration
)

func init() {
	flag.StringVar(&upstreamURIStr, "upstream-uri", "", "storage uri")
	flag.StringVar(&downstreamURIStr, "downstream-uri", "", "downstream sink uri")
	flag.StringVar(&configFile, "config", "", "changefeed configuration file")
	flag.StringVar(&logFile, "log-file", "", "log file path")
	flag.StringVar(&logLevel, "log-level", "info", "log level")
	flag.DurationVar(&flushInterval, "flush-interval", 10*time.Second, "flush interval")
	flag.Parse()

	err := logutil.InitLogger(&logutil.Config{
		Level: logLevel,
		File:  logFile,
	})
	if err != nil {
		log.Error("init logger failed", zap.Error(err))
		os.Exit(1)
	}

	uri, err := url.Parse(upstreamURIStr)
	if err != nil {
		log.Error("invalid upstream-uri", zap.Error(err))
		os.Exit(1)
	}
	upstreamURI = uri
	scheme := strings.ToLower(upstreamURI.Scheme)
	if !psink.IsStorageScheme(scheme) {
		log.Error("invalid storage scheme, the scheme of upstream-uri must be file/s3/azblob/gcs")
		os.Exit(1)
	}

	if len(configFile) == 0 {
		log.Error("changefeed configuration file must be provided")
		os.Exit(1)
	}
}

type schemaPathKey struct {
	schema       string
	table        string
	tableVersion int64
}

func (s schemaPathKey) generagteSchemaFilePath() string {
	return fmt.Sprintf("%s/%s/%d/schema.json", s.schema, s.table, s.tableVersion)
}

func (s *schemaPathKey) parseSchemaFilePath(path string) error {
	str := `(\w+)\/(\w+)\/(\d+)\/schema.json`
	pathRE, err := regexp.Compile(str)
	if err != nil {
		return err
	}

	matches := pathRE.FindStringSubmatch(path)
	if len(matches) != 4 {
		return fmt.Errorf("cannot match schema path pattern for %s", path)
	}

	version, err := strconv.ParseUint(matches[3], 10, 64)
	if err != nil {
		return err
	}

	*s = schemaPathKey{
		schema:       matches[1],
		table:        matches[2],
		tableVersion: int64(version),
	}
	return nil
}

type dmlPathKey struct {
	schemaPathKey
	partitionNum int64
	date         string
}

func (d *dmlPathKey) generateDMLFilePath(idx uint64) string {
	var elems []string

	elems = append(elems, d.schema)
	elems = append(elems, d.table)
	elems = append(elems, fmt.Sprintf("%d", d.tableVersion))

	if d.partitionNum != 0 {
		elems = append(elems, fmt.Sprintf("%d", d.partitionNum))
	}
	if len(d.date) != 0 {
		elems = append(elems, d.date)
	}
	elems = append(elems, fmt.Sprintf("CDC%06d.csv", idx))

	return strings.Join(elems, "/")
}

// dml file path pattern is as follows:
// {schema}/{table}/{table-version-separator}/{partition-separator}/{date-separator}/CDC{num}.extension
// in this pattern, partition-separator and date-separator could be empty.
func (d *dmlPathKey) parseDMLFilePath(dateSeparator, path string) (uint64, error) {
	var partitionNum int64

	str := `(\w+)\/(\w+)\/(\d+)\/(\d+\/)*`
	switch dateSeparator {
	case config.DateSeparatorNone.String():
		str += `(\d{4})*`
	case config.DateSeparatorYear.String():
		str += `(\d{4})\/`
	case config.DateSeparatorMonth.String():
		str += `(\d{4}-\d{2})\/`
	case config.DateSeparatorDay.String():
		str += `(\d{4}-\d{2}-\d{2})\/`
	}
	str += `CDC(\d+).\w+`
	pathRE, err := regexp.Compile(str)
	if err != nil {
		return 0, err
	}

	matches := pathRE.FindStringSubmatch(path)
	if len(matches) != 7 {
		return 0, fmt.Errorf("cannot match dml path pattern for %s", path)
	}

	version, err := strconv.ParseInt(matches[3], 10, 64)
	if err != nil {
		return 0, err
	}

	if len(matches[4]) > 0 {
		partitionNum, err = strconv.ParseInt(matches[4], 10, 64)
		if err != nil {
			return 0, err
		}
	}
	fileIdx, err := strconv.ParseUint(strings.TrimLeft(matches[6], "0"), 10, 64)
	if err != nil {
		return 0, err
	}

	*d = dmlPathKey{
		schemaPathKey: schemaPathKey{
			schema:       matches[1],
			table:        matches[2],
			tableVersion: version,
		},
		partitionNum: partitionNum,
		date:         matches[5],
	}

	return fileIdx, nil
}

// fileIndexRange defines a range of files. eg. CDC000002.csv ~ CDC000005.csv
type fileIndexRange struct {
	start uint64
	end   uint64
}

type consumer struct {
	sink            sink.Sink
	replicationCfg  *config.ReplicaConfig
	externalStorage storage.ExternalStorage
	// tableIdxMap maintains a map of <dmlPathKey, max file index>
	tableIdxMap map[dmlPathKey]uint64
	// tableTsMap maintains a map of <dmlPathKey, max commit ts>
	tableTsMap       map[dmlPathKey]uint64
	tableIDGenerator *fakeTableIDGenerator
}

func newConsumer(ctx context.Context) (*consumer, error) {
	replicaConfig := config.GetDefaultReplicaConfig()
	err := util.StrictDecodeFile(configFile, "storage consumer", replicaConfig)
	if err != nil {
		log.Error("failed to decode config file", zap.Error(err))
		return nil, err
	}

	err = replicaConfig.ValidateAndAdjust(upstreamURI)
	if err != nil {
		log.Error("failed to validate replica config", zap.Error(err))
		return nil, err
	}

	if replicaConfig.Sink.Protocol != config.ProtocolCsv.String() {
		return nil, fmt.Errorf("data encoded in protocol %s is not supported yet",
			replicaConfig.Sink.Protocol)
	}

	bs, err := storage.ParseBackend(upstreamURIStr, nil)
	if err != nil {
		log.Error("failed to parse storage backend", zap.Error(err))
		return nil, err
	}

	storage, err := storage.New(ctx, bs, &storage.ExternalStorageOptions{
		SendCredentials: false,
		S3Retryer:       rcommon.DefaultS3Retryer(),
	})
	if err != nil {
		log.Error("failed to create external storage", zap.Error(err))
		return nil, err
	}

	errCh := make(chan error, 1)
	s, err := sink.New(ctx, model.DefaultChangeFeedID("storage-consumer"),
		downstreamURIStr, config.GetDefaultReplicaConfig(), errCh)
	if err != nil {
		log.Error("failed to create sink", zap.Error(err))
		return nil, err
	}

	return &consumer{
		sink:            s,
		replicationCfg:  replicaConfig,
		externalStorage: storage,
		tableIdxMap:     make(map[dmlPathKey]uint64),
		tableTsMap:      make(map[dmlPathKey]uint64),
		tableIDGenerator: &fakeTableIDGenerator{
			tableIDs: make(map[string]int64),
		},
	}, nil
}

// map1 - map2
func (c *consumer) diffTwoMaps(map1, map2 map[dmlPathKey]uint64) map[dmlPathKey]fileIndexRange {
	resMap := make(map[dmlPathKey]fileIndexRange)
	for k, v := range map1 {
		if _, ok := map2[k]; !ok {
			resMap[k] = fileIndexRange{
				start: 1,
				end:   v,
			}
		} else if v > map2[k] {
			resMap[k] = fileIndexRange{
				start: map2[k] + 1,
				end:   v,
			}
		}
	}

	return resMap
}

// getNewFiles returns dml files in specific ranges.
func (c *consumer) getNewFiles(ctx context.Context) (map[dmlPathKey]fileIndexRange, error) {
	m := make(map[dmlPathKey]fileIndexRange)
	opt := &storage.WalkOption{SubDir: ""}

	origTableMap := make(map[dmlPathKey]uint64, len(c.tableIdxMap))
	for k, v := range c.tableIdxMap {
		origTableMap[k] = v
	}

	schemaSet := make(map[schemaPathKey]struct{})
	err := c.externalStorage.WalkDir(ctx, opt, func(path string, size int64) error {
		var dmlkey dmlPathKey
		var schemaKey schemaPathKey

		if strings.HasSuffix(path, "metadata") {
			return nil
		}

		if strings.HasSuffix(path, "schema.json") {
			err := schemaKey.parseSchemaFilePath(path)
			if err != nil {
				log.Error("failed to parse schema file path", zap.Error(err))
			} else {
				schemaSet[schemaKey] = struct{}{}
			}

			return nil
		}

		fileIdx, err := dmlkey.parseDMLFilePath(c.replicationCfg.Sink.CSVConfig.DateSeparator, path)
		if err != nil {
			log.Error("failed to parse dml file path", zap.Error(err))
			// skip handling this file
			return nil
		}

		if _, ok := c.tableIdxMap[dmlkey]; !ok || fileIdx >= c.tableIdxMap[dmlkey] {
			c.tableIdxMap[dmlkey] = fileIdx
		}

		return nil
	})
	if err != nil {
		return m, err
	}

	// filter out those files whose "schema.json" file has not been generated yet.
	for key := range c.tableIdxMap {
		schemaKey := key.schemaPathKey
		// cannot find the scheme file, filter out the item.
		if _, ok := schemaSet[schemaKey]; !ok {
			delete(c.tableIdxMap, key)
		}
	}

	m = c.diffTwoMaps(c.tableIdxMap, origTableMap)
	return m, err
}

// emitDMLEvents decodes RowChangedEvents from file content and emit them.
func (c *consumer) emitDMLEvents(ctx context.Context, tableID int64, pathKey dmlPathKey, content []byte) error {
	var events []*model.RowChangedEvent
	var tableDetail cloudstorage.TableDetail

	schemaFilePath := pathKey.schemaPathKey.generagteSchemaFilePath()
	schemaContent, err := c.externalStorage.ReadFile(ctx, schemaFilePath)
	if err != nil {
		return errors.Trace(err)
	}

	err = json.Unmarshal(schemaContent, &tableDetail)
	if err != nil {
		return errors.Trace(err)
	}
	tableInfo, err := tableDetail.ToTableInfo()
	if err != nil {
		return errors.Trace(err)
	}

	decoder, err := csv.NewBatchDecoder(ctx, c.replicationCfg.Sink.CSVConfig, tableInfo, content)
	if err != nil {
		return errors.Trace(err)
	}

	cnt := 0
	for {
		tp, hasNext, err := decoder.HasNext()
		if err != nil {
			log.Error("failed to decode message", zap.Error(err))
			return err
		}
		if !hasNext {
			break
		}
		cnt++

		if tp == model.MessageTypeRow {
			row, err := decoder.NextRowChangedEvent()
			if err != nil {
				log.Error("failed to get next row changed event", zap.Error(err))
				return errors.Trace(err)
			}

			if _, ok := c.tableTsMap[pathKey]; !ok || row.CommitTs >= c.tableTsMap[pathKey] {
				c.tableTsMap[pathKey] = row.CommitTs
			} else {
				log.Warn("row changed event commit ts fallback, ignore",
					zap.Uint64("commitTs", row.CommitTs),
					zap.Uint64("tableMaxCommitTs", c.tableTsMap[pathKey]),
					zap.Any("row", row),
				)
				continue
			}
			row.Table.TableID = tableID
			events = append(events, row)
		}
	}
	log.Info("decode success", zap.Int("decodeRowsCnt", cnt),
		zap.Int("filteredRowsCnt", len(events)))

	err = c.sink.EmitRowChangedEvents(ctx, events...)
	return err
}

func (c *consumer) run(ctx context.Context) error {
	ticker := time.NewTicker(flushInterval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		fileMap, err := c.getNewFiles(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		keys := make([]dmlPathKey, 0, len(fileMap))
		for k := range fileMap {
			keys = append(keys, k)
		}

		if len(keys) == 0 {
			log.Info("no new files found since last round")
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].schema != keys[j].schema {
				return keys[i].schema < keys[j].schema
			}
			if keys[i].table != keys[j].table {
				return keys[i].table < keys[j].table
			}
			if keys[i].tableVersion != keys[j].tableVersion {
				return keys[i].tableVersion < keys[j].tableVersion
			}
			if keys[i].partitionNum != keys[j].partitionNum {
				return keys[i].partitionNum < keys[j].partitionNum
			}
			return keys[i].date < keys[j].date
		})

		for _, k := range keys {
			v := fileMap[k]
			for i := v.start; i <= v.end; i++ {
				filePath := k.generateDMLFilePath(i)
				content, err := c.externalStorage.ReadFile(ctx, filePath)
				if err != nil {
					return errors.Trace(err)
				}
				tableID := c.tableIDGenerator.generateFakeTableID(
					k.schema, k.table, k.partitionNum)
				err = c.emitDMLEvents(ctx, tableID, k, content)
				if err != nil {
					return errors.Trace(err)
				}

				resolvedTs := model.NewResolvedTs(c.tableTsMap[k])
				_, err = c.sink.FlushRowChangedEvents(ctx, tableID, resolvedTs)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
}

// copied from kafka-consumer
type fakeTableIDGenerator struct {
	tableIDs       map[string]int64
	currentTableID int64
	mu             sync.Mutex
}

func (g *fakeTableIDGenerator) generateFakeTableID(schema, table string, partition int64) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	key := quotes.QuoteSchema(schema, table)
	if partition != 0 {
		key = fmt.Sprintf("%s.`%d`", key, partition)
	}
	if tableID, ok := g.tableIDs[key]; ok {
		return tableID
	}
	g.currentTableID++
	g.tableIDs[key] = g.currentTableID
	return g.currentTableID
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	consumer, err := newConsumer(ctx)
	if err != nil {
		log.Error("failed to create storage consumer", zap.Error(err))
		os.Exit(1)
	}

	if err := consumer.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
		log.Error("error occurred while running consumer", zap.Error(err))
		os.Exit(1)
	}
	os.Exit(0)
}
