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
// See the License for the specific language governing pemissions and
// limitations under the License.

package redo

import (
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

// ReaderConfig is the configuration used by a redo log reader
type ReaderConfig struct {
	Storage string
}

// NewRedoReader creates a new redo log reader
func NewRedoReader(cfg *ReaderConfig) (reader LogReader, err error) {
	switch consistentStorage(cfg.Storage) {
	case consistentStorageBlackhole:
		reader = NewBlackholeReader()
	default:
		err = cerror.ErrConsistentStorage.GenWithStackByArgs(cfg.Storage)
	}
	return
}
