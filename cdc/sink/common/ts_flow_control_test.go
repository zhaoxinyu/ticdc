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
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

type tsFlowControlSuite struct{}

var _ = check.Suite(&tsFlowControlSuite{})

func goconsume(controller *TableTsFlowController, ts uint64) chan struct{} {
	readyCh := make(chan struct{}, 1)
	go func() {
		readyCh <- struct{}{}
		controller.Consume(ts, 1)
		close(readyCh)
	}()
	select {
	case <-time.After(500 * time.Millisecond):
		panic("timeout")
	case <-readyCh:
	}
	return readyCh
}

func (s *tsFlowControlSuite) TestTableTsFlowControlBasic(c *check.C) {
	defer testleak.AfterTest(c)()

	upperBound := 10
	tsFlowControl := NewReactiveTsFlowControl(uint64(upperBound))
	controller := NewTableTsFlowController(tsFlowControl)

	for i := 0; i <= upperBound; i++ {
		controller.Consume(uint64(i), 1)
	}

	overUpperBound := uint64(upperBound) + 1
	readyCh := goconsume(controller, overUpperBound)

	select {
	case <-time.After(500 * time.Millisecond):
	case <-readyCh:
		c.Fatal("must be blocked")
	}

	// Consumer requests more data
	upperBound++
	tsFlowControl.Request(uint64(upperBound))

	select {
	case <-time.After(time.Second):
		c.Fatal("timeout")
	case <-readyCh:
	}
}

func (s *tsFlowControlSuite) TestTableTsFlowControlConcurrent(c *check.C) {
	defer testleak.AfterTest(c)()

	upperBound := 10
	tsFlowControl := NewReactiveTsFlowControl(uint64(upperBound))
	controller := NewTableTsFlowController(tsFlowControl)

	overUpperBound := uint64(upperBound) + 1
	readys := make([]chan struct{}, 0, 512)
	for i := 0; i < cap(readys); i++ {
		ready := goconsume(controller, overUpperBound)
		readys = append(readys, ready)
	}
	select {
	case <-time.After(500 * time.Millisecond):
	case <-readys[0]:
		c.Fatal("must be blocked")
	}

	// Consumer requests more data
	upperBound++
	tsFlowControl.Request(uint64(upperBound))

	for _, ready := range readys {
		select {
		case <-time.After(time.Second):
			c.Fatal("timeout")
		case <-ready:
		}
	}
}

// TestTableTsFlowControlAbort verifies that Abort works
func (s *tsFlowControlSuite) TestTableTsFlowControlAbort(c *check.C) {
	defer testleak.AfterTest(c)()

	upperBound := 10
	tsFlowControl := NewReactiveTsFlowControl(uint64(upperBound))
	controller := NewTableTsFlowController(tsFlowControl)

	overUpperBound := uint64(upperBound) + 1
	readyCh := goconsume(controller, overUpperBound)
	select {
	case <-time.After(500 * time.Millisecond):
	case <-readyCh:
		c.Fatal("must be blocked")
	}

	tsFlowControl.Abort()

	select {
	case <-time.After(500 * time.Millisecond):
		c.Fatal("timeout")
	case <-readyCh:
	}
}

// TestMemoryQuotaReleaseZero verifies that releasing 0 bytes is successful
func (s *tsFlowControlSuite) TestTsFlowControlReleaseZero(c *check.C) {
	defer testleak.AfterTest(c)()

	controller := NewReactiveTsFlowControl(1024)
	controller.Request(0)
}
