// Copyright 2015 monsterqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mongodb_test

import (
	"github.com/tsuru/monsterqueue/mongodb"
	"github.com/tsuru/monsterqueue/monsterqueuetest"
	"gopkg.in/check.v1"
)

func (s *S) TestQueueRegisterTask(c *check.C) {
	queue, err := mongodb.NewQueue(mongodb.QueueConfig{Url: mongoTestUrl})
	c.Assert(err, check.IsNil)
	monsterqueuetest.TestQueueRegisterTask(queue, c)
}

func (s *S) TestQueueEnqueueAndProcess(c *check.C) {
	queue, err := mongodb.NewQueue(mongodb.QueueConfig{Url: mongoTestUrl})
	c.Assert(err, check.IsNil)
	monsterqueuetest.TestQueueEnqueueAndProcess(queue, c)
}

func (s *S) TestQueueEnqueueWaitAndProcess(c *check.C) {
	queue, err := mongodb.NewQueue(mongodb.QueueConfig{Url: mongoTestUrl})
	c.Assert(err, check.IsNil)
	monsterqueuetest.TestQueueEnqueueWaitAndProcess(queue, c)
}

func (s *S) TestQueueEnqueueWaitTimeout(c *check.C) {
	queue, err := mongodb.NewQueue(mongodb.QueueConfig{Url: mongoTestUrl})
	c.Assert(err, check.IsNil)
	monsterqueuetest.TestQueueEnqueueWaitTimeout(queue, c)
}

func (s *S) TestQueueEnqueueWaitError(c *check.C) {
	queue, err := mongodb.NewQueue(mongodb.QueueConfig{Url: mongoTestUrl})
	c.Assert(err, check.IsNil)
	monsterqueuetest.TestQueueEnqueueWaitError(queue, c)
}

func (s *S) TestQueueEnqueueWaitInvalidTaskName(c *check.C) {
	queue, err := mongodb.NewQueue(mongodb.QueueConfig{Url: mongoTestUrl})
	c.Assert(err, check.IsNil)
	monsterqueuetest.TestQueueEnqueueWaitInvalidTaskName(queue, c)
}
