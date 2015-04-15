// Copyright 2015 monsterqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mongodb_test

import (
	"testing"

	"github.com/tsuru/monsterqueue/mongodb"
	"github.com/tsuru/monsterqueue/monsterqueuetest"
	"gopkg.in/check.v1"
)

var mongoTestUrl = "127.0.0.1:27017/queuetest"

func Test(t *testing.T) {
	check.Suite(&monsterqueuetest.Suite{
		SetUpTestFunc: func(s *monsterqueuetest.Suite, c *check.C) {
			var err error
			s.Queue, err = mongodb.NewQueue(mongodb.QueueConfig{Url: mongoTestUrl})
			c.Assert(err, check.IsNil)
			s.Queue.ResetStorage()
		},
	})
	check.Suite(&S{})
	check.TestingT(t)
}

type S struct{}

func (s *S) TestNewQueue(c *check.C) {
	_, err := mongodb.NewQueue(mongodb.QueueConfig{})
	c.Assert(err, check.ErrorMatches, "setting QueueConfig.Url is required")
	_, err = mongodb.NewQueue(mongodb.QueueConfig{Url: "127.0.0.1:27017"})
	c.Assert(err, check.ErrorMatches, "database name should be set in QueueConfig.Url or QueueConfig.Database")
	_, err = mongodb.NewQueue(mongodb.QueueConfig{Url: "127.0.0.1:27017", Database: "queuetest"})
	c.Assert(err, check.IsNil)
}
