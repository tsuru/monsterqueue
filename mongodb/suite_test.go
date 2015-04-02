// Copyright 2015 monsterqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mongodb_test

import (
	"testing"

	"github.com/tsuru/monsterqueue/mongodb"
	"gopkg.in/check.v1"
)

func Test(t *testing.T) { check.TestingT(t) }

type S struct{}

var _ = check.Suite(&S{})

var mongoTestUrl = "127.0.0.1:27017/queuetest"

func (s *S) SetUpTest(c *check.C) {
	queue, err := mongodb.NewQueue(mongodb.QueueConfig{Url: mongoTestUrl})
	c.Assert(err, check.IsNil)
	queue.ResetStorage()
}
