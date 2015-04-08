// Copyright 2015 monsterqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package redis_test

import (
	"testing"

	"github.com/tsuru/monsterqueue/monsterqueuetest"
	"github.com/tsuru/monsterqueue/redis"
	"gopkg.in/check.v1"
)

var redisKeyPrefix = "monsterqueue-test-prefix"

func Test(t *testing.T) {
	check.Suite(&monsterqueuetest.Suite{
		SetUpTestFunc: func(s *monsterqueuetest.Suite, c *check.C) {
			var err error
			s.Queue, err = redis.NewQueue(redis.QueueConfig{KeyPrefix: redisKeyPrefix})
			c.Assert(err, check.IsNil)
			s.Queue.ResetStorage()
		},
	})
	check.TestingT(t)
}
