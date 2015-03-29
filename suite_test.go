// Copyright 2015 redisqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package redisqueue_test

import (
	"testing"

	"github.com/garyburd/redigo/redis"
	"gopkg.in/check.v1"
)

func Test(t *testing.T) { check.TestingT(t) }

type S struct{}

var _ = check.Suite(&S{})

var redisKeyPrefix = "redisqueue-test-prefix"

func ClearRedisKeys(keysPattern string, c *check.C) {
	redisConn, err := redis.Dial("tcp", "127.0.0.1:6379")
	c.Assert(err, check.IsNil)
	defer redisConn.Close()
	result, err := redisConn.Do("KEYS", keysPattern)
	c.Assert(err, check.IsNil)
	keys := result.([]interface{})
	for _, key := range keys {
		keyName := string(key.([]byte))
		redisConn.Do("DEL", keyName)
	}
}

func (s *S) SetUpTest(c *check.C) {
	ClearRedisKeys(redisKeyPrefix+"*", c)
}
