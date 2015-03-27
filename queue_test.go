// Copyright 2015 redisqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package redisqueue

import (
	"errors"
	"time"

	"gopkg.in/check.v1"
)

type TestTask struct {
	callCount int
	acked     bool
	params    JobParams
}

func (t *TestTask) Run(j *Job) error {
	if j.Params["err"] != nil {
		return errors.New(j.Params["err"].(string))
	}
	t.params = j.Params
	acked, err := j.Ack("my result")
	t.acked = acked
	t.callCount++
	return err
}

func (t *TestTask) Name() string {
	return "test-task"
}

func (s *S) TestQueueRegisterTask(c *check.C) {
	queue, err := NewQueue(QueueConfig{KeyPrefix: redisKeyPrefix})
	c.Assert(err, check.IsNil)
	task := &TestTask{}
	err = queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	_, ok := queue.tasks["test-task"]
	c.Assert(ok, check.Equals, true)
}

func (s *S) TestQueueEnqueueAndProcessManualWait(c *check.C) {
	queue, err := NewQueue(QueueConfig{KeyPrefix: redisKeyPrefix})
	c.Assert(err, check.IsNil)
	task := &TestTask{}
	err = queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	job, err := queue.Enqueue("test-task", JobParams{"a": "b"})
	c.Assert(err, check.IsNil)
	queue.Stop()
	queue.ProcessLoop()
	waitFor(c, func() bool {
		return task.callCount > 0
	})
	c.Assert(task.callCount, check.Equals, 1)
	c.Assert(task.params, check.DeepEquals, JobParams{"a": "b"})
	c.Assert(task.acked, check.Equals, false)
	result, err := queue.JobResult(job.Id)
	c.Assert(err, check.IsNil)
	c.Assert(result.Error, check.Equals, "")
	c.Assert(result.Result, check.Equals, "my result")
	jobFromResult, err := result.Job()
	c.Assert(err, check.IsNil)
	c.Assert(jobFromResult.Id, check.Equals, job.Id)
	c.Assert(jobFromResult.Params, check.DeepEquals, job.Params)
	c.Assert(jobFromResult.TaskName, check.Equals, job.TaskName)
}

func waitFor(c *check.C, fn func() bool) {
	done := make(chan bool)
	go func() {
		for range time.Tick(100 * time.Millisecond) {
			select {
			case <-done:
				return
			default:
			}
			if fn() {
				close(done)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		defer close(done)
		c.Fatal("Timed out.")
	}
}
