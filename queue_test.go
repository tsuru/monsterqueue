// Copyright 2015 redisqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package redisqueue

import (
	"errors"
	"sync"
	"time"

	"gopkg.in/check.v1"
)

type TestTask struct {
	callCount int
	acked     bool
	params    JobParams
}

func (t *TestTask) Run(j *Job) {
	if j.Params["sleep"] != nil {
		time.Sleep(1 * time.Second)
	}
	acked := false
	if j.Params["err"] != nil {
		acked, _ = j.Error(errors.New(j.Params["err"].(string)))
	} else {
		acked, _ = j.Success("my result")
	}
	t.params = j.Params
	t.acked = acked
	t.callCount++
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
	waitFor(c, func() bool { return task.callCount > 0 })
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

func (s *S) TestQueueEnqueueWaitAndProcess(c *check.C) {
	queue, err := NewQueue(QueueConfig{KeyPrefix: redisKeyPrefix})
	c.Assert(err, check.IsNil)
	task := &TestTask{}
	err = queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	wg := sync.WaitGroup{}
	wg.Add(1)
	var result JobResultMessage
	go func() {
		defer wg.Done()
		var err error
		var job *Job
		result, job, err = queue.EnqueueWait("test-task", JobParams{"a": "b"}, 2*time.Second)
		c.Assert(err, check.IsNil)
		c.Assert(job.TaskName, check.Equals, "test-task")
	}()
	queue.Stop()
	queue.ProcessLoop()
	wg.Wait()
	c.Assert(result.Error, check.Equals, "")
	c.Assert(result.Result, check.Equals, "my result")
	waitFor(c, func() bool { return task.callCount > 0 })
	c.Assert(task.callCount, check.Equals, 1)
	c.Assert(task.params, check.DeepEquals, JobParams{"a": "b"})
	c.Assert(task.acked, check.Equals, true)
	jobFromResult, err := result.Job()
	c.Assert(err, check.IsNil)
	c.Assert(jobFromResult.Params, check.DeepEquals, JobParams{"a": "b"})
	c.Assert(jobFromResult.TaskName, check.Equals, "test-task")
}

func (s *S) TestQueueEnqueueWaitTimeout(c *check.C) {
	queue, err := NewQueue(QueueConfig{KeyPrefix: redisKeyPrefix})
	c.Assert(err, check.IsNil)
	task := &TestTask{}
	err = queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	wg := sync.WaitGroup{}
	wg.Add(1)
	var job *Job
	go func() {
		defer wg.Done()
		var err error
		_, job, err = queue.EnqueueWait("test-task", JobParams{"sleep": true}, 500*time.Millisecond)
		c.Assert(err, check.Equals, ErrQueueWaitTimeout)
	}()
	queue.Stop()
	queue.ProcessLoop()
	wg.Wait()
	waitFor(c, func() bool { return task.callCount > 0 })
	c.Assert(task.callCount, check.Equals, 1)
	c.Assert(task.params, check.DeepEquals, JobParams{"sleep": true})
	c.Assert(task.acked, check.Equals, false)
	result, err := queue.JobResult(job.Id)
	c.Assert(err, check.IsNil)
	c.Assert(result.Error, check.Equals, "")
	c.Assert(result.Result, check.Equals, "my result")
}

func (s *S) TestQueueEnqueueWaitError(c *check.C) {
	queue, err := NewQueue(QueueConfig{KeyPrefix: redisKeyPrefix})
	c.Assert(err, check.IsNil)
	task := &TestTask{}
	err = queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	wg := sync.WaitGroup{}
	wg.Add(1)
	var result JobResultMessage
	go func() {
		defer wg.Done()
		var err error
		result, _, err = queue.EnqueueWait("test-task", JobParams{"err": "fear is the mind-killer"}, 2*time.Second)
		c.Assert(err, check.IsNil)
	}()
	queue.Stop()
	queue.ProcessLoop()
	wg.Wait()
	c.Assert(result.Result, check.IsNil)
	c.Assert(result.Error, check.Equals, "fear is the mind-killer")
	waitFor(c, func() bool { return task.callCount > 0 })
	c.Assert(task.callCount, check.Equals, 1)
	c.Assert(task.params, check.DeepEquals, JobParams{"err": "fear is the mind-killer"})
	c.Assert(task.acked, check.Equals, true)
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
