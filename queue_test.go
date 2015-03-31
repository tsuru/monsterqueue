// Copyright 2015 redisqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package redisqueue_test

import (
	"errors"
	"sync"
	"time"

	"github.com/tsuru/redisqueue"
	"gopkg.in/check.v1"
)

type TestTask struct {
	callCount int
	acked     bool
	params    redisqueue.JobParams
}

func (t *TestTask) Run(j *redisqueue.Job) {
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
	queue, err := redisqueue.NewQueue(redisqueue.QueueConfig{KeyPrefix: redisKeyPrefix})
	c.Assert(err, check.IsNil)
	task := &TestTask{}
	err = queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	// _, ok := queue.tasks["test-task"]
	// c.Assert(ok, check.Equals, true)
}

func (s *S) TestQueueEnqueueAndProcessManualWait(c *check.C) {
	queue, err := redisqueue.NewQueue(redisqueue.QueueConfig{KeyPrefix: redisKeyPrefix})
	c.Assert(err, check.IsNil)
	task := &TestTask{}
	err = queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	job, err := queue.Enqueue("test-task", redisqueue.JobParams{"a": "b"})
	c.Assert(err, check.IsNil)
	queue.Stop()
	queue.ProcessLoop()
	queue.Wait()
	c.Assert(task.callCount, check.Equals, 1)
	c.Assert(task.params, check.DeepEquals, redisqueue.JobParams{"a": "b"})
	c.Assert(task.acked, check.Equals, false)
	job2, err := queue.RetrieveJob(job.Id)
	c.Assert(err, check.IsNil)
	result, err := job2.Result()
	c.Assert(err, check.IsNil)
	c.Assert(result, check.Equals, "my result")
	c.Assert(job2.Id, check.Equals, job.Id)
	c.Assert(job2.Params, check.DeepEquals, job.Params)
	c.Assert(job2.TaskName, check.Equals, job.TaskName)
}

func (s *S) TestQueueEnqueueWaitAndProcess(c *check.C) {
	queue, err := redisqueue.NewQueue(redisqueue.QueueConfig{KeyPrefix: redisKeyPrefix})
	c.Assert(err, check.IsNil)
	task := &TestTask{}
	err = queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	wg := sync.WaitGroup{}
	wg.Add(1)
	var job *redisqueue.Job
	go func() {
		defer wg.Done()
		var err error
		job, err = queue.EnqueueWait("test-task", redisqueue.JobParams{"a": "b"}, 2*time.Second)
		c.Assert(err, check.IsNil)
	}()
	queue.Stop()
	queue.ProcessLoop()
	wg.Wait()
	c.Assert(job.TaskName, check.Equals, "test-task")
	result, err := job.Result()
	c.Assert(err, check.IsNil)
	c.Assert(result, check.Equals, "my result")
	queue.Wait()
	c.Assert(task.callCount, check.Equals, 1)
	c.Assert(task.params, check.DeepEquals, redisqueue.JobParams{"a": "b"})
	c.Assert(task.acked, check.Equals, true)
	c.Assert(job.Params, check.DeepEquals, redisqueue.JobParams{"a": "b"})
	c.Assert(job.TaskName, check.Equals, "test-task")
}

func (s *S) TestQueueEnqueueWaitTimeout(c *check.C) {
	queue, err := redisqueue.NewQueue(redisqueue.QueueConfig{KeyPrefix: redisKeyPrefix})
	c.Assert(err, check.IsNil)
	task := &TestTask{}
	err = queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	wg := sync.WaitGroup{}
	wg.Add(1)
	var job *redisqueue.Job
	go func() {
		defer wg.Done()
		var err error
		job, err = queue.EnqueueWait("test-task", redisqueue.JobParams{"sleep": true}, 500*time.Millisecond)
		c.Assert(err, check.Equals, redisqueue.ErrQueueWaitTimeout)
	}()
	queue.Stop()
	queue.ProcessLoop()
	wg.Wait()
	c.Assert(job.TaskName, check.Equals, "test-task")
	_, err = job.Result()
	c.Assert(err, check.Equals, redisqueue.ErrNoJobResult)
	queue.Wait()
	c.Assert(task.callCount, check.Equals, 1)
	c.Assert(task.params, check.DeepEquals, redisqueue.JobParams{"sleep": true})
	c.Assert(task.acked, check.Equals, false)
	job2, err := queue.RetrieveJob(job.Id)
	c.Assert(err, check.IsNil)
	result, err := job2.Result()
	c.Assert(err, check.IsNil)
	c.Assert(result, check.Equals, "my result")
}

func (s *S) TestQueueEnqueueWaitError(c *check.C) {
	queue, err := redisqueue.NewQueue(redisqueue.QueueConfig{KeyPrefix: redisKeyPrefix})
	c.Assert(err, check.IsNil)
	task := &TestTask{}
	err = queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	wg := sync.WaitGroup{}
	wg.Add(1)
	var job *redisqueue.Job
	go func() {
		defer wg.Done()
		var err error
		job, err = queue.EnqueueWait("test-task", redisqueue.JobParams{"err": "fear is the mind-killer"}, 2*time.Second)
		c.Assert(err, check.IsNil)
	}()
	queue.Stop()
	queue.ProcessLoop()
	wg.Wait()
	result, err := job.Result()
	c.Assert(result, check.IsNil)
	c.Assert(err, check.ErrorMatches, "fear is the mind-killer")
	queue.Wait()
	c.Assert(task.callCount, check.Equals, 1)
	c.Assert(task.params, check.DeepEquals, redisqueue.JobParams{"err": "fear is the mind-killer"})
	c.Assert(task.acked, check.Equals, true)
}
