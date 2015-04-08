// Copyright 2015 monsterqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package monsterqueuetest

import (
	"errors"
	"sync"
	"time"

	"github.com/tsuru/monsterqueue"
	"gopkg.in/check.v1"
)

type Suite struct {
	Queue         monsterqueue.Queue
	SetUpTestFunc func(*Suite, *check.C)
}

type TestTask struct {
	callCount int
	acked     bool
	params    monsterqueue.JobParams
}

func (t *TestTask) Run(j monsterqueue.Job) {
	params := j.Parameters()
	if params["sleep"] != nil {
		time.Sleep(1 * time.Second)
	}
	acked := false
	if params["err"] != nil {
		acked, _ = j.Error(errors.New(params["err"].(string)))
	} else {
		acked, _ = j.Success("my result")
	}
	t.params = params
	t.acked = acked
	t.callCount++
}

func (t *TestTask) Name() string {
	return "test-task"
}

func (s *Suite) SetUpTest(c *check.C) {
	if s.SetUpTestFunc != nil {
		s.SetUpTestFunc(s, c)
	}
}

func (s *Suite) TestQueueRegisterTask(c *check.C) {
	task := &TestTask{}
	err := s.Queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
}

func (s *Suite) TestQueueEnqueueAndProcess(c *check.C) {
	task := &TestTask{}
	err := s.Queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	job, err := s.Queue.Enqueue("test-task", monsterqueue.JobParams{"a": "b"})
	c.Assert(err, check.IsNil)
	s.Queue.Stop()
	s.Queue.ProcessLoop()
	s.Queue.Wait()
	c.Assert(task.callCount, check.Equals, 1)
	c.Assert(task.params, check.DeepEquals, monsterqueue.JobParams{"a": "b"})
	c.Assert(task.acked, check.Equals, false)
	job2, err := s.Queue.RetrieveJob(job.ID())
	c.Assert(err, check.IsNil)
	result, err := job2.Result()
	c.Assert(err, check.IsNil)
	c.Assert(result, check.Equals, "my result")
	c.Assert(job2.ID(), check.Equals, job.ID())
	c.Assert(job2.Parameters(), check.DeepEquals, job.Parameters())
	c.Assert(job2.TaskName(), check.Equals, job.TaskName())
}

func (s *Suite) TestQueueEnqueueWaitAndProcess(c *check.C) {
	task := &TestTask{}
	err := s.Queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	wg := sync.WaitGroup{}
	wg.Add(1)
	var job monsterqueue.Job
	go func() {
		defer wg.Done()
		var err error
		job, err = s.Queue.EnqueueWait("test-task", monsterqueue.JobParams{"a": "b"}, 2*time.Second)
		c.Assert(err, check.IsNil)
	}()
	go func() { time.Sleep(2 * time.Second); s.Queue.Stop() }()
	s.Queue.ProcessLoop()
	wg.Wait()
	c.Assert(job.TaskName(), check.Equals, "test-task")
	result, err := job.Result()
	c.Assert(err, check.IsNil)
	c.Assert(result, check.Equals, "my result")
	s.Queue.Wait()
	c.Assert(task.callCount, check.Equals, 1)
	c.Assert(task.params, check.DeepEquals, monsterqueue.JobParams{"a": "b"})
	c.Assert(task.acked, check.Equals, true)
	c.Assert(job.Parameters(), check.DeepEquals, monsterqueue.JobParams{"a": "b"})
	c.Assert(job.TaskName(), check.Equals, "test-task")
}

func (s *Suite) TestQueueEnqueueWaitTimeout(c *check.C) {
	task := &TestTask{}
	err := s.Queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	wg := sync.WaitGroup{}
	wg.Add(1)
	var job monsterqueue.Job
	go func() {
		defer wg.Done()
		var err error
		job, err = s.Queue.EnqueueWait("test-task", monsterqueue.JobParams{"sleep": true}, 500*time.Millisecond)
		c.Assert(err, check.Equals, monsterqueue.ErrQueueWaitTimeout)
	}()
	go func() { time.Sleep(2 * time.Second); s.Queue.Stop() }()
	s.Queue.ProcessLoop()
	wg.Wait()
	c.Assert(job.TaskName(), check.Equals, "test-task")
	_, err = job.Result()
	c.Assert(err, check.Equals, monsterqueue.ErrNoJobResult)
	s.Queue.Wait()
	c.Assert(task.callCount, check.Equals, 1)
	c.Assert(task.params, check.DeepEquals, monsterqueue.JobParams{"sleep": true})
	c.Assert(task.acked, check.Equals, false)
	job2, err := s.Queue.RetrieveJob(job.ID())
	c.Assert(err, check.IsNil)
	result, err := job2.Result()
	c.Assert(err, check.IsNil)
	c.Assert(result, check.Equals, "my result")
}

func (s *Suite) TestQueueEnqueueWaitError(c *check.C) {
	task := &TestTask{}
	err := s.Queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	wg := sync.WaitGroup{}
	wg.Add(1)
	var job monsterqueue.Job
	go func() {
		defer wg.Done()
		var err error
		job, err = s.Queue.EnqueueWait("test-task", monsterqueue.JobParams{"err": "fear is the mind-killer"}, 2*time.Second)
		c.Assert(err, check.IsNil)
	}()
	go func() { time.Sleep(2 * time.Second); s.Queue.Stop() }()
	s.Queue.ProcessLoop()
	wg.Wait()
	result, err := job.Result()
	c.Assert(result, check.IsNil)
	c.Assert(err, check.ErrorMatches, "fear is the mind-killer")
	s.Queue.Wait()
	c.Assert(task.callCount, check.Equals, 1)
	c.Assert(task.params, check.DeepEquals, monsterqueue.JobParams{"err": "fear is the mind-killer"})
	c.Assert(task.acked, check.Equals, true)
}

func (s *Suite) TestQueueEnqueueWaitInvalidTaskName(c *check.C) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	var job monsterqueue.Job
	go func() {
		defer wg.Done()
		var err error
		job, err = s.Queue.EnqueueWait("invalid-task", monsterqueue.JobParams{"a": "b"}, 2*time.Second)
		c.Assert(err, check.IsNil)
	}()
	go func() { time.Sleep(2 * time.Second); s.Queue.Stop() }()
	s.Queue.ProcessLoop()
	wg.Wait()
	c.Assert(job.TaskName(), check.Equals, "invalid-task")
	result, err := job.Result()
	c.Assert(err, check.ErrorMatches, ".*unregistered.*invalid-task.")
	c.Assert(result, check.IsNil)
}
