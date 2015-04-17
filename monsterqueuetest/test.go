// Copyright 2015 monsterqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package monsterqueuetest

import (
	"errors"
	"sort"
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

type NoReturnTask struct {
	callCount int
}

func (t *NoReturnTask) Run(j monsterqueue.Job) {
	t.callCount++
}

func (t *NoReturnTask) Name() string {
	return "no-return-task"
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
	go s.Queue.ProcessLoop()
	s.Queue.Stop()
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
	status := job2.Status()
	c.Assert(status.State, check.Equals, monsterqueue.JobStateDone)
	c.Assert(status.Enqueued.IsZero(), check.Equals, false)
	c.Assert(status.Started.IsZero(), check.Equals, false)
	c.Assert(status.Done.IsZero(), check.Equals, false)
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
	go s.Queue.ProcessLoop()
	s.Queue.Stop()
	wg.Wait()
	c.Assert(job.TaskName(), check.Equals, "test-task")
	result, err := job.Result()
	c.Assert(err, check.IsNil)
	c.Assert(result, check.Equals, "my result")
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
	go s.Queue.ProcessLoop()
	s.Queue.Stop()
	wg.Wait()
	c.Assert(job.TaskName(), check.Equals, "test-task")
	_, err = job.Result()
	c.Assert(err, check.Equals, monsterqueue.ErrNoJobResult)
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
	go s.Queue.ProcessLoop()
	s.Queue.Stop()
	wg.Wait()
	result, err := job.Result()
	c.Assert(result, check.IsNil)
	c.Assert(err, check.ErrorMatches, "fear is the mind-killer")
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
	go s.Queue.ProcessLoop()
	s.Queue.Stop()
	wg.Wait()
	c.Assert(job.TaskName(), check.Equals, "invalid-task")
	result, err := job.Result()
	c.Assert(err, check.ErrorMatches, ".*unregistered.*invalid-task.")
	c.Assert(result, check.IsNil)
}

func (s *Suite) TestQueueEnqueueNoReturnTask(c *check.C) {
	task := &NoReturnTask{}
	err := s.Queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	job, err := s.Queue.Enqueue("no-return-task", nil)
	c.Assert(err, check.IsNil)
	go s.Queue.ProcessLoop()
	s.Queue.Stop()
	c.Assert(task.callCount, check.Equals, 1)
	job2, err := s.Queue.RetrieveJob(job.ID())
	c.Assert(err, check.IsNil)
	result, err := job2.Result()
	c.Assert(err, check.DeepEquals, monsterqueue.ErrNoJobResultSet)
	c.Assert(result, check.IsNil)
	c.Assert(job2.ID(), check.Equals, job.ID())
	c.Assert(job2.TaskName(), check.Equals, job.TaskName())
}

func (s *Suite) TestQueueStatusWithNoResult(c *check.C) {
	task := &TestTask{}
	err := s.Queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	job, err := s.Queue.Enqueue("test-task", monsterqueue.JobParams{"sleep": true})
	c.Assert(err, check.IsNil)
	status := job.Status()
	c.Assert(status.State, check.Equals, monsterqueue.JobStateEnqueued)
	c.Assert(status.Enqueued.IsZero(), check.Equals, false)
	c.Assert(status.Started.IsZero(), check.Equals, true)
	c.Assert(status.Done.IsZero(), check.Equals, true)
	result, err := job.Result()
	c.Assert(err, check.DeepEquals, monsterqueue.ErrNoJobResult)
	c.Assert(result, check.IsNil)
	job2, err := s.Queue.RetrieveJob(job.ID())
	c.Assert(err, check.IsNil)
	c.Assert(job2.ID(), check.Equals, job.ID())
	c.Assert(job2.TaskName(), check.Equals, job.TaskName())
	status2 := job2.Status()
	c.Assert(status2.State, check.DeepEquals, status.State)
	c.Assert(status2.Enqueued.IsZero(), check.Equals, false)
	c.Assert(status2.Started.IsZero(), check.Equals, true)
	c.Assert(status2.Done.IsZero(), check.Equals, true)
	go s.Queue.ProcessLoop()
	time.Sleep(500 * time.Millisecond)
	job3, err := s.Queue.RetrieveJob(job.ID())
	c.Assert(err, check.IsNil)
	status3 := job3.Status()
	c.Assert(status3.State, check.Equals, monsterqueue.JobStateRunning)
	c.Assert(status3.Enqueued.IsZero(), check.Equals, false)
	c.Assert(status3.Started.IsZero(), check.Equals, false)
	c.Assert(status3.Done.IsZero(), check.Equals, true)
	s.Queue.Stop()
}

func (s *Suite) TestQueueListJobs(c *check.C) {
	task := &TestTask{}
	err := s.Queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	job, err := s.Queue.Enqueue("test-task", monsterqueue.JobParams{"a": "b"})
	c.Assert(err, check.IsNil)
	go s.Queue.ProcessLoop()
	s.Queue.Stop()
	job2, err := s.Queue.Enqueue("test-task", monsterqueue.JobParams{"d": "e"})
	c.Assert(err, check.IsNil)
	jobs, err := s.Queue.ListJobs()
	c.Assert(err, check.IsNil)
	sort.Sort(monsterqueue.JobList(jobs))
	c.Assert(jobs, check.HasLen, 2)
	c.Assert(jobs[0].ID(), check.Equals, job.ID())
	c.Assert(jobs[0].Status().State, check.Equals, monsterqueue.JobStateDone)
	c.Assert(jobs[0].Parameters(), check.DeepEquals, monsterqueue.JobParams{"a": "b"})
	c.Assert(jobs[1].ID(), check.Equals, job2.ID())
	c.Assert(jobs[1].Status().State, check.Equals, monsterqueue.JobStateEnqueued)
	c.Assert(jobs[1].Parameters(), check.DeepEquals, monsterqueue.JobParams{"d": "e"})
}

func (s *Suite) TestQueueDeleteJob(c *check.C) {
	task := &TestTask{}
	err := s.Queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	job, err := s.Queue.Enqueue("test-task", monsterqueue.JobParams{"a": "b"})
	c.Assert(err, check.IsNil)
	go s.Queue.ProcessLoop()
	s.Queue.Stop()
	s.Queue.DeleteJob(job.ID())
	_, err = s.Queue.RetrieveJob(job.ID())
	c.Assert(err, check.Equals, monsterqueue.ErrNoSuchJob)
	jobs, err := s.Queue.ListJobs()
	c.Assert(err, check.IsNil)
	c.Assert(jobs, check.HasLen, 0)
}

func (s *Suite) TestJobStack(c *check.C) {
	task := &TestTask{}
	err := s.Queue.RegisterTask(task)
	c.Assert(err, check.IsNil)
	job, err := s.Queue.Enqueue("test-task", monsterqueue.JobParams{"a": "b"})
	c.Assert(err, check.IsNil)
	c.Assert(job.EnqueueStack(), check.Matches, `(?s).*TestJobStack.*`)
	go s.Queue.ProcessLoop()
	s.Queue.Stop()
	c.Assert(task.callCount, check.Equals, 1)
	job2, err := s.Queue.RetrieveJob(job.ID())
	c.Assert(err, check.IsNil)
	c.Assert(job2.EnqueueStack(), check.Equals, job.EnqueueStack())
}
