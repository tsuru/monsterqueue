// Copyright 2015 monsterqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package monsterqueue

import (
	"errors"
	"time"
)

var ErrNoJobResult = errors.New("no result available")
var ErrQueueWaitTimeout = errors.New("timeout waiting for result")

type JobParams map[string]interface{}
type JobResult interface{}

type Job interface {
	Success(result JobResult) (bool, error)
	Error(jobErr error) (bool, error)
	Result() (JobResult, error)
	ID() string
	Parameters() JobParams
	TaskName() string
}

type Task interface {
	Run(job Job)
	Name() string
}

type Queue interface {
	RegisterTask(task Task) error
	Enqueue(taskName string, params JobParams) (Job, error)
	EnqueueWait(taskName string, params JobParams, timeout time.Duration) (Job, error)
	ProcessLoop()
	Stop()
	Wait()
	ResetStorage() error
	RetrieveJob(jobId string) (Job, error)
}
