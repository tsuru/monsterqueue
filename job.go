// Copyright 2015 redisqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package redisqueue

import (
	"encoding/json"
	"errors"
)

var ErrNoJobResult = errors.New("no result available")

type JobParams map[string]interface{}
type JobResult interface{}

type jobResultMessage struct {
	RawJob []byte
	Error  string
	Result JobResult
}

type Job struct {
	Id            string
	TaskName      string
	Params        JobParams
	rawJob        []byte
	queue         *Queue
	resultMessage *jobResultMessage
}

func newJobFromRaw(data []byte) (Job, error) {
	job := Job{
		rawJob: data,
	}
	err := json.Unmarshal(data, &job)
	return job, err
}

func newJobFromResultRaw(data []byte) (Job, error) {
	var result jobResultMessage
	err := json.Unmarshal(data, &result)
	if err != nil {
		return Job{}, err
	}
	job, err := newJobFromRaw(result.RawJob)
	job.resultMessage = &result
	return job, err
}

func (m *jobResultMessage) Serialize() ([]byte, error) {
	return json.Marshal(m)
}

func (m *jobResultMessage) Job() (Job, error) {
	return newJobFromRaw(m.RawJob)
}

func (j *Job) Success(result JobResult) (bool, error) {
	resultData, err := j.queue.moveToResult(j, result, nil)
	if err != nil {
		return false, err
	}
	receivers, err := j.queue.publishResult(j.Id, resultData)
	return receivers > 0, err
}

func (j *Job) Error(jobErr error) (bool, error) {
	resultData, err := j.queue.moveToResult(j, nil, jobErr)
	if err != nil {
		return false, err
	}
	receivers, err := j.queue.publishResult(j.Id, resultData)
	return receivers > 0, err
}

func (j *Job) Serialize() ([]byte, error) {
	return json.Marshal(j)
}

func (j *Job) Result() (JobResult, error) {
	if j.resultMessage == nil {
		return nil, ErrNoJobResult
	}
	var err error
	if j.resultMessage.Error != "" {
		err = errors.New(j.resultMessage.Error)
	}
	return j.resultMessage.Result, err
}
