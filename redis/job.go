// Copyright 2015 monsterqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package redis

import (
	"encoding/json"
	"errors"

	"github.com/tsuru/monsterqueue"
)

type jobResultMessage struct {
	RawJob []byte
	Error  string
	Result monsterqueue.JobResult
}

type JobRedis struct {
	Id            string
	Task          string
	Params        monsterqueue.JobParams
	rawJob        []byte
	queue         *QueueRedis
	resultMessage *jobResultMessage
}

func newJobFromRaw(data []byte) (JobRedis, error) {
	job := JobRedis{
		rawJob: data,
	}
	err := json.Unmarshal(data, &job)
	return job, err
}

func newJobFromResultRaw(data []byte) (JobRedis, error) {
	var result jobResultMessage
	err := json.Unmarshal(data, &result)
	if err != nil {
		return JobRedis{}, err
	}
	job, err := newJobFromRaw(result.RawJob)
	job.resultMessage = &result
	return job, err
}

func (m *jobResultMessage) Serialize() ([]byte, error) {
	return json.Marshal(m)
}

func (m *jobResultMessage) Job() (JobRedis, error) {
	return newJobFromRaw(m.RawJob)
}

func (j *JobRedis) ID() string {
	return j.Id
}

func (j *JobRedis) Parameters() monsterqueue.JobParams {
	return j.Params
}

func (j *JobRedis) TaskName() string {
	return j.Task
}

func (j *JobRedis) Success(result monsterqueue.JobResult) (bool, error) {
	resultData, err := j.queue.moveToResult(j, result, nil)
	if err != nil {
		return false, err
	}
	receivers, err := j.queue.publishResult(j.Id, resultData)
	return receivers > 0, err
}

func (j *JobRedis) Error(jobErr error) (bool, error) {
	resultData, err := j.queue.moveToResult(j, nil, jobErr)
	if err != nil {
		return false, err
	}
	receivers, err := j.queue.publishResult(j.Id, resultData)
	return receivers > 0, err
}

func (j *JobRedis) Serialize() ([]byte, error) {
	return json.Marshal(j)
}

func (j *JobRedis) Result() (monsterqueue.JobResult, error) {
	if j.resultMessage == nil {
		return nil, monsterqueue.ErrNoJobResult
	}
	var err error
	if j.resultMessage.Error != "" {
		err = errors.New(j.resultMessage.Error)
	}
	return j.resultMessage.Result, err
}
