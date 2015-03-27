// Copyright 2015 redisqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package redisqueue

import (
	"encoding/json"
)

type JobParams map[string]interface{}
type JobResult interface{}

type JobResultMessage struct {
	RawJob []byte
	Error  string
	Result JobResult
}

type Job struct {
	Id       string
	TaskName string
	Params   JobParams
	rawJob   []byte
	queue    *Queue
}

func NewJobFromRaw(data []byte) (Job, error) {
	job := Job{
		rawJob: data,
	}
	err := json.Unmarshal(data, &job)
	return job, err
}

func NewJobResultFromRaw(data []byte) (JobResultMessage, error) {
	var result JobResultMessage
	err := json.Unmarshal(data, &result)
	return result, err
}

func (m *JobResultMessage) Serialize() ([]byte, error) {
	return json.Marshal(m)
}

func (m *JobResultMessage) Job() (Job, error) {
	return NewJobFromRaw(m.RawJob)
}

func (j *Job) Ack(result JobResult) (bool, error) {
	resultData, err := j.queue.moveToResult(j, result, nil)
	if err != nil {
		return false, err
	}
	receivers, err := j.queue.publishResult(j.Id, resultData)
	return receivers > 0, err
}

func (j *Job) Serialize() ([]byte, error) {
	return json.Marshal(j)
}
