// Copyright 2015 redisqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package redisqueue

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/tsuru/redisqueue/log"
)

var ErrQueueWaitTimeout = errors.New("timeout waiting for result")

type Task interface {
	Run(job *Job)
	Name() string
}

type Queue struct {
	config *QueueConfig
	pool   *redis.Pool
	tasks  map[string]Task
	done   chan bool
}

type QueueConfig struct {
	Host            string
	Port            int
	Password        string
	Db              string
	PoolMaxIdle     int
	KeyPrefix       string
	PoolIdleTimeout time.Duration
	MaxBlockTime    time.Duration
}

func (q *Queue) RegisterTask(task Task) error {
	if _, isRegistered := q.tasks[task.Name()]; isRegistered {
		return errors.New("task already registered")
	}
	q.tasks[task.Name()] = task
	return nil
}

func (q *Queue) Enqueue(taskName string, params JobParams) (*Job, error) {
	conn := q.pool.Get()
	defer conn.Close()
	j := Job{
		Id:       randomString(),
		TaskName: taskName,
		Params:   params,
		queue:    q,
	}
	data, err := j.Serialize()
	if err != nil {
		return nil, err
	}
	_, err = conn.Do("LPUSH", q.enqueuedKey(), data)
	return &j, err
}

func (q *Queue) receiveMessage(psc *redis.PubSubConn, key string) (chan []byte, error) {
	err := psc.Subscribe(key)
	if err != nil {
		return nil, err
	}
	dataChan := make(chan []byte)
	go func() {
		defer close(dataChan)
		defer psc.Unsubscribe(key)
		defer psc.Close()
		for {
			switch v := psc.Receive().(type) {
			case redis.Message:
				dataChan <- v.Data
				return
			case error:
				log.Errorf("Error receiving redis pub/sub message: %s", v.Error())
				return
			}
		}
	}()
	return dataChan, nil
}

func (q *Queue) EnqueueWait(taskName string, params JobParams, timeout time.Duration) (JobResultMessage, *Job, error) {
	var result JobResultMessage
	j := Job{
		Id:       randomString(),
		TaskName: taskName,
		Params:   params,
		queue:    q,
	}
	pscConn, err := q.dial()
	if err != nil {
		return result, nil, err
	}
	psc := redis.PubSubConn{Conn: pscConn}
	key := q.resultPubSubKey(j.Id)
	resultChan, err := q.receiveMessage(&psc, key)
	if err != nil {
		return result, nil, err
	}
	data, err := j.Serialize()
	if err != nil {
		return result, nil, err
	}
	conn := q.pool.Get()
	defer conn.Close()
	_, err = conn.Do("LPUSH", q.enqueuedKey(), data)
	select {
	case resultData := <-resultChan:
		result, err := NewJobResultFromRaw(resultData)
		return result, &j, err
	case <-time.After(timeout):
		psc.Unsubscribe(key)
		psc.Close()
	}
	return result, &j, ErrQueueWaitTimeout
}

func (q *Queue) ProcessLoop() {
	for {
		err := q.waitForMessage()
		if err != nil {
			log.Debugf("error getting message from queue: %s", err.Error())
		}
		select {
		case <-q.done:
			return
		}
	}
}

func (q *Queue) Stop() {
	close(q.done)
}

func (q *Queue) JobResult(jobId string) (JobResultMessage, error) {
	conn := q.pool.Get()
	defer conn.Close()
	var result JobResultMessage
	rawResult, err := redis.Bytes(conn.Do("HGET", q.resultKey(), jobId))
	if err != nil {
		return result, err
	}
	if len(rawResult) == 0 {
		return result, nil
	}
	return NewJobResultFromRaw(rawResult)
}

func (q *Queue) waitForMessage() error {
	conn := q.pool.Get()
	defer conn.Close()
	blockTime := int(q.config.MaxBlockTime / time.Second)
	if blockTime == 0 {
		blockTime = 1
	}
	rawJob, err := redis.Bytes(conn.Do("BRPOPLPUSH", q.enqueuedKey(), q.runningKey(), blockTime))
	if err != nil {
		return err
	}
	job, err := NewJobFromRaw(rawJob)
	job.queue = q
	if err != nil {
		q.moveToResult(&job, nil, err)
		return err
	}
	task, _ := q.tasks[job.TaskName]
	if task == nil {
		err := fmt.Errorf("unregistered task name %q", job.TaskName)
		q.moveToResult(&job, nil, err)
		return err
	}
	go task.Run(&job)
	return nil
}

func (q *Queue) moveToResult(job *Job, result JobResult, jobErr error) ([]byte, error) {
	conn := q.pool.Get()
	defer conn.Close()
	err := conn.Send("MULTI")
	if err != nil {
		return nil, err
	}
	err = conn.Send("LREM", q.runningKey(), 0, job.rawJob)
	if err != nil {
		return nil, err
	}
	resultMsg := JobResultMessage{RawJob: job.rawJob, Result: result}
	if jobErr != nil {
		resultMsg.Error = jobErr.Error()
	}
	data, _ := resultMsg.Serialize()
	err = conn.Send("HSET", q.resultKey(), job.Id, data)
	if err != nil {
		return nil, err
	}
	_, err = conn.Do("EXEC")
	return data, err
}

func (q *Queue) publishResult(jobId string, resultData []byte) (int, error) {
	conn := q.pool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("PUBLISH", q.resultPubSubKey(jobId), resultData))
}

func (q *Queue) key(base string) string {
	if q.config.KeyPrefix != "" {
		base = fmt.Sprintf("%s:%s", q.config.KeyPrefix, base)
	}
	return base
}

func (q *Queue) enqueuedKey() string {
	return q.key("enqueued")
}

func (q *Queue) resultKey() string {
	return q.key("result")
}

func (q *Queue) runningKey() string {
	return q.key("running")
}

func (q *Queue) resultPubSubKey(jobId string) string {
	return q.key(fmt.Sprintf("result:%s", jobId))
}

func (q *Queue) dial() (redis.Conn, error) {
	if q.config.Host == "" {
		q.config.Host = "127.0.0.1"
	}
	if q.config.Port == 0 {
		q.config.Port = 6379
	}
	conn, err := redis.Dial("tcp", fmt.Sprintf("%s:%d", q.config.Host, q.config.Port))
	if err != nil {
		return nil, err
	}
	if q.config.Password != "" {
		_, err = conn.Do("AUTH", q.config.Password)
		if err != nil {
			return nil, err
		}
	}
	_, err = conn.Do("SELECT", q.config.Db)
	return conn, err
}

func NewQueue(conf QueueConfig) (*Queue, error) {
	q := &Queue{
		config: &conf,
		tasks:  make(map[string]Task),
		done:   make(chan bool),
	}
	q.pool = &redis.Pool{
		MaxIdle:     conf.PoolMaxIdle,
		IdleTimeout: conf.PoolIdleTimeout,
		Dial:        q.dial,
	}
	conn := q.pool.Get()
	defer conn.Close()
	_, err := conn.Do("PING")
	return q, err
}

func randomString() string {
	id := make([]byte, 32)
	io.ReadFull(rand.Reader, id)
	return hex.EncodeToString(id)
}
