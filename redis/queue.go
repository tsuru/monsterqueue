// Copyright 2015 monsterqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package redis

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/tsuru/monsterqueue"
	"github.com/tsuru/monsterqueue/log"
)

type queueRedis struct {
	config *QueueConfig
	pool   *redis.Pool
	tasks  map[string]monsterqueue.Task
	done   chan bool
	wg     sync.WaitGroup
}

type QueueConfig struct {
	Host      string // Redis host
	Port      int    // Redis port
	Password  string // Redis password (can be empty)
	Db        int    // Redis db (default to 0)
	KeyPrefix string // Prefix for all keys storede in Redis

	// Maximum number of idle connections in redis connection pool, defaults
	// to 10.
	PoolMaxIdle int

	// Timeout for idle connections in redis connection pool. If 0, idle connections
	// won't be closed.
	PoolIdleTimeout time.Duration

	// Wait time blocked in redis, waiting for new messages to arrive. This value
	// should be greater than one second.
	MaxBlockTime time.Duration
}

// Creates a new queue. The QueueConfig parameter will tell us how Redis to
// connect to redis, among other things. This command will fail if the Redis
// server is not available.
//
// Tasks registered in this queue instance will run when `ProcessLoop` is
// called in this *same* instance.
func NewQueue(conf QueueConfig) (monsterqueue.Queue, error) {
	if conf.PoolMaxIdle == 0 {
		conf.PoolMaxIdle = 10
	}
	q := &queueRedis{
		config: &conf,
		tasks:  make(map[string]monsterqueue.Task),
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

func (q *queueRedis) RegisterTask(task monsterqueue.Task) error {
	if _, isRegistered := q.tasks[task.Name()]; isRegistered {
		return errors.New("task already registered")
	}
	q.tasks[task.Name()] = task
	return nil
}

func (q *queueRedis) Enqueue(taskName string, params monsterqueue.JobParams) (monsterqueue.Job, error) {
	conn := q.pool.Get()
	defer conn.Close()
	j := jobRedis{
		Id:      randomString(),
		Task:    taskName,
		Params:  params,
		Created: time.Now().UTC(),
		queue:   q,
	}
	data, err := j.Serialize()
	if err != nil {
		return nil, err
	}
	err = q.lowEnqueue(j.Id, data)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

func (q *queueRedis) EnqueueWait(taskName string, params monsterqueue.JobParams, timeout time.Duration) (monsterqueue.Job, error) {
	j := jobRedis{
		Id:      randomString(),
		Task:    taskName,
		Params:  params,
		Created: time.Now().UTC(),
		queue:   q,
	}
	pscConn, err := q.dial()
	if err != nil {
		return nil, err
	}
	psc := redis.PubSubConn{Conn: pscConn}
	key := q.resultPubSubKey(j.Id)
	resultChan, err := q.receiveMessage(&psc, key)
	if err != nil {
		return nil, err
	}
	data, err := j.Serialize()
	if err != nil {
		return nil, err
	}
	err = q.lowEnqueue(j.Id, data)
	if err != nil {
		return nil, err
	}
	select {
	case resultData := <-resultChan:
		j, err := newJobFromResultRaw(resultData)
		return &j, err
	case <-time.After(timeout):
		psc.Unsubscribe(key)
		psc.Close()
	}
	return &j, monsterqueue.ErrQueueWaitTimeout
}

func (q *queueRedis) ProcessLoop() {
	for {
		q.wg.Add(1)
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

func (q *queueRedis) Stop() {
	close(q.done)
	q.Wait()
}

func (q *queueRedis) Wait() {
	q.wg.Wait()
}

func (q *queueRedis) ResetStorage() error {
	conn := q.pool.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", q.enqueuedKey(), q.resultKey(), q.runningKey())
	return err
}

func (q *queueRedis) RetrieveJob(jobId string) (monsterqueue.Job, error) {
	conn := q.pool.Get()
	defer conn.Close()
	rawResult, err := redis.Bytes(conn.Do("HGET", q.resultKey(), jobId))
	if err != nil {
		return nil, err
	}
	if len(rawResult) == 0 {
		return nil, nil
	}
	job, err := newJobFromResultRaw(rawResult)
	return &job, err
}

func (q *queueRedis) lowEnqueue(id string, data []byte) error {
	conn := q.pool.Get()
	defer conn.Close()
	err := conn.Send("MULTI")
	if err != nil {
		return err
	}
	result := jobResultMessage{RawJob: data}
	dataRestult, _ := result.Serialize()
	err = conn.Send("HSET", q.resultKey(), id, dataRestult)
	if err != nil {
		return err
	}
	err = conn.Send("LPUSH", q.enqueuedKey(), data)
	if err != nil {
		return err
	}
	_, err = conn.Do("EXEC")
	if err != nil {
		return err
	}
	return nil
}

func (q *queueRedis) receiveMessage(psc *redis.PubSubConn, key string) (chan []byte, error) {
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

func (q *queueRedis) waitForMessage() error {
	conn := q.pool.Get()
	defer conn.Close()
	blockTime := int(q.config.MaxBlockTime / time.Second)
	if blockTime == 0 {
		blockTime = 1
	}
	rawJob, err := redis.Bytes(conn.Do("BRPOPLPUSH", q.enqueuedKey(), q.runningKey(), blockTime))
	if err != nil {
		q.wg.Done()
		if err == redis.ErrNil {
			return nil
		}
		return err
	}
	job, err := newJobFromRaw(rawJob)
	job.queue = q
	if err != nil {
		data, _ := q.moveToResult(&job, nil, err)
		q.publishResult(job.Id, data)
		q.wg.Done()
		return err
	}
	err = q.saveJobEntry(&job)
	if err != nil {
		data, _ := q.moveToResult(&job, nil, err)
		q.publishResult(job.Id, data)
		q.wg.Done()
		return err
	}
	task, _ := q.tasks[job.Task]
	if task == nil {
		err := fmt.Errorf("unregistered task name %q", job.Task)
		data, _ := q.moveToResult(&job, nil, err)
		q.publishResult(job.Id, data)
		q.wg.Done()
		return err
	}
	go func() {
		defer q.wg.Done()
		task.Run(&job)
		if !job.done {
			data, _ := q.moveToResult(&job, nil, monsterqueue.ErrNoJobResultSet)
			q.publishResult(job.Id, data)
		}
	}()
	return nil
}

func (q *queueRedis) saveJobEntry(job *jobRedis) error {
	job.resultMessage = &jobResultMessage{RawJob: job.rawJob, Started: time.Now().UTC()}
	data, _ := job.resultMessage.Serialize()
	conn := q.pool.Get()
	defer conn.Close()
	_, err := conn.Do("HSET", q.resultKey(), job.Id, data)
	if err != nil {
		return err
	}
	return nil
}

func (q *queueRedis) moveToResult(job *jobRedis, result monsterqueue.JobResult, jobErr error) ([]byte, error) {
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
	resultMsg := jobResultMessage{RawJob: job.rawJob, Result: result, Done: time.Now().UTC()}
	if job.resultMessage != nil {
		resultMsg.Started = job.resultMessage.Started
	}
	if jobErr != nil {
		resultMsg.Error = jobErr.Error()
	}
	data, _ := resultMsg.Serialize()
	err = conn.Send("HSET", q.resultKey(), job.Id, data)
	if err != nil {
		return nil, err
	}
	_, err = conn.Do("EXEC")
	if err == nil {
		job.done = true
	}
	return data, err
}

func (q *queueRedis) publishResult(jobId string, resultData []byte) (int, error) {
	conn := q.pool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("PUBLISH", q.resultPubSubKey(jobId), resultData))
}

func (q *queueRedis) key(base string) string {
	base = fmt.Sprintf("monsterqueue:%s", base)
	if q.config.KeyPrefix != "" {
		base = fmt.Sprintf("%s:%s", q.config.KeyPrefix, base)
	}
	return base
}

func (q *queueRedis) enqueuedKey() string {
	return q.key("enqueued")
}

func (q *queueRedis) resultKey() string {
	return q.key("result")
}

func (q *queueRedis) runningKey() string {
	return q.key("running")
}

func (q *queueRedis) resultPubSubKey(jobId string) string {
	return q.key(fmt.Sprintf("result:%s", jobId))
}

func (q *queueRedis) dial() (redis.Conn, error) {
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

func randomString() string {
	id := make([]byte, 32)
	io.ReadFull(rand.Reader, id)
	return hex.EncodeToString(id)
}
