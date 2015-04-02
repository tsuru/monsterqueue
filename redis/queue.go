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

type QueueRedis struct {
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
	q := &QueueRedis{
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

func (q *QueueRedis) RegisterTask(task monsterqueue.Task) error {
	if _, isRegistered := q.tasks[task.Name()]; isRegistered {
		return errors.New("task already registered")
	}
	q.tasks[task.Name()] = task
	return nil
}

func (q *QueueRedis) Enqueue(taskName string, params monsterqueue.JobParams) (monsterqueue.Job, error) {
	conn := q.pool.Get()
	defer conn.Close()
	j := JobRedis{
		Id:     randomString(),
		Task:   taskName,
		Params: params,
		queue:  q,
	}
	data, err := j.Serialize()
	if err != nil {
		return nil, err
	}
	_, err = conn.Do("LPUSH", q.enqueuedKey(), data)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

func (q *QueueRedis) EnqueueWait(taskName string, params monsterqueue.JobParams, timeout time.Duration) (monsterqueue.Job, error) {
	j := JobRedis{
		Id:     randomString(),
		Task:   taskName,
		Params: params,
		queue:  q,
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
	conn := q.pool.Get()
	defer conn.Close()
	_, err = conn.Do("LPUSH", q.enqueuedKey(), data)
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

func (q *QueueRedis) ProcessLoop() {
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

func (q *QueueRedis) Stop() {
	close(q.done)
	q.Wait()
}

func (q *QueueRedis) Wait() {
	q.wg.Wait()
}

func (q *QueueRedis) ResetStorage() error {
	conn := q.pool.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", q.enqueuedKey(), q.resultKey(), q.runningKey())
	return err
}

func (q *QueueRedis) RetrieveJob(jobId string) (monsterqueue.Job, error) {
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

func (q *QueueRedis) receiveMessage(psc *redis.PubSubConn, key string) (chan []byte, error) {
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

func (q *QueueRedis) waitForMessage() error {
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
		q.moveToResult(&job, nil, err)
		q.wg.Done()
		return err
	}
	task, _ := q.tasks[job.Task]
	if task == nil {
		err := fmt.Errorf("unregistered task name %q", job.TaskName)
		q.moveToResult(&job, nil, err)
		q.wg.Done()
		return err
	}
	go func() {
		defer q.wg.Done()
		task.Run(&job)
	}()
	return nil
}

func (q *QueueRedis) moveToResult(job *JobRedis, result monsterqueue.JobResult, jobErr error) ([]byte, error) {
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
	resultMsg := jobResultMessage{RawJob: job.rawJob, Result: result}
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

func (q *QueueRedis) publishResult(jobId string, resultData []byte) (int, error) {
	conn := q.pool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("PUBLISH", q.resultPubSubKey(jobId), resultData))
}

func (q *QueueRedis) key(base string) string {
	base = fmt.Sprintf("monsterqueue:%s", base)
	if q.config.KeyPrefix != "" {
		base = fmt.Sprintf("%s:%s", q.config.KeyPrefix, base)
	}
	return base
}

func (q *QueueRedis) enqueuedKey() string {
	return q.key("enqueued")
}

func (q *QueueRedis) resultKey() string {
	return q.key("result")
}

func (q *QueueRedis) runningKey() string {
	return q.key("running")
}

func (q *QueueRedis) resultPubSubKey(jobId string) string {
	return q.key(fmt.Sprintf("result:%s", jobId))
}

func (q *QueueRedis) dial() (redis.Conn, error) {
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
