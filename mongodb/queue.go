// Copyright 2015 monsterqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mongodb

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/tsuru/monsterqueue"
	"github.com/tsuru/monsterqueue/log"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type queueMongoDB struct {
	config  *QueueConfig
	session *mgo.Session
	tasks   map[string]monsterqueue.Task
	done    chan bool
	wg      sync.WaitGroup
}

type QueueConfig struct {
	Url              string // MongoDB connection url
	CollectionPrefix string // Prefix for all collections created in MongoDB
}

// Creates a new queue. The QueueConfig parameter will tell us how to connect
// to mongodb. This command will fail if the MongoDB server is not available.
//
// Tasks registered in this queue instance will run when `ProcessLoop` is
// called in this *same* instance.
func NewQueue(conf QueueConfig) (monsterqueue.Queue, error) {
	q := &queueMongoDB{
		config: &conf,
		tasks:  make(map[string]monsterqueue.Task),
		done:   make(chan bool),
	}
	var err error
	q.session, err = mgo.Dial(conf.Url)
	if err != nil {
		return nil, err
	}
	db := q.session.DB("")
	if db.Name == "test" {
		q.session.Close()
		return nil, errors.New("mongodb connection url must include database")
	}
	return q, err
}

func (q *queueMongoDB) tasksColl() *mgo.Collection {
	s := q.session.Copy()
	name := "queue_tasks"
	if q.config.CollectionPrefix != "" {
		name = fmt.Sprintf("%s_%s", q.config.CollectionPrefix, name)
	}
	return s.DB("").C(name)
}

func (q *queueMongoDB) RegisterTask(task monsterqueue.Task) error {
	if _, isRegistered := q.tasks[task.Name()]; isRegistered {
		return errors.New("task already registered")
	}
	q.tasks[task.Name()] = task
	return nil
}

func (q *queueMongoDB) Enqueue(taskName string, params monsterqueue.JobParams) (monsterqueue.Job, error) {
	coll := q.tasksColl()
	defer coll.Database.Session.Close()
	j := jobMongoDB{
		Id:        bson.NewObjectId(),
		Task:      taskName,
		Params:    params,
		Timestamp: time.Now().UTC(),
		queue:     q,
	}
	err := coll.Insert(j)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

func (q *queueMongoDB) getDoneJob(jobId bson.ObjectId) (*jobMongoDB, error) {
	coll := q.tasksColl()
	defer coll.Database.Session.Close()
	var resultJob jobMongoDB
	err := coll.Find(bson.M{"_id": jobId, "resultmessage.done": true}).One(&resultJob)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &resultJob, nil
}

func (q *queueMongoDB) EnqueueWait(taskName string, params monsterqueue.JobParams, timeout time.Duration) (monsterqueue.Job, error) {
	j := jobMongoDB{
		Id:        bson.NewObjectId(),
		Task:      taskName,
		Params:    params,
		Timestamp: time.Now().UTC(),
		Waited:    true,
		queue:     q,
	}
	coll := q.tasksColl()
	defer coll.Database.Session.Close()
	err := coll.Insert(j)
	if err != nil {
		return nil, err
	}
	result := make(chan *jobMongoDB)
	quit := make(chan bool)
	go func() {
		for range time.Tick(200 * time.Millisecond) {
			select {
			case <-quit:
				return
			default:
			}
			job, err := q.getDoneJob(j.Id)
			if err != nil {
				log.Errorf("error trying to get job %s: %s", job.Id, err.Error())
			}
			if job != nil {
				result <- job
				return
			}
		}
	}()
	var resultJob *jobMongoDB
	select {
	case resultJob = <-result:
	case <-time.After(timeout):
		quit <- true
	}
	close(quit)
	close(result)
	if resultJob != nil {
		return resultJob, nil
	}
	err = coll.Update(bson.M{
		"_id":    j.Id,
		"waited": true,
	}, bson.M{"$set": bson.M{"waited": false}})
	if err == mgo.ErrNotFound {
		resultJob, err = q.getDoneJob(j.Id)
	}
	if err != nil {
		return &j, err
	}
	if resultJob != nil {
		return resultJob, nil
	}
	return &j, monsterqueue.ErrQueueWaitTimeout
}

func (q *queueMongoDB) ProcessLoop() {
	for {
		q.wg.Add(1)
		err := q.waitForMessage()
		if err != nil {
			log.Debugf("error getting message from queue: %s", err.Error())
		}
		select {
		case <-time.After(1 * time.Second):
		case <-q.done:
			return
		}
	}
}

func (q *queueMongoDB) Stop() {
	close(q.done)
	q.Wait()
}

func (q *queueMongoDB) Wait() {
	q.wg.Wait()
}

func (q *queueMongoDB) ResetStorage() error {
	coll := q.tasksColl()
	defer coll.Database.Session.Close()
	return coll.DropCollection()
}

func (q *queueMongoDB) RetrieveJob(jobId string) (monsterqueue.Job, error) {
	coll := q.tasksColl()
	defer coll.Database.Session.Close()
	var job jobMongoDB
	err := coll.FindId(bson.ObjectIdHex(jobId)).One(&job)
	if err != nil {
		return nil, err
	}
	return &job, err
}

func (q *queueMongoDB) waitForMessage() error {
	coll := q.tasksColl()
	defer coll.Database.Session.Close()
	var job jobMongoDB
	hostname, _ := os.Hostname()
	ownerData := jobOwnership{
		Name:      fmt.Sprintf("%s_%d", hostname, os.Getpid()),
		Owned:     true,
		Timestamp: time.Now().UTC(),
	}
	_, err := coll.Find(bson.M{
		"owner.owned":        false,
		"resultmessage.done": false,
	}).Sort("_id").Apply(mgo.Change{
		Update: bson.M{
			"$set": bson.M{"owner": ownerData},
		},
	}, &job)
	if err != nil {
		q.wg.Done()
		if err == mgo.ErrNotFound {
			return nil
		}
		return err
	}
	job.queue = q
	if err != nil {
		q.moveToResult(&job, nil, err)
		q.wg.Done()
		return err
	}
	task, _ := q.tasks[job.Task]
	if task == nil {
		err := fmt.Errorf("unregistered task name %q", job.Task)
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

func (q *queueMongoDB) moveToResult(job *jobMongoDB, result monsterqueue.JobResult, jobErr error) error {
	var resultMsg jobResultMessage
	resultMsg.Result = result
	resultMsg.Timestamp = time.Now().UTC()
	resultMsg.Done = true
	if jobErr != nil {
		resultMsg.Error = jobErr.Error()
	}
	job.ResultMessage = resultMsg
	coll := q.tasksColl()
	defer coll.Database.Session.Close()
	return coll.UpdateId(job.Id, bson.M{"$set": bson.M{"resultmessage": resultMsg, "owner.owned": false}})
}

func (q *queueMongoDB) publishResult(job *jobMongoDB) (bool, error) {
	coll := q.tasksColl()
	defer coll.Database.Session.Close()
	err := coll.Update(bson.M{"_id": job.Id, "waited": true}, bson.M{"$set": bson.M{"waited": false}})
	if err != nil {
		if err == mgo.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
