// Copyright 2015 monsterqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"html/template"
	"net"
	"net/http"
	"os"
	"regexp"
	"sort"
	"time"

	"github.com/codegangsta/cli"
	"github.com/tsuru/monsterqueue"
	"github.com/tsuru/monsterqueue/mongodb"
	"github.com/tsuru/monsterqueue/redis"
)

var (
	deletePath = regexp.MustCompile(`^/(.+?)/delete$`)
	retryPath  = regexp.MustCompile(`^/(.+?)/retry$`)
)

type monsterHandler struct {
	queue    monsterqueue.Queue
	template *template.Template
}

type jobData struct {
	ID       string
	Task     string
	Success  bool
	Result   monsterqueue.JobResult
	State    string
	Enqueued string
	Started  string
	Done     string
	Params   monsterqueue.JobParams
}

func initQueue(c *cli.Context) (monsterqueue.Queue, error) {
	backend := c.String("backend")
	var queue monsterqueue.Queue
	var err error
	if backend == "mongodb" {
		conf := mongodb.QueueConfig{
			Url:              c.String("mongodb-url"),
			Database:         c.String("mongodb-database"),
			CollectionPrefix: c.String("mongodb-prefix"),
		}
		queue, err = mongodb.NewQueue(conf)
		if err != nil {
			return nil, err
		}
	} else if backend == "redis" {
		conf := redis.QueueConfig{
			Host:      c.String("redis-host"),
			Port:      c.Int("redis-port"),
			Password:  c.String("redis-password"),
			Db:        c.Int("redis-db"),
			KeyPrefix: c.String("redis-prefix"),
		}
		queue, err = redis.NewQueue(conf)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("invalid backend %q", backend)
	}
	return queue, nil
}

func runServer(c *cli.Context) {
	indexTemplate, err := template.New("index").Parse(indexTemplateData)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: could not parse template: %s\n\n", err)
		cli.ShowAppHelp(c)
		os.Exit(1)
	}
	queue, err := initQueue(c)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: could not create queue: %s\n\n", err)
		cli.ShowAppHelp(c)
		os.Exit(1)
	}
	binding := c.String("binding")
	listener, err := net.Listen("tcp", binding)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: could not create listener: %s\n\n", err)
		cli.ShowAppHelp(c)
		os.Exit(1)
	}
	http.Handle("/", &monsterHandler{
		queue:    queue,
		template: indexTemplate,
	})
	fmt.Printf("Listening at %q\n", binding)
	http.Serve(listener, nil)
}

func httpErr(w http.ResponseWriter, err error) {
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
}

func (h *monsterHandler) index(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.queue.ListJobs()
	if err != nil {
		httpErr(w, err)
		return
	}
	sort.Sort(sort.Reverse(monsterqueue.JobList(jobs)))
	entries := make([]jobData, len(jobs))
	for i, j := range jobs {
		status := j.Status()
		var result monsterqueue.JobResult
		success := true
		if status.State == monsterqueue.JobStateDone {
			var err error
			result, err = j.Result()
			if err != nil {
				success = false
				result = err.Error()
			}
		}
		data := jobData{
			ID:       j.ID(),
			Task:     j.TaskName(),
			Success:  success,
			Result:   result,
			State:    status.State,
			Enqueued: status.Enqueued.Format(time.Stamp),
			Started:  status.Started.Format(time.Stamp),
			Done:     status.Done.Format(time.Stamp),
			Params:   j.Parameters(),
		}
		entries[i] = data
	}
	err = h.template.Execute(w, entries)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error trying to run template: %s\n", err)
	}
}

func (h *monsterHandler) delete(jobId string, w http.ResponseWriter, r *http.Request) {
	err := h.queue.DeleteJob(jobId)
	if err != nil {
		httpErr(w, err)
		return
	}
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

func (h *monsterHandler) retry(jobId string, w http.ResponseWriter, r *http.Request) {
	job, err := h.queue.RetrieveJob(jobId)
	if err != nil {
		httpErr(w, err)
		return
	}
	_, err = h.queue.Enqueue(job.TaskName(), job.Parameters())
	if err != nil {
		httpErr(w, err)
		return
	}
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

func (h *monsterHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		h.index(w, r)
		return
	}
	matches := deletePath.FindStringSubmatch(r.URL.Path)
	if len(matches) > 1 && r.Method == "POST" {
		h.delete(matches[1], w, r)
		return
	}
	matches = retryPath.FindStringSubmatch(r.URL.Path)
	if len(matches) > 1 && r.Method == "POST" {
		h.retry(matches[1], w, r)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "binding",
			Value: "0.0.0.0:8778",
			Usage: "binding address",
		},
		cli.StringFlag{
			Name:  "backend",
			Usage: "chosen backend, mongodb or redis",
		},
		cli.StringFlag{
			Name: "mongodb-url",
		},
		cli.StringFlag{
			Name: "mongodb-database",
		},
		cli.StringFlag{
			Name: "mongodb-prefix",
		},
		cli.StringFlag{
			Name: "redis-host",
		},
		cli.IntFlag{
			Name: "redis-port",
		},
		cli.StringFlag{
			Name: "redis-password",
		},
		cli.IntFlag{
			Name: "redis-db",
		},
		cli.StringFlag{
			Name: "redis-prefix",
		},
	}
	app.Version = "0.0.1"
	app.Name = "monsterweb"
	app.Usage = "monsterqueue dashboard application"
	app.Action = runServer
	app.Run(os.Args)
}
