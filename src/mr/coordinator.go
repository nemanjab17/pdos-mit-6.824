package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)


type Job struct {
	id int
	file string
}

type Coordinator struct {
	// Your definitions here.
	jobs *list.List
	chs map[int]chan string
	runningJobsCount int32
	mu sync.Mutex
	nReduce int
	phase int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) assign() (string, int) {
	if c.jobs.Len() == 0 {
		return "done", 0
	}
	c.mu.Lock()
	el := c.jobs.Front()
	c.jobs.Remove(el)
	c.mu.Unlock()
	job := el.Value.(Job)
	// create a channel for this job
	ch := make(chan string)
	c.chs[job.id] = ch

	go func() {
		// wait 10 seconds for the job to finish
		atomic.AddInt32(&c.runningJobsCount, 1)
		select {
		case <-time.After(10 * time.Second):
			c.mu.Lock()
			c.jobs.PushBack(job)
			c.mu.Unlock()
			delete(c.chs, job.id)
			atomic.AddInt32(&c.runningJobsCount, -1)
			fmt.Printf("job %v timed out\n", job)
		case <-ch:
			// job finished
			delete(c.chs, job.id)
			// rename file from mr-jobid-tmp to mr-jobid
			if c.phase == 1 {
				for i := 0; i < c.nReduce; i++ {
					os.Rename("mr-" + strconv.Itoa(job.id) + "-" + strconv.Itoa(i) + "-tmp", "mr-" + strconv.Itoa(job.id) + "-" + strconv.Itoa(i))
				}
			} else {
				os.Rename("mr-out-" + strconv.Itoa(job.id) + "-tmp", "mr-out-" + strconv.Itoa(job.id))
			}
			atomic.AddInt32(&c.runningJobsCount, -1)

			if c.jobs.Len() == 0 && c.runningJobsCount == 0 {

				if c.phase == 1 {
					for i := 0; i < c.nReduce; i++ {
						c.jobs.PushBack(Job{i, "dummy"})
					}
					c.phase = 2
				}

			}
		}
	}()
	return job.file, job.id
}

func (c *Coordinator) GetJob(args *GetJobArgs, reply *GetJobReply) error {
	
		reply.Status = "ok"
		reply.Job, reply.Id = c.assign()
		reply.NReduce = c.nReduce
		reply.Phase = c.phase


	return nil
}

func (c *Coordinator) FinishJob(args *FinishJobArgs, reply *FinishJobReply) error {
	// check if channel exists
	if _, ok := c.chs[args.Id]; !ok {
		return nil
	}
	c.chs[args.Id] <- "done"
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.jobs.Len() == 0 && atomic.LoadInt32(&c.runningJobsCount) == 0  && c.phase == 2 {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.phase = 1
	c.chs = make(map[int]chan string)
	map_tasks := list.New()
	for i, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		file.Close()
		map_tasks.PushBack(Job{i, filename})
	}
	c.mu.Lock()
	c.jobs = map_tasks
	c.mu.Unlock()

	fmt.Printf("Map tasks: %d\n", c.jobs.Len())
	
	
	c.server()
	return &c
}
