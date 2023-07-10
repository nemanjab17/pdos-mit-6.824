package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
	"strings"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// loop forever with sleep
	for {
		// sleep 1 second
		time.Sleep(time.Second)
		// ask for a job
		filename, status, id, nReduce, phase := CallGetJob()

		if status == "empty" {
			continue
		} else if status == "done" {
			fmt.Printf("all jobs done!\n")
			break
		}
		// do the job
		if phase == 1 {
			fmt.Printf("job %v\n", filename)
			intermediate := [][]KeyValue{}
			for i := 0; i < nReduce; i++ {
				intermediate = append(intermediate, []KeyValue{})
			}
			
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			
			for _, kv := range kva {
				reduceId := ihash(kv.Key) % nReduce
				intermediate[reduceId] = append(intermediate[reduceId], kv)
			}
			
			//iterate over nReduce
			for i := 0; i < nReduce; i++ {
				oname := "mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(i) + "-tmp"
				ofile, _ := os.Create(oname)
				for _, kv := range intermediate[i] {
					fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
				}
			}
		} else if phase == 2 {
			intermediate := []KeyValue{}
			// read all intermediate files for this reduce task
			for i := 0; i < 7; i++ {
				iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(id)
				file, err := os.Open(iname)
				if err != nil {
					log.Fatalf("cannot open %v", iname)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", iname)
				}
				file.Close()
				os.Remove(iname)
				
				for _, line := range strings.Split(string(content), "\n") {
					if len(line) == 0 {
						continue
					}
					parts := strings.Split(line, " ")
					kv := KeyValue{parts[0], parts[1]}
					intermediate = append(intermediate, kv)
				}
			}

			// sort by key
			sort.Sort(ByKey(intermediate))

			oname := "mr-out" + "-" + strconv.Itoa(id)
			ofile, _ := os.Create(oname)
			
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()



			
		}
		// report the job is done
		CallFinishJob(id)

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}


func CallFinishJob(id int)  {

	args := FinishJobArgs{
		Id: id,
	}
	reply := FinishJobReply{}

	ok := call("Coordinator.FinishJob", &args, &reply)
	if ok {
		fmt.Print("call finish job success!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallGetJob() (string, string, int, int, int) {

	args := GetJobArgs{
	}
	reply := GetJobReply{}

	ok := call("Coordinator.GetJob", &args, &reply)
	if ok {
		fmt.Printf("reply.Job %v\n", reply.NReduce)
		return reply.Job, reply.Status, reply.Id, reply.NReduce, reply.Phase
	} else {
		fmt.Printf("call failed!\n")
	}
	return "", "", 0, 0, 0
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
