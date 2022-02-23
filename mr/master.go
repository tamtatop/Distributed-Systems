package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "errors"
import "time"


type Master struct {
	// Your definitions here.
	nReduce int
	fileNames []string

	assignedMapTasks []bool
	doneMapTasks []bool
	mapsDone int

	assignedReduceTasks []bool
	doneReduceTasks []bool
	allDone int

	mutex sync.Mutex
	
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) AssignTask (args *interface{}, reply *TaskAssign) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for i, isDone := range m.doneMapTasks {
		if !isDone && !m.assignedMapTasks[i] {
			reply.TaskType = MapTask
			reply.FileName = m.fileNames[i]
			reply.TaskIndex = i
			reply.NReduce = m.nReduce

			m.assignedMapTasks[i] = true

			go func() {
				time.Sleep(10000)

				m.mutex.Lock()
				if !m.doneMapTasks[i] {
					m.assignedMapTasks[i] = false
				}
				m.mutex.Unlock()
			}()
			return nil
		}
	}

	if m.mapsDone < len(m.doneMapTasks){
		reply.TaskType = WaitForTask
		return nil
	} // Maps Done

	for i, isDone := range m.doneReduceTasks {
		if !isDone && !m.assignedReduceTasks[i] {
			reply.TaskType = ReduceTask
			reply.TaskIndex = i
			reply.NMap = len(m.doneMapTasks)
			
			m.assignedReduceTasks[i] = true

			go func() {
				time.Sleep(10000)

				m.mutex.Lock()
				if !m.doneReduceTasks[i] {
					m.assignedReduceTasks[i] = false
				}
				m.mutex.Unlock()
			}()
			return nil
		}
	} // Reduces Done

	
	if m.allDone < len(m.doneReduceTasks){
		reply.TaskType = NoMoreTasks
		return nil
	} // All Done
	return nil
}

func (m *Master) MarkFinishedTask (args *TaskFinished, reply *interface{}) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	switch args.TaskType {
	case MapTask:
		if !m.doneMapTasks[args.TaskIndex] {m.mapsDone++}
		m.doneMapTasks[args.TaskIndex] = true
	case ReduceTask:
		if !m.doneReduceTasks[args.TaskIndex] {m.allDone++}
		m.doneReduceTasks[args.TaskIndex] = true
	default:
		return errors.New("wrong TaskType")
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.allDone >= len(m.doneReduceTasks);
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.nReduce = nReduce
	m.fileNames = files

	m.assignedMapTasks = make([]bool, len(files))
	m.doneMapTasks = make([]bool, len(files))

	m.assignedReduceTasks = make([]bool, nReduce)
	m.doneReduceTasks = make([]bool, nReduce)
	
	m.server()
	return &m
}
