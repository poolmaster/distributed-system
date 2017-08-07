package mapreduce

import (
  "fmt"
  "sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
  var inFiles []string
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
    inFiles = mapFiles
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
    for i := 0; i < nReduce; i++ {
      inFiles = append(inFiles, mergeName(jobName, i))
    }
	}

  //generic code to schedule both map and reduce tasks
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
  var taskWg sync.WaitGroup
  for i, f := range inFiles{
    wkAddr := <- registerChan 
    wkArgs := DoTaskArgs{jobName, f, phase, i, n_other}
    taskWg.Add(1)  //increment waitgrp counter
    go func() {
      defer func() {
        taskWg.Done()
        registerChan <- wkAddr //put back for reuse 
      }()
      for { //retrying until task succeed 
        ok := call(wkAddr, "Worker.DoTask", wkArgs, nil) 
        if ok { break; }
      	fmt.Printf("Cleanup: RPC %s error.\n", wkAddr)
        //registerChan <- wkAddr //dont put server back
        wkAddr = <- registerChan 
      }
    }()
  }
  debug("wait for all tasks to complete.\n")
  taskWg.Wait()  //wait until all tasks in current phase done
	fmt.Printf("Schedule: %v phase done\n", phase)
}
