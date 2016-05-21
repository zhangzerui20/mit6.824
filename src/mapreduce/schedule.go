package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var nowTask int
	var completeTask int
	nowTask = 0
	completeTask = 0
	if phase == mapPhase {
		for {
			str := <-mr.registerChannel

			//this shows that the task on worker[n] is over
			if str[:6] == "worker" {
				completeTask++              //one task is over, add the complete task num
				if completeTask == ntasks { //all the task is over, break out the loop
					break
				}

				if nowTask != ntasks {
					//has task to assign
					var n int
					var temp string
					fmt.Sscanf(str, "%s %d", &temp, &n)
					go mr.taskFun(n, nowTask, ntasks, nios, phase)
					nowTask++
				}
			} else {
				//we assign task to this new worker
				mr.Lock()
				n := len(mr.workers) - 1 //the last worker

				if nowTask != ntasks {
					go mr.taskFun(n, nowTask, ntasks, nios, phase)
					nowTask++
				}
				mr.Unlock()
			}
		}
	} else if phase == reducePhase {
		//reduce do not need the filename
		//now the worker are all in idle
		//assign the task to workers
		mr.Lock()
		for i := 0; i < len(mr.workers); i++ {
			go mr.taskFun(i, nowTask, ntasks, nios, phase)
			nowTask++
			if nowTask == ntasks {
				break
			}
		}
		mr.Unlock()
		for {
			str := <-mr.registerChannel

			//this shows that the task on worker[n] is over
			if str[:6] == "worker" {
				completeTask++              //one task is over, add the complete task num
				if completeTask == ntasks { //all the task is over, kill the workers
					mr.killWorkers()
					break
				}

				if nowTask != ntasks {
					//has task to assign
					var n int
					var temp string
					fmt.Sscanf(str, "%s %d", &temp, &n)
					go mr.taskFun(n, nowTask, ntasks, nios, phase)
					nowTask++
				}
			} else {
				//we assign task to this new worker
				mr.Lock()
				n := len(mr.workers) - 1 //the last worker

				if nowTask != ntasks {
					go mr.taskFun(n, nowTask, ntasks, nios, phase)
					nowTask++
				}
				mr.Unlock()
			}

		}
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}

func (mr *Master) taskFun(n int, i int, ntasks int, nios int, phase jobPhase) {

	args := new(DoTaskArgs)
	args.File = mr.files[i]
	args.JobName = mr.jobName
	args.NumOtherPhase = nios
	args.Phase = phase
	args.TaskNumber = i

	// lock for the mr.workers
	mr.Lock()
	defer mr.Unlock()

	ok := call(mr.workers[n], "Worker.DoTask", args, new(struct{}))
	if ok == false {
		fmt.Printf("DoTask: worker %s dotask error\n", mr.workers[n])
	}
	str := fmt.Sprint("worker %d", n)
	mr.registerChannel <- str
}
