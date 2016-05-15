package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	// sort 这里指的将 相同key的value存入一个数组中
	// 流程如下：1. 找到属于自己的中间文件，2。读出这些文件中的每一个key/value对 3. sort
	// 4.创建 名字叫做mergername的file  5. 对每一个key/value对调用reducef 6。将每一个reducef的结果写入到disk

	type sortedKeyValue struct {
		key   string
		value []string
	}

	var mykeyValue []KeyValue
	var sortedkv []sortedKeyValue

	for i := 0; i < nMap; i++ {
		//step1:
		str := reduceName(jobName, i, reduceTaskNumber)

		file, err := os.Open(str)
		if err != nil {
			fmt.Println(err.Error())
		}

		enc := json.NewDecoder(file)

		var tmpkv KeyValue
		//step2:
		for {
			err := enc.Decode(&tmpkv)
			mykeyValue = append(mykeyValue, tmpkv)
			if err != nil {
				break
			}
		}

		file.Close()
	}

	fmt.Println("get before ******************")
	//step3:
	for _, x := range mykeyValue {
		var flag = 0 //统计中是否有这个 key,有的话 将value加入这个key的value 中，没有的话，新加入一个sortedkeyvalue
		for _, y := range sortedkv {
			if x.Key == y.key {
				flag = 1
				y.value = append(y.value, x.Value)
			}
		}
		if flag == 0 {
			tmpkv := new(sortedKeyValue)
			tmpkv.key = x.Key
			tmpkv.value = append(tmpkv.value, x.Value)
			sortedkv = append(sortedkv, *tmpkv)
		}
		//fmt.Println("get here ******************")
	}

	//step4:
	fileMergeName := mergeName(jobName, reduceTaskNumber)
	file, err := os.Create(fileMergeName)
	if err != nil {
		fmt.Println(err.Error())
	}

	enc := json.NewEncoder(file)
	//step5:
	for _, x := range sortedkv {
		str := reduceF(x.key, x.value)

		//step6:
		enc.Encode(KeyValue{x.key, str})
	}
	file.Close()

}
