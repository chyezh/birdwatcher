package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type Resp struct {
	Data []Data `json:"data"`
}

type Data struct {
	InstanceId string   `json:"instanceId"`
	ETCDIP     []string `json:"etcdIp"`
	MilvusIP   []string `json:"milvusIp"`
}

func main() {
	decoder := json.NewDecoder(os.Stdin)
	var result Resp
	if err := decoder.Decode(&result); err != nil {
		panic(err)
	}
	for _, item := range result.Data {
		fmt.Printf("%s\t%s\n", item.InstanceId, item.ETCDIP[0])
	}
}
