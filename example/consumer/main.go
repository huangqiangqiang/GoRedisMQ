package main

import (
	"fmt"

	"github.com/huangqiangqiang/goredismq/example/consumer/consumer"
	"github.com/huangqiangqiang/goredismq/example/consumer/util"
)

func main() {
	response, err := util.Post("http://localhost:7890/sub", map[string]interface{}{
		"topic": "test",
		"name":  "consumer1",
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	client := consumer.NewConsumer(response["redis_addr"].(string), response["queue_name"].(string))
	client.StartConsuming()
}
