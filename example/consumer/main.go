package main

import (
	"fmt"

	"github.com/huangqiangqiang/GoRedisMQ/example/consumer/consumer"
	"github.com/huangqiangqiang/GoRedisMQ/example/consumer/util"
)

func main() {
	response, err := util.Post("http://localhost:7890/sub?topic=test", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	client := consumer.NewConsumer(response["redis_addr"].(string), response["queue_name"].(string))
	client.StartConsuming()
}
