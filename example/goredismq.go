package main

import (
	MQ "github.com/huangqiangqiang/GoRedisMQ"
)

func main() {
	cnf := &MQ.Config{
		Broker:  "redis://:hqq@localhost:6379/0",
		Backend: "mongodb://lianluo:com.lianluo.tthigo@localhost:27017?authSource=admin",
	}
	MQ.StartHTTPServer(cnf)
}
