package main

import (
	"github.com/huangqiangqiang/GoRedisMQ"
)

func main() {
	cnf := &GoRedisMQ.Config{
		HttpServerPort: "7890",
		Broker:         "redis://:hqq@localhost:6379/0",
		Backend:        "mongodb://lianluo:com.lianluo.tthigo@localhost:27017?authSource=admin",
	}
	GoRedisMQ.StartHTTPServer(cnf)
}
