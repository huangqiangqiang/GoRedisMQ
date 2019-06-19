package main

import (
	"github.com/huangqiangqiang/goredismq"
)

func main() {
	cnf := &goredismq.Config{
		HttpServerPort:      "7890",
		Broker:              "redis://localhost:6379/0",
		Backend:             "mongodb://localhost:27017",
		DefaultRetryTimeout: 0,
		DefaultRetryCount:   0,
	}
	goredismq.StartHTTPServer(cnf)
}
