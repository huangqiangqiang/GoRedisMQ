# GoRedisMQ
go语言实现基于redis的消息中间件（目前只是demo）

# 使用
### 启动消息中间件
```
import (
	"github.com/huangqiangqiang/GoRedisMQ"
)

func main() {
	cnf := &GoRedisMQ.Config{
		HttpServerPort:      "7890",
		Broker:              "redis://localhost:6379/0",
		Backend:             "mongodb://localhost:27017",
		DefaultRetryTimeout: 0,
		DefaultRetryCount:   0,
	}
	GoRedisMQ.StartHTTPServer(cnf)
}
```

### 启动消费者
参考`example`文件夹内的例子
```
import (
	"fmt"

	"github.com/huangqiangqiang/GoRedisMQ/example/consumer/consumer"
	"github.com/huangqiangqiang/GoRedisMQ/example/consumer/util"
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
```