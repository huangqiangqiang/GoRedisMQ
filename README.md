# goredismq
目前只是demo

go语言实现基于redis的消息中间件

支持PUB/SUB和消息重试

# 使用
本地先启动`mongodb`和`redis`服务，配置在`example/goredismq.go`
```
cnf := &goredismq.Config{
  HttpServerPort:      "7890",
  Broker:              "redis://localhost:6379/0",
  Backend:             "mongodb://localhost:27017",
  DefaultRetryTimeout: 0,
  DefaultRetryCount:   0,
}
```
### 启动消息中间件
```
go run example/goredismq.go
```

### 启动消费者
```
go run example/consumer/main.go
```