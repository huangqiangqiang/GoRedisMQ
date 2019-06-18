package GoRedisMQ

import (
	"fmt"

	"github.com/kataras/iris"
)

type httpServer struct {
}

var (
	mq  *MQ
	cnf *Config
)

func StartHTTPServer(config *Config) error {
	cnf = config
	mqInstance, err := NewMQ(cnf)
	if err != nil {
		return err
	}
	mq = mqInstance
	s := &httpServer{}
	app := iris.New()
	app.Post("/pub", s.doPub)
	app.Post("/sub", s.doSub)
	app.Post("/ack", s.doAck)
	app.Get("/lookup", s.doLookUp)
	app.Run(iris.Addr(fmt.Sprintf(":%s", cnf.HttpServerPort)))
	return nil
}

func (s *httpServer) doPub(ctx iris.Context) {

	// 1. 解析请求参数
	params, err := CheckParams(ctx, []string{"topic", "name", "msg"})
	if err != nil {
		ctx.JSON(iris.Map{
			"status":  "ERR",
			"message": err.Error(),
		})
		return
	}
	topicName := params["topic"].(string)
	// producerName := params["name"].(string)
	messageBody := params["msg"].(map[string]interface{})

	// 2. 从 topicName 获取 topic 对象
	topic := mq.GetTopicFromName(topicName)
	// 3. 构建 message 对象，放入 topic
	message := NewMessage(messageBody, cnf)
	message.Topic = topicName
	if params["retry_count"] != nil {
		message.RetryCount = int64(params["retry_count"].(float64))
	}
	if params["retry_timeout"] != nil {
		message.RetryTimeout = int64(params["retry_timeout"].(float64))
	}
	// 4. 发布 message
	topic.PutMessage(message)
	ctx.JSON(iris.Map{
		"status": "OK",
	})
}

func (s *httpServer) doSub(ctx iris.Context) {
	// 1. 获取请求参数
	params, err := CheckParams(ctx, []string{"topic", "name"})
	if err != nil {
		ctx.JSON(iris.Map{
			"status":  "ERR",
			"message": err.Error(),
		})
		return
	}
	topicName := params["topic"].(string)
	consumerName := params["name"].(string)

	// 2. 从 topicName 获取 topic 对象
	topic := mq.GetTopicFromName(topicName)

	// 3. 获取或创建一个 channel
	// 测试发现 client 的 RemoteAddr 每次请求都不一样，所以不能用这个字段作为唯一标识
	// 这里用 consumer name 作为 channelName
	channelName := consumerName
	c := topic.GetChannel(channelName)
	mq.SaveChannel(c)
	ctx.JSON(iris.Map{
		"status":     "OK",
		"redis_addr": mq.broker.cnf.Broker,
		"queue_name": fmt.Sprintf("%s:%s", c.TopicName, c.Name),
	})
}

func (s *httpServer) doAck(ctx iris.Context) {
	// 1. 获取请求参数
	params, err := CheckParams(ctx, []string{"topic", "name", "msg_id"})
	if err != nil {
		ctx.JSON(iris.Map{
			"status":  "ERR",
			"message": err.Error(),
		})
		return
	}
	topicName := params["topic"].(string)
	consumerName := params["name"].(string)
	msgId := params["msg_id"].(string)

	t := mq.GetTopicFromName(topicName)
	channelName := consumerName
	c := t.GetChannel(channelName)

	// ack
	c.AckMessage(msgId)

	ctx.JSON(iris.Map{
		"status": "OK",
		"msg_id": msgId,
	})
}

func (s *httpServer) doLookUp(ctx iris.Context) {
	// producers, _ := s.backend.FindProducers()
	ctx.JSON(iris.Map{
		"status": "OK",
		// "producer": producers,
	})
}
