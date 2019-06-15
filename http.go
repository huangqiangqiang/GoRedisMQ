package GoRedisMQ

import (
	"fmt"

	"github.com/kataras/iris"
)

type httpServer struct {
	broker  *Broker
	backend *Backend
}

func StartHTTPServer(cnf *Config) error {
	s := &httpServer{}
	broker := NewBroker(cnf)
	backend, err := NewBackend(cnf)
	if err != nil {
		return err
	}
	s.broker = broker
	s.backend = backend
	app := iris.New()
	app.Post("/pub", s.doPub)
	app.Post("/sub", s.doSub)
	app.Get("/lookup", s.doLookUp)
	app.Run(iris.Addr(":7890"))
	return nil
}

func (s *httpServer) doPub(ctx iris.Context) {
	// 1. 获取请求参数 topicName 和 message body
	topicName, messageMap := s.getTopicAndMessageFromReq(ctx)
	// 2. 从 topicName 获取 topic 对象
	topic := s.broker.GetTopicFromName(topicName)
	// 3. 构建 message 对象，放入 topic
	message := NewMessage(messageMap)
	// 4. 发布 message
	topic.PutMessage(message)
	ctx.JSON(iris.Map{
		"status": "OK",
	})
}

func (s *httpServer) doSub(ctx iris.Context) {
	// 1. 获取请求参数 topicName 和 message body
	topicName, _ := s.getTopicAndMessageFromReq(ctx)
	// 2. 从 topicName 获取 topic 对象
	topic := s.broker.GetTopicFromName(topicName)
	// 3. 获取或创建一个 channel
	c := topic.GetChannel("1111")
	ctx.JSON(iris.Map{
		"status":     "OK",
		"redis_addr": s.broker.cnf.Broker,
		"queue_name": fmt.Sprintf("%s:%s", c.TopicName, c.Name),
	})
}

func (s *httpServer) doLookUp(ctx iris.Context) {
	producers, _ := s.backend.FindProducers()
	ctx.JSON(iris.Map{
		"status":   "OK",
		"producer": producers,
	})
}

func (s *httpServer) getTopicAndMessageFromReq(ctx iris.Context) (string, map[string]interface{}) {
	// 1. 从 url 中获取 topic name
	topicName := ctx.URLParam("topic")
	// 2. 从 body 中获取 message
	var messageMap = make(map[string]interface{})
	ctx.ReadJSON(&messageMap)
	return topicName, messageMap
}
