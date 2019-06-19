package goredismq

import (
	"fmt"

	"github.com/google/uuid"
)

type Channel struct {
	ID        string `bson:"_id"`
	TopicName string `json:"topic_name" bson:"topic_name"`
	Name      string `json:"name" bson:"name"` // 消费端的唯一标识
	broker    *Broker
	backend   *Backend
}

func NewChannelWithNameAndBrokerBackend(topicName string, name string, broker *Broker, backend *Backend) *Channel {
	c := &Channel{
		ID:        fmt.Sprintf("channel_%v", uuid.New().String()),
		TopicName: topicName,
		Name:      name,
		broker:    broker,
		backend:   backend,
	}
	// 启动消息重试队列
	c.broker.ListenRetryMessageId(c.getBakQueueName(), c.handleRetryMessage)
	return c
}

func (c *Channel) handleRetryMessage(msgId string) {
	msg, _ := c.backend.GetMessage(msgId)
	fmt.Printf("[GoRedisMQ] retryMessage: %s retryCount:%d retryTimeout:%d.\n", msg.ID, msg.RetryCount, msg.RetryTimeout)
	if msg != nil {
		if msg.RetryCount > 0 {
			msg.RetryCount--
			c.PutMessage(msg, StateRetry)
		} else {
			c.messageFailed(msg, "Timeout")
		}
	}
}

func (c *Channel) messageFailed(msg *Message, err string) {
	fmt.Printf("[GoRedisMQ] message_id: %s failed.\n", msg.ID)
	c.backend.SetMessageFailure(msg, err)
}

func (c *Channel) AckMessage(msgId string, results interface{}) {
	// redis 备份一份，实现 ack 无响应重试的功能
	msg, _ := c.backend.GetMessage(msgId)
	fmt.Printf("[GoRedisMQ] message_id %s success.\n", msg.ID)
	c.broker.Remove(c.getBakQueueName(), msg)
	c.backend.SetMessageSuccess(msg, results)
}

func (c *Channel) PutMessage(msg *Message, state string) error {
	if state == StatePending {
		c.backend.SetMessagePending(msg)
	} else {
		c.backend.SetMessageRetry(msg)
	}
	// redis 备份一份，实现 ack 无响应重试的功能
	c.broker.Bak2retry(c.getBakQueueName(), msg)
	// redis 发布一份供消费者消费
	return c.broker.Publish(c.getQueueName(), msg)
}

// 备份重试的 redis 队列名
func (c *Channel) getBakQueueName() string {
	return fmt.Sprintf("%s:%s:%s", c.TopicName, c.Name, "retry")
}

// 发布 redis 消息的队列名
func (c *Channel) getQueueName() string {
	return fmt.Sprintf("%s:%s", c.TopicName, c.Name)
}
