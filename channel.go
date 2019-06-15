package GoRedisMQ

import (
	"fmt"
)

type Channel struct {
	TopicName string `json:"topic_name"`
	Name      string `json:"name"`
	broker    *Broker
}

func NewChannel(topicName string, name string, broker *Broker) *Channel {
	return &Channel{
		TopicName: topicName,
		Name:      name,
		broker:    broker,
	}
}

func (c *Channel) PutMessage(msg *Message) error {
	return c.broker.Publish(fmt.Sprintf("%s:%s", c.TopicName, c.Name), msg)
}
