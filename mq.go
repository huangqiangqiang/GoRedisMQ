package GoRedisMQ

import (
	"fmt"
	"sync"
)

type MQ struct {
	cnf      *Config
	broker   *Broker
	backend  *Backend
	topicMap map[string]*Topic
	sync.RWMutex
}

func NewMQ(cnf *Config) (*MQ, error) {
	mq := &MQ{
		cnf: cnf,
	}
	broker := NewBroker(cnf)
	backend, err := NewBackend(cnf)
	if err != nil {
		return nil, err
	}
	mq.broker = broker
	mq.backend = backend
	mq.loadTopics()
	return mq, nil
}

func (mq *MQ) GetTopicFromName(topicName string) *Topic {
	mq.Lock()
	t, ok := mq.topicMap[topicName]
	mq.Unlock()
	if ok {
		return t
	}
	// 如果没有topic，则创建一个
	t = NewTopicWithNameAndBrokerBackend(topicName, mq.cnf, mq.broker, mq.backend)
	t.cnf = mq.cnf
	t.broker = mq.broker
	t.backend = mq.backend
	mq.Lock()
	mq.topicMap[topicName] = t
	mq.Unlock()
	fmt.Printf("[GoRedisMQ] Topic(%s): created\n", t.Name)
	return t
}

func (mq *MQ) SaveChannel(c *Channel) {
	mq.backend.AddChannel(c)
}

func (mq *MQ) loadTopics() {
	topics := make(map[string]*Topic, 0)
	channels, _ := mq.backend.GetAllChannels()
	if len(channels) > 0 {
		for _, channel := range channels {
			channel.broker = mq.broker
			channel.backend = mq.backend
			topic := topics[channel.TopicName]
			if topic == nil {
				topic = NewTopicWithNameAndBrokerBackend(channel.TopicName, mq.cnf, mq.broker, mq.backend)
				topics[channel.TopicName] = topic
			}
			topic.channelMap[channel.Name] = channel
		}
	}
	mq.topicMap = topics
}
