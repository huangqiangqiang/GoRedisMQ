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
		cnf:      cnf,
		topicMap: make(map[string]*Topic),
	}
	broker := NewBroker(cnf)
	backend, err := NewBackend(cnf)
	if err != nil {
		return nil, err
	}
	mq.broker = broker
	mq.backend = backend
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
	t = NewTopic(topicName)
	t.cnf = mq.cnf
	t.broker = mq.broker
	t.backend = mq.backend
	mq.Lock()
	mq.topicMap[topicName] = t
	mq.Unlock()
	fmt.Printf("[GoRedisMQ] Topic(%s): created\n", t.Name)
	return t
}
