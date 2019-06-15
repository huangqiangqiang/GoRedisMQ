package GoRedisMQ

import (
	"fmt"
	"sync"
)

type Topic struct {
	Name          string
	memoryMsgChan chan *Message
	channelMap    map[string]*Channel
	broker        *Broker

	sync.RWMutex
}

func NewTopic(topicName string, broker *Broker) *Topic {
	t := &Topic{
		Name:          topicName,
		memoryMsgChan: make(chan *Message),
		channelMap:    make(map[string]*Channel),
		broker:        broker,
	}
	// 启动
	t.messagePump()
	return t
}

func (t *Topic) PutMessage(msg *Message) error {
	// 首先 put 到 memoryMsgChan 队列中
	select {
	case t.memoryMsgChan <- msg:
	}
	return nil
}

func (t *Topic) messagePump() {
	go func() {
		var msg *Message
		for {
			select {
			case msg = <-t.memoryMsgChan:
			}
			for _, channel := range t.channelMap {
				channel.PutMessage(msg)
			}
		}
	}()
}

func (t *Topic) GetChannel() *Channel {
	t.Lock()
	channelName := fmt.Sprintf("%d", len(t.channelMap))
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()
	if isNew {
		//
	}
	return channel
}

func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		channel = NewChannel(t.Name, channelName, t.broker)
		t.channelMap[channelName] = channel
		fmt.Printf("[GoRedisMQ] Topic(%s): new channel(%s)\n", t.Name, channel.Name)
		return channel, true
	}
	return channel, false
}
