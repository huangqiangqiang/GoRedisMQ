package GoRedisMQ

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Broker struct {
	cnf                 *Config
	registeredTaskNames []string
	// 与redis的连接池，有多个链接
	pool              *redis.Pool
	redisConn         redis.Conn
	redisOnce         sync.Once
	stopReceivingChan chan int

	topicMap map[string]*Topic
	sync.RWMutex
}

func NewBroker(cnf *Config) *Broker {
	b := &Broker{}
	b.cnf = cnf
	b.topicMap = make(map[string]*Topic)
	return b
}

func (b *Broker) GetTopicFromName(topicName string) *Topic {
	b.Lock()
	t, ok := b.topicMap[topicName]
	b.Unlock()
	if ok {
		return t
	}
	// 如果没有topic，则创建一个
	t = NewTopic(topicName, b)
	b.Lock()
	b.topicMap[topicName] = t
	b.Unlock()
	fmt.Printf("[GoRedisMQ] Topic(%s): created\n", t.Name)
	return t
}

func (b *Broker) GetAllTopics() []*Topic {
	var topics = make([]*Topic, 0)
	for _, topic := range b.topicMap {
		topics = append(topics, topic)
	}
	return topics
}

func (b *Broker) GetAllChannels() {

}

func (b *Broker) Publish(queueName string, msg *Message) error {
	// 序列化数据
	m, err := json.Marshal(msg.Body)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}
	// 获取redis链接
	conn := b.open()
	defer conn.Close()
	// 发布task到redis
	// RPUSH 将msg插入到 taskqueue_tasks 队列的尾部
	_, err = conn.Do("RPUSH", queueName, m)
	return err
}

// 链接redis
func (b *Broker) open() redis.Conn {
	b.redisOnce.Do(func() {
		b.pool = NewPool(b.cnf.Broker)
	})
	return b.pool.Get()
}

// 返回redis连接池
func NewPool(url string) *redis.Pool {
	return &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL(url)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		// PINGs connections that have been idle more than 10 seconds
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Duration(10*time.Second) {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}
