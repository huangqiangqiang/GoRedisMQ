package goredismq

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
}

func NewBroker(cnf *Config) *Broker {
	b := &Broker{}
	b.cnf = cnf
	return b
}

func (b *Broker) Publish(queueName string, msg *Message) error {
	// 转成 json 字符串
	m, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}
	// 获取redis链接
	conn := b.open()
	defer conn.Close()
	// 发布task到redis
	// RPUSH 将msg插入到 taskqueue_tasks 队列的尾部
	_, err = conn.Do("RPUSH", queueName, string(m))
	return err
}

// 监听需要重试的 message
func (b *Broker) ListenRetryMessageId(bakQueueName string, handleNeedRetryMsg func(string)) {
	go func() {
		for {
			var msgId string
			select {
			default:
				msgId = b.nextRetryMessageId(bakQueueName)
				if msgId != "" {
					handleNeedRetryMsg(msgId)
				}
			}
		}
	}()
}

func (b *Broker) nextRetryMessageId(key string) string {
	conn := b.open()
	defer conn.Close()
	for {
		// 1秒执行1次
		time.Sleep(time.Duration(1) * time.Second)
		now := time.Now().UnixNano()
		items, err := redis.ByteSlices(conn.Do("ZRANGEBYSCORE", key, 0, now, "LIMIT", 0, 1))
		if err != nil {
			return ""
		}
		if len(items) != 1 {
			err = redis.ErrNil
			return ""
		}
		_ = conn.Send("ZREM", key, items[0])
		return string(items[0])
	}
}

func (b *Broker) Bak2retry(queueName string, msg *Message) error {
	// 获取redis链接
	conn := b.open()
	defer conn.Close()
	score := time.Now().Add(time.Second * time.Duration(msg.RetryTimeout)).UnixNano()
	_, err := conn.Do("ZADD", queueName, score, msg.ID)
	return err
}

func (b *Broker) Remove(queueName string, msg *Message) {
	conn := b.open()
	defer conn.Close()
	conn.Do("ZREM", queueName, msg.ID)
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
