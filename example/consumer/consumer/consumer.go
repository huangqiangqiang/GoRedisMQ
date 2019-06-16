package consumer

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/gomodule/redigo/redis"
	"github.com/huangqiangqiang/GoRedisMQ/example/consumer/util"
)

type Consumer struct {
	redisAddr         string
	queueName         string
	redisOnce         sync.Once
	pool              *redis.Pool
	stopReceivingChan chan int
}

func NewConsumer(redisAddr string, queueName string) *Consumer {
	fmt.Printf("redisAddr: %s, queueName: %s\n", redisAddr, queueName)
	return &Consumer{
		redisAddr: redisAddr,
		queueName: queueName,
	}
}

func (c *Consumer) StartConsuming() error {
	c.stopReceivingChan = make(chan int)
	pool := make(chan struct{}, 1)
	deliveries := make(chan []byte)
	pool <- struct{}{}
	go func() {
		for {
			select {
			case <-c.stopReceivingChan:
				return
			case <-pool:
				// 从redis中取出task的byte数据
				msg, _ := c.nextMessage(c.queueName)
				if len(msg) > 0 {
					// 把任务放入 deliveries 管道
					deliveries <- msg
				}
				pool <- struct{}{}
			}
		}
	}()

	c.consume(deliveries)
	return nil
}

func (c *Consumer) consume(deliveries <-chan []byte) error {
	errorsChan := make(chan error)
	for {
		select {
		case err := <-errorsChan:
			return err
		case msg := <-deliveries:
			// 处理单个任务
			var msgMap map[string]interface{}
			json.Unmarshal(msg, &msgMap)
			fmt.Printf("[Consumer] receive message:%#v\n", msgMap)
			c.ConsumeOne(msgMap)
		}
	}
}

// 从指定的队列里获取下一个任务
func (c *Consumer) nextMessage(queue string) (result []byte, err error) {
	conn := c.open()
	defer conn.Close()

	// BLPOP: 如果列表为空，返回一个 nil 。 否则，返回一个含有两个元素的列表，第一个元素是被弹出元素所属的 key ，第二个元素是被弹出元素的值。
	// 该操作会被阻塞，如果指定的列表 key list1 存在数据则会返回第一个元素，否则在等待 1000 秒后会返回 nil 。
	items, err := redis.ByteSlices(conn.Do("BLPOP", queue, 1000))
	if err != nil {
		return []byte{}, err
	}

	// items[0] - key
	// items[1] - value
	if len(items) != 2 {
		return []byte{}, redis.ErrNil
	}

	result = items[1]

	return result, nil
}

// 链接redis
func (c *Consumer) open() redis.Conn {
	c.redisOnce.Do(func() {
		c.pool = util.NewPool(c.redisAddr)
	})
	return c.pool.Get()
}
