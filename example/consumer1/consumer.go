package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Consumer struct {
	redisAddr         string
	queueName         string
	redisOnce         sync.Once
	pool              *redis.Pool
	stopReceivingChan chan int
}

func main() {
	response, err := POST("http://localhost:7890/sub?topic=test", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	client := NewConsumer(response["redis_addr"].(string), response["queue_name"].(string))
	client.StartConsuming()
}

func NewConsumer(redisAddr string, queueName string) *Consumer {
	fmt.Printf("redisAddr: %s, queueName: %s\n", redisAddr, queueName)
	return &Consumer{
		redisAddr: redisAddr,
		queueName: queueName,
	}
}

func POST(url string, params interface{}) (map[string]interface{}, error) {
	paramsByte, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	// client将不再对服务端的证书进行校验
	client := &http.Client{Transport: tr}
	res, err := client.Post(url, "application/json;charset=utf-8", bytes.NewBuffer(paramsByte))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	var response map[string]interface{}
	json.Unmarshal(content, &response)
	return response, nil
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
			// decoder := json.NewDecoder(bytes.NewReader(msg))
			// decoder.UseNumber()
			// if err := decoder.Decode(&msgMap); err != nil {
			// 	return errors.New("Could Not Unmarsha Task Signature")
			// }
			json.Unmarshal(msg, &msgMap)
			fmt.Printf("[Consumer] receive message:%#v\n", msgMap)
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
		c.pool = NewPool(c.redisAddr)
	})
	return c.pool.Get()
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
