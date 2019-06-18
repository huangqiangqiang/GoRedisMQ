package GoRedisMQ

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Message struct {
	ID           string                 `bson:"_id"`
	Topic        string                 `bson:"topic"`
	Channel      string                 `bson:"channel"`
	Body         map[string]interface{} `bson:"body"`
	CreatedAt    int64                  `bson:"created_at"`
	RetryTimeout int64                  `bson:"retry_timeout"` // 超时时间，单位（秒）
	RetryCount   int64                  `bson:"retry_count"`   // 重试次数
}

func NewMessage(body map[string]interface{}, cnf *Config) *Message {
	msgID := uuid.New().String()
	retryCount := cnf.DefaultRetryCount
	retryTimeout := cnf.DefaultRetryTimeout
	return &Message{
		ID:        fmt.Sprintf("message_%v", msgID),
		Body:      body,
		CreatedAt: time.Now().Unix(),
		// RetryTimeout 秒后未收到 ack 就判定为超时，如果是noack模式，设置为0
		RetryTimeout: retryTimeout,
		RetryCount:   retryCount,
	}
}
