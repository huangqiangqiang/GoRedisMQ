package GoRedisMQ

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Message struct {
	ID        string
	Body      map[string]interface{}
	Timestamp int64
}

func NewMessage(body map[string]interface{}) *Message {
	msgID := uuid.New().String()
	return &Message{
		ID:        fmt.Sprintf("go_redis_mq_%v", msgID),
		Body:      body,
		Timestamp: time.Now().Unix(),
	}
}
