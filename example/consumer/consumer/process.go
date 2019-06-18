package consumer

import (
	"time"
)

func (c *Consumer) ConsumeOne(msg map[string]interface{}) (map[string]interface{}, bool) {
	// TODO
	time.Sleep(time.Duration(3) * time.Second)
	return map[string]interface{}{
		"status":  "OK",
		"message": "",
	}, true
}
