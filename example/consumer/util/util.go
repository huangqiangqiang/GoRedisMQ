package util

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gomodule/redigo/redis"
)

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

func Post(url string, params interface{}) (map[string]interface{}, error) {
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
