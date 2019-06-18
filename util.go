package GoRedisMQ

import (
	"errors"

	"github.com/kataras/iris"
)

func CheckParams(ctx iris.Context, keys []string) (map[string]interface{}, error) {
	var err error
	var result = make(map[string]interface{})
	var params = make(map[string]interface{})
	err = ctx.ReadJSON(&params)
	if err != nil {
		err = errors.New("参数解析出错")
		return nil, err
	}

	for _, key := range keys {
		val := params[key]
		if val == nil {
			err = errors.New("参数" + key + "不存在")
			break
		}
	}
	result = params
	return result, err
}
