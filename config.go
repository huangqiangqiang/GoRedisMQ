package GoRedisMQ

type Config struct {
	HttpServerPort string
	Broker         string
	Backend        string

	// 超时时间
	DefaultRetryTimeout int64
	DefaultRetryCount   int64
}
