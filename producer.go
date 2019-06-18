package GoRedisMQ

type Producer struct {
	RemoteAddress string   `json:"remote_address"`
	Topics        []*Topic `json:"topics"`
}
