package GoRedisMQ

type Producer struct {
	RemoteAddress    string   `json:"remote_address"`
	Hostname         string   `json:"hostname"`
	BroadcastAddress string   `json:"broadcast_address"`
	HTTPPort         int      `json:"http_port"`
	Topics           []*Topic `json:"topics"`
}
