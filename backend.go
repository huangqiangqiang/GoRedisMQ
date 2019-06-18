package GoRedisMQ

import (
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Backend struct {
	cnf               *Config
	client            *mgo.Session
	mqCollection      *mgo.Collection
	pCollection       *mgo.Collection
	channelCollection *mgo.Collection
}

func NewBackend(cnf *Config) (*Backend, error) {
	backend := &Backend{}
	backend.cnf = cnf
	err := backend.connect()
	if err != nil {
		return nil, err
	}
	return backend, nil
}

func (b *Backend) connect() error {
	client, err := mgo.Dial(b.cnf.Backend)
	if err != nil {
		return err
	}
	// fmt.Printf("[TaskQueue] backend %s connect success.\n", b.cnf.Backend)
	b.client = client
	databaseName := "go_redis_mq"
	b.mqCollection = client.DB(databaseName).C("messages")
	b.pCollection = b.client.DB(databaseName).C("producers")
	b.channelCollection = b.client.DB(databaseName).C("channels")
	return nil
}

func (b *Backend) FindProducers() ([]*Producer, error) {
	var producers []*Producer
	err := b.pCollection.Find(nil).All(&producers)
	if err != nil {
		return nil, err
	}
	fmt.Printf("results: %#v\n", producers)
	return producers, nil
}

func (b *Backend) AddProducer(p *Producer) error {
	err := b.pCollection.Insert(p)
	return err
}

func (b *Backend) AddChannel(c *Channel) error {
	update := bson.M{
		"topic_name": c.TopicName,
		"name":       c.Name,
	}
	update = bson.M{"$set": update}
	_, err := b.channelCollection.Upsert(bson.M{"_id": c.ID}, update)
	return err
}

func (b *Backend) GetAllChannels() ([]*Channel, error) {
	var results []*Channel
	err := b.channelCollection.Find(nil).All(&results)
	return results, err
}

func (b *Backend) GetMessage(msgId string) (*Message, error) {
	var msg *Message
	err := b.mqCollection.Find(bson.M{"_id": msgId}).One(&msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (b *Backend) SetStatePending(msg *Message) error {
	update := bson.M{
		"state":         StatePending,
		"topic":         msg.Topic,
		"channel":       msg.Channel,
		"body":          msg.Body,
		"created_at":    msg.CreatedAt,
		"retry_count":   msg.RetryCount,
		"retry_timeout": msg.RetryTimeout,
	}
	return b.updateState(msg, update)
}

func (b *Backend) SetStateRetry(msg *Message) error {
	update := bson.M{
		"state":       StateRetry,
		"retry_count": msg.RetryCount,
	}
	return b.updateState(msg, update)
}

func (b *Backend) SetStateSuccess(msg *Message, results []interface{}) error {
	update := bson.M{
		"state":   StateSuccess,
		"results": results,
	}
	return b.updateState(msg, update)
}

func (b *Backend) SetStateFailure(msg *Message, err string) error {
	update := bson.M{
		"state":  StateFailure,
		"errmsg": err,
	}
	return b.updateState(msg, update)
}

func (b *Backend) updateState(msg *Message, update bson.M) error {
	update = bson.M{"$set": update}
	_, err := b.mqCollection.Upsert(bson.M{"_id": msg.ID}, update)
	return err
}
