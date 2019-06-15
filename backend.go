package GoRedisMQ

import (
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Backend struct {
	cnf          *Config
	client       *mgo.Session
	mqCollection *mgo.Collection
	pCollection  *mgo.Collection
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
	databaseName := "messagequeue"
	collectionName := "mq"
	b.mqCollection = client.DB(databaseName).C(collectionName)
	collectionName = "producers"
	b.pCollection = b.client.DB(databaseName).C(collectionName)
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

func (b *Backend) GetState(taskUUID string) {

}

func (b *Backend) SetStatePending(msg *Message) error {
	update := bson.M{
		"state":      StatePending,
		"body":       msg.Body,
		"created_at": msg.Timestamp,
	}
	return b.updateState(msg, update)
}

func (b *Backend) SetStateReceived(msg *Message) error {
	update := bson.M{"state": StateReceived}
	return b.updateState(msg, update)
}

func (b *Backend) SetStateStarted(msg *Message) error {
	update := bson.M{"state": StateStarted}
	return b.updateState(msg, update)
}

func (b *Backend) SetStateRetry(msg *Message) error {
	update := bson.M{"state": StateRetry}
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
	update := bson.M{"state": StateFailure}
	return b.updateState(msg, update)
}

func (b *Backend) updateState(msg *Message, update bson.M) error {
	update = bson.M{"$set": update}
	_, err := b.mqCollection.Upsert(bson.M{"_id": msg.ID}, update)
	return err
}
