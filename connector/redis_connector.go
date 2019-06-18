package connector

import (
	"github.com/garyburd/redigo/redis"
	"github.com/vwdsrc/wrench"
	"github.com/vwdsrc/wrench/config"
)

// RedisPubSubConnectorFactory implements ConnectorFactory by creating a
// Requester which publishes messages to Redis and waits to receive them.
type RedisPubSubConnectorFactory struct {
	BaseConnectorFactory
	Channel string
}

// redisPubSubPublisher implements Publisher by publishing a message to Redis
// and waiting to receive it.
type redisPubSubPublisher struct {
	wrench.BasePublisher
	url         string
	channel     string
	publishConn redis.Conn
}

// GetPublisher returns a new Publisher, called for each Benchmark connection.
func (r *RedisPubSubConnectorFactory) GetPublisher(num uint64) wrench.Publisher {
	return &redisPubSubPublisher{
		BasePublisher: wrench.BasePublisher{ID: num},
		url:           r.GetNextPublisherIP() + ":6379",
		channel:       r.Channel,
	}
}

// Setup prepares the Publisher for benchmarking.
func (r *redisPubSubPublisher) Setup() error {
	pubConn, err := redis.Dial("tcp", r.url)
	if err != nil {
		return err
	}
	r.publishConn = pubConn
	return nil
}

// Request performs an asynchronous request to the system under test.
func (r *redisPubSubPublisher) Publish(payload *[]byte) error {
	if err := r.publishConn.Send("PUBLISH", r.channel, *payload); err != nil {
		return err
	}
	if err := r.publishConn.Flush(); err != nil {
		return err
	}
	return nil
}

// Teardown is called upon benchmark completion.
func (r *redisPubSubPublisher) Teardown() error {

	// Drain the pending server replies to our publish commands
	// TODO: Is there a way to skip this?
	if _, err := r.publishConn.Do(""); err != nil {
		return err
	}
	if err := r.publishConn.Close(); err != nil {
		return err
	}
	r.publishConn = nil
	return nil
}

// redisPubSubSubscriber implements Subscriber by publishing a message to Redis
// and waiting to receive it.
type redisPubSubSubscriber struct {
	wrench.BaseSubscriber
	url           string
	channel       string
	subscribeConn *redis.PubSubConn
}

// GetSubscriber returns a new Subscriber, called for each Benchmark connection.
func (r *RedisPubSubConnectorFactory) GetSubscriber(num uint64) wrench.Subscriber {
	return &redisPubSubSubscriber{
		BaseSubscriber: wrench.BaseSubscriber{ID: num},
		url:            r.GetNextSubscriberIP() + ":6379",
		channel:        r.Channel,
	}
}

// Setup prepares the Subscriber for benchmarking.
func (r *redisPubSubSubscriber) Setup(opts *config.Options) error {
	subConn, err := redis.Dial("tcp", r.url)
	if err != nil {
		return err
	}

	subscribeConn := &redis.PubSubConn{Conn: subConn}
	if err := subscribeConn.Subscribe(r.channel); err != nil {
		subscribeConn.Close()
		return err
	}
	// Receive subscription message.
	switch recv := subscribeConn.Receive().(type) {
	case error:
		return recv
	}
	r.subscribeConn = subscribeConn

	r.BaseSetup(opts)

	go r.startReceiving()

	return nil
}

func (r *redisPubSubSubscriber) startReceiving() {
	for r.subscribeConn != nil {
		switch n := r.subscribeConn.Receive().(type) {
		case redis.Message:
			r.RecordLatency(&n.Data)
		case error:
			return
		}
	}
}

// Teardown is called upon benchmark completion.
func (r *redisPubSubSubscriber) Teardown() error {
	if err := r.subscribeConn.Unsubscribe(r.channel); err != nil {
		return err
	}
	if err := r.subscribeConn.Close(); err != nil {
		return err
	}
	r.subscribeConn = nil
	r.BaseTeardown()
	return nil
}
