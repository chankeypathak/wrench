package connector

import (
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/vwdsrc/wrench"
	"github.com/vwdsrc/wrench/config"
)

// NATSStreamingConnectorFactory implements RequesterFactory by creating a
// Requester which publishes messages to NATS Streaming and waits to receive
// them.
type NATSStreamingConnectorFactory struct {
	BaseConnectorFactory
	Subject  string
	ClientID string
	URL      string
}

// GetPublisher returns a new Publisher, called for each Benchmark connection.
func (n *NATSStreamingConnectorFactory) GetPublisher(num uint64) wrench.Publisher {
	return &natsStreamingPublisher{
		BasePublisher: wrench.BasePublisher{ID: num},
		url:           "nats://" + n.GetNextPublisherIP() + ":4222",
		clientID:      "pub" + n.ClientID + strconv.FormatUint(num, 10),
		subject:       n.Subject, // + "-" + strconv.FormatUint(num, 10),
	}
}

// natsStreamingRequester implements Requester by publishing a message to NATS
// Streaming and waiting to receive it.
type natsStreamingPublisher struct {
	wrench.BasePublisher
	url      string
	clientID string
	subject  string
	conn     stan.Conn
}

// Setup prepares the Publisher for benchmarking.
func (n *natsStreamingPublisher) Setup() error {
	conn, err := stan.Connect("test-cluster", n.clientID, stan.NatsURL(n.url))
	if err != nil {
		return err
	}
	n.conn = conn
	return nil
}

// Request performs a synchronous request to the system under test.
func (n *natsStreamingPublisher) Publish(payload *[]byte) error {
	if _, err := n.conn.PublishAsync(n.subject, *payload, nil); err != nil {
		return err
	}
	return nil
}

// Teardown is called upon benchmark completion.
func (n *natsStreamingPublisher) Teardown() error {
	n.conn.Close()
	n.conn = nil
	return nil
}

// ============================================================================================

// GetSubscriber returns a new Subscriber, called for each Benchmark connection.
func (n *NATSStreamingConnectorFactory) GetSubscriber(num uint64) wrench.Subscriber {
	return &natsStreamingSubscriber{
		BaseSubscriber: wrench.BaseSubscriber{ID: num},
		url:            "nats://" + n.GetNextSubscriberIP() + ":4222",
		clientID:       "sub" + n.ClientID + strconv.FormatUint(num, 10),
		subject:        n.Subject, // + "-" + strconv.FormatUint(num, 10),
	}
}

// natsStreamingSubscriber implements Subscriber by publishing a message to NATS
// Streaming and waiting to receive it.
type natsStreamingSubscriber struct {
	wrench.BaseSubscriber
	url      string
	clientID string
	subject  string
	conn     stan.Conn
	sub      stan.Subscription
}

// Setup prepares the Subscriber for benchmarking.
func (n *natsStreamingSubscriber) Setup(o *config.Options) error {
	conn, err := stan.Connect("test-cluster", n.clientID, stan.NatsURL(n.url))
	if err != nil {
		return err
	}
	n.conn = conn

	n.BaseSetup(o)

	sub, err := n.conn.Subscribe(n.subject, func(msg *stan.Msg) {
		n.RecordLatency(&msg.Data)
	})
	if err != nil {
		conn.Close()
		return err
	}
	if err := sub.SetPendingLimits(nats.DefaultSubPendingMsgsLimit*10, nats.DefaultSubPendingBytesLimit); err != nil {
		return err
	}
	if err := n.conn.NatsConn().Flush(); err != nil {
		return err
	}
	n.sub = sub

	return nil
}

// Teardown is called upon benchmark completion.
func (n *natsStreamingSubscriber) Teardown() error {
	d, _ := n.sub.Dropped()
	if d > 0 {
		fmt.Println(n.ID, "Dropped:", d)
	}
	if err := n.sub.Unsubscribe(); err != nil {
		return err
	}
	n.sub = nil
	n.conn.Close()
	n.conn = nil
	return nil
}
