package requester

import (
	"fmt"
	"strconv"
	"time"

	"github.com/vwdsrc/wrench"
	"github.com/vwdsrc/wrench/config"
	"github.com/nats-io/go-nats"
)

// NATSConnectorFactory implements ConnectorFactory by creating a Requester
// which publishes messages to NATS and waits to receive them.
type NATSConnectorFactory struct {
	BaseConnectorFactory
	Subject     string
	NumSubjects uint64
}

// GetPublisher returns a new Publisher, called for each Benchmark connection.
func (n *NATSConnectorFactory) GetPublisher(num uint64) wrench.Publisher {
	return &natsPublisher{
		BasePublisher: wrench.BasePublisher{ID: num},
		url:           "nats://" + n.GetNextPublisherIP() + ":4222",
		subject:       n.Subject + "-" + strconv.FormatUint(num%n.NumSubjects, 10),
	}
}

// natsPublisher implements Publisher by publishing a message to NATS and
// waiting to receive it.
type natsPublisher struct {
	wrench.BasePublisher
	url     string
	subject string
	conn    *nats.Conn
}

// Setup prepares the Publisher for benchmarking.
func (n *natsPublisher) Setup() error {
	opts := nats.GetDefaultOptions()
	opts.Url = n.url
	opts.ReconnectWait = 10 * time.Millisecond
	opts.Timeout = 2 * time.Second
	conn, err := opts.Connect()
	if err != nil {
		return err
	}
	n.conn = conn

	//conn, err := nats.Connect(n.url)
	//if err != nil {
	//return err
	//}
	//n.conn = conn
	return nil
}

// Publish performs a synchronous request to the system under test.
func (n *natsPublisher) Publish(payload *[]byte) error {
	if err := n.conn.Publish(n.subject, *payload); err != nil {
		return err
	}
	return nil
}

// Teardown is called upon benchmark completion.
func (n *natsPublisher) Teardown() error {
	if err := n.conn.Flush(); err != nil {
		return err
	}
	n.conn.Close()
	n.conn = nil
	return nil
}

// GetSubscriber returns a new Subsciber
func (n *NATSConnectorFactory) GetSubscriber(num uint64) wrench.Subscriber {
	return &natsSubscriber{
		BaseSubscriber: wrench.BaseSubscriber{ID: num},
		url:            "nats://" + n.GetNextSubscriberIP() + ":4222",
		subject:        n.Subject + "-" + strconv.FormatUint(num%n.NumSubjects, 10),
	}
}

// natsSubscriber implements Subscriber by publishing a message to NATS and
// waiting to receive it.
type natsSubscriber struct {
	wrench.BaseSubscriber
	url          string
	subject      string
	conn         *nats.Conn
	subscription *nats.Subscription
}

// setup prepares the Subsciber for receiving.
func (n *natsSubscriber) Setup(o *config.Options) error {
	opts := nats.GetDefaultOptions()
	opts.Url = n.url
	opts.AsyncErrorCB = func(_ *nats.Conn, s *nats.Subscription, _ error) {
		msgLimit, bytesLimit, _ := s.PendingLimits()
		msgSize, bytesSize, _ := s.Pending()

		fmt.Println(n.ID, "msgLimit", msgLimit, msgSize)
		fmt.Println(n.ID, "bytesLimit", bytesLimit, bytesSize)
	}
	opts.ReconnectWait = 10 * time.Millisecond
	opts.Timeout = 2 * time.Second
	conn, err := opts.Connect()
	if err != nil {
		return err
	}
	n.conn = conn

	n.BaseSetup(o)

	s, err := n.conn.Subscribe(n.subject, func(msg *nats.Msg) {
		n.RecordLatency(&msg.Data)
	})
	if err != nil {
		return err
	}
	if err := s.SetPendingLimits(nats.DefaultSubPendingMsgsLimit*10, nats.DefaultSubPendingBytesLimit); err != nil {
		return err
	}
	n.subscription = s

	if err = n.conn.Flush(); err != nil {
		return err
	}

	return nil
}

// Teardown is called upon benchmark completion.
func (n *natsSubscriber) Teardown() error {
	d, _ := n.subscription.Dropped()
	if d > 0 {
		fmt.Println(n.ID, "Dropped:", d)
	}
	n.conn.Close()
	n.conn = nil
	n.BaseTeardown()
	return nil
}
