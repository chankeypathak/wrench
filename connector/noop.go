package connector

import (
	"sync"

	"github.com/vwdsrc/wrench"
	"github.com/vwdsrc/wrench/config"
)

// NOOPConnectorFactory implements ConnectorFactory by providing methods
// to create Publisher and Subscriber instances
type NOOPConnectorFactory struct {
}

// GetPublisher returns a new Publisher
func (n *NOOPConnectorFactory) GetPublisher(num uint64) wrench.Publisher {
	return &noopPublisher{
		BasePublisher: wrench.BasePublisher{ID: num},
	}
}

type noopPublisher struct {
	wrench.BasePublisher
}

func (n *noopPublisher) Setup() error {
	return nil
}

func (n *noopPublisher) Publish(payload *[]byte) error {
	for _, subscriber := range subscribers {
		subscriber.RecordLatency(payload)
	}
	return nil
}

func (n *noopPublisher) Teardown() error {
	return nil
}

var initSubscribersOnce sync.Once
var subscribers map[uint64]*noopSubscriber

// GetSubscriber returns a new Subsciber
func (n *NOOPConnectorFactory) GetSubscriber(num uint64) wrench.Subscriber {
	initSubscribersOnce.Do(func() {
		subscribers = make(map[uint64]*noopSubscriber)
	})
	r := &noopSubscriber{
		BaseSubscriber: wrench.BaseSubscriber{ID: num},
	}
	subscribers[num] = r
	return r
}

type noopSubscriber struct {
	wrench.BaseSubscriber
}

func (n *noopSubscriber) Setup(o *config.Options) error {
	n.BaseSetup(o)
	return nil
}

func (n *noopSubscriber) Teardown() error {
	n.BaseTeardown()
	return nil
}
