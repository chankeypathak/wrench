package connector

import "sync"

// BaseConnectorFactory contains underlying common methods for all ConnectorFactory implementations
type BaseConnectorFactory struct {
	URLs                       []string
	pubMu                      sync.Mutex
	subMu                      sync.Mutex
	publisherControllerCounter  uint64
	subscriberControllerCounter uint64
}

// GetNextPublisherIP returns the next IP for a publisher to connect to
// in a round-robin-manner from the list of available broker servers
func (b *BaseConnectorFactory) GetNextPublisherIP() string {
	b.pubMu.Lock()
	defer b.pubMu.Unlock()
	index := b.publisherControllerCounter % uint64(len(b.URLs))
	b.publisherControllerCounter++
	return b.URLs[index]
}

// GetNextSubscriberIP returns the next IP for a subscriber to connect to
// in a reversed round-robin-manner from the list of available broker servers
func (b *BaseConnectorFactory) GetNextSubscriberIP() string {
	b.subMu.Lock()
	defer b.subMu.Unlock()
	index := uint64(len(b.URLs)) - 1 - b.subscriberControllerCounter%uint64(len(b.URLs))
	b.subscriberControllerCounter++
	return b.URLs[index]
}
