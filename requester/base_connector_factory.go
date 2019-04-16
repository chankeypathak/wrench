package requester

import "sync"

// BaseConnectorFactory contains underlying common methods for all ConnectorFactory implementations
type BaseConnectorFactory struct {
	URLs                        []string
	pubMu                       sync.Mutex
	subMu                       sync.Mutex
	publisherConnectionCounter  uint64
	subscriberConnectionCounter uint64
}

// GetNextPublisherIP returns the next IP for a publisher to connect to
// in a round-robin-manner from the list of available broker servers
func (b *BaseConnectorFactory) GetNextPublisherIP() string {
	b.pubMu.Lock()
	defer b.pubMu.Unlock()
	index := b.publisherConnectionCounter % uint64(len(b.URLs))
	b.publisherConnectionCounter++
	return b.URLs[index]
}

// GetNextSubscriberIP returns the next IP for a subscriber to connect to
// in a reversed round-robin-manner from the list of available broker servers
func (b *BaseConnectorFactory) GetNextSubscriberIP() string {
	b.subMu.Lock()
	defer b.subMu.Unlock()
	index := uint64(len(b.URLs)) - 1 - b.subscriberConnectionCounter%uint64(len(b.URLs))
	b.subscriberConnectionCounter++
	return b.URLs[index]
}
