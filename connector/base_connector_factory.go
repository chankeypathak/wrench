package connector

import "sync"

// BaseConnectorFactory contains underlying common methods for all ConnectorFactory implementations
type BaseConnectorFactory struct {
	URLs                       []string
	pubMu                      sync.Mutex
	subMu                      sync.Mutex
	publisherConrollerCounter  uint64
	subscriberConrollerCounter uint64
}

// GetNextPublisherIP returns the next IP for a publisher to connect to
// in a round-robin-manner from the list of available broker servers
func (b *BaseConnectorFactory) GetNextPublisherIP() string {
	b.pubMu.Lock()
	defer b.pubMu.Unlock()
	index := b.publisherConrollerCounter % uint64(len(b.URLs))
	b.publisherConrollerCounter++
	return b.URLs[index]
}

// GetNextSubscriberIP returns the next IP for a subscriber to connect to
// in a reversed round-robin-manner from the list of available broker servers
func (b *BaseConnectorFactory) GetNextSubscriberIP() string {
	b.subMu.Lock()
	defer b.subMu.Unlock()
	index := uint64(len(b.URLs)) - 1 - b.subscriberConrollerCounter%uint64(len(b.URLs))
	b.subscriberConrollerCounter++
	return b.URLs[index]
}
