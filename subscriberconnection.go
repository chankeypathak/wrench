package wrench

import "github.com/vwdsrc/wrench/config"

// subscriberConnection is a wrapper for a Subscriber instance
type subscriberConnection struct {
	subscriber Subscriber
	config.Options
}

// newSubscriberConnection creates a new instance of a SubscriberConnection
func newSubscriberConnection(subscriber Subscriber, opts *config.Options) *subscriberConnection {
	return &subscriberConnection{
		subscriber: subscriber,
		Options:    *opts,
	}
}

// setup this SubscriberConnection and the underlying Subscriber instance
func (s *subscriberConnection) setup() error {
	return s.subscriber.Setup(&s.Options)
}

// result waits for all messages being fetched and returns an appropriate result object
func (s *subscriberConnection) result() *result {
	err := s.subscriber.CheckForCompletion()
	return &result{summary: s.subscriber.Summarize(), err: err}
}

// teardown the SubscriberConnection and its underlying Subscriber instance
func (s *subscriberConnection) teardown() error {
	return s.subscriber.Teardown()
}
