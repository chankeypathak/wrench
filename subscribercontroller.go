package wrench

import "github.com/vwdsrc/wrench/config"

// subscriberController is a wrapper for a Subscriber instance
type subscriberController struct {
	subscriber Subscriber
	config.Options
}

// newSubscriberController creates a new instance of a SubscriberController
func newSubscriberConnection(subscriber Subscriber, opts *config.Options) *subscriberController {
	return &subscriberController{
		subscriber: subscriber,
		Options:    *opts,
	}
}

// setup this SubscriberController and the underlying Subscriber instance
func (s *subscriberController) setup() error {
	return s.subscriber.Setup(&s.Options)
}

// result waits for all messages being fetched and returns an appropriate result object
func (s *subscriberController) result() *result {
	err := s.subscriber.CheckForCompletion()
	return &result{summary: s.subscriber.Summarize(), err: err}
}

// teardown the SubscriberController and its underlying Subscriber instance
func (s *subscriberController) teardown() error {
	return s.subscriber.Teardown()
}
