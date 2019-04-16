package wrench

// PublisherFactory creates new Publishers.
type PublisherFactory interface {
	// GetPublisher returns a new Publisher, called for each Benchmark
	// connection.
	GetPublisher(number uint64) Publisher
}

// Publisher issues requests for a particular system under test.
type Publisher interface {
	// Setup prepares the Publisher for benchmarking.
	Setup() error

	// Publish performs a request to the system under test.
	Publish(payload *[]byte) error

	// Teardown is called upon benchmark completion.
	Teardown() error

	// GetID returns the Id of the Publisher
	GetID() uint64
}

// BasePublisher provides methods common to all Publisher implementations
type BasePublisher struct {

	// ID of the Publisher instance
	ID uint64
}

// GetID returns the Id of the Publisher
func (b *BasePublisher) GetID() uint64 {
	return b.ID
}
