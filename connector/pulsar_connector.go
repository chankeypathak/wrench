package connector

import (
	"context"
	"log"
	"runtime"

	"github.com/vwdsrc/wrench"
	"github.com/vwdsrc/wrench/config"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
)

//////////////////////////////////////////////////////////////////////////////////////////////////////////
// Basic Pulsar Connection

// PulsarConnectorFactory implements ConnectorFactory by creating a Requester
// which publishes messages to Pulsar and waits to consume them.
type PulsarConnectorFactory struct {
	BaseConnectorFactory
	Topic  string
	Client pulsar.Client
}

// initConfig initializes the commonly used config object of all producer/consumer instances
func (k *PulsarConnectorFactory) initConfig() {
	initConfigOnce.Do(func() {

		clientOptions := pulsar.ClientOptions{
			URL:                     k.URLs[0] + ":6650/",
			OperationTimeoutSeconds: 5,
			MessageListenerThreads:  runtime.NumCPU(),
		}

		client, err := pulsar.NewClient(clientOptions)

		if err != nil {
			log.Fatalf("Could not instantiate Pulsar client: %v", err)
		}

		k.Client = client
		//defer client.Close()
	})
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////
// Pulsar Publisher

// GetPublisher returns a new Publisher, called for each Benchmark connection.
func (k *PulsarConnectorFactory) GetPublisher(num uint64) wrench.Publisher {
	k.initConfig()

	return &pulsarPublisher{
		BasePublisher: wrench.BasePublisher{ID: num},
		urls:          []string{k.GetNextPublisherIP() + ":6650/"},
		topic:         k.Topic,
		client:        k.Client,
	}
}

// pulsarPublisher implements Publisher by publishing a message to Pulsar and
// waiting to consume it.
type pulsarPublisher struct {
	wrench.BasePublisher
	urls     []string
	topic    string
	producer pulsar.Producer
	client   pulsar.Client
}

// Setup prepares the Publisher for benchmarking.
func (k *pulsarPublisher) Setup() error {

	producer, err := k.client.CreateProducer(pulsar.ProducerOptions{
		Topic: k.topic,
	})

	if err != nil {
		log.Fatalf("Could not instantiate Pulsar producer: %v", err)
	}

	k.producer = producer
	//defer producer.Close()

	return nil
}

// Request performs a synchronous request to the system under test.
func (k *pulsarPublisher) Publish(payload *[]byte) error {
	msg := pulsar.ProducerMessage{
		Payload: *payload,
	}

	ctx := context.Background()

	if err := k.producer.Send(ctx, msg); err != nil {
		log.Fatalf("Producer could not send message: %v", err)
	}
	return nil
}

// Teardown is called upon benchmark completion.
func (k *pulsarPublisher) Teardown() error {
	if err := k.producer.Close(); err != nil {
		return err
	}
	k.producer = nil
	return nil
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////
// Pulsar Subscriber

// GetSubscriber returns a new Subscriber, called for each Benchmark connection.
func (k *PulsarConnectorFactory) GetSubscriber(num uint64) wrench.Subscriber {
	k.initConfig()

	return &pulsarSubscriber{
		BaseSubscriber: wrench.BaseSubscriber{ID: num},
		urls:           []string{k.GetNextSubscriberIP() + ":6650/"},
		topic:          k.Topic,
		client:         k.Client,
	}
}

// pulsarSubscriber implements Subscriber by publishing a message to Pulsar and
// waiting to consume it.
type pulsarSubscriber struct {
	wrench.BaseSubscriber
	urls       []string
	topic      string
	consumer   pulsar.Consumer
	client     pulsar.Client
	msgChannel chan pulsar.ConsumerMessage
}

// Setup prepares the Subscriber for benchmarking.
func (k *pulsarSubscriber) Setup(opts *config.Options) error {
	msgChannel := make(chan pulsar.ConsumerMessage)
	k.msgChannel = msgChannel

	consumerOpts := pulsar.ConsumerOptions{
		Topic:            k.topic,
		SubscriptionName: "my-subscription-1",
		Type:             pulsar.Exclusive,
		MessageChannel:   msgChannel,
	}

	consumer, err := k.client.Subscribe(consumerOpts)

	if err != nil {
		log.Fatalf("Could not establish subscription: %v", err)
	}

	k.consumer = consumer
	//defer consumer.Close()

	k.BaseSetup(opts)

	k.startReceiving()

	return nil
}

func (k *pulsarSubscriber) startReceiving() {
	go func(c chan pulsar.ConsumerMessage) {
		for msg := range c {
			m := msg.Message
			payload := m.Payload()
			k.RecordLatency(&payload)
			k.consumer.Ack(m)
		}
	}(k.msgChannel)
}

// Teardown is called upon benchmark completion.
func (k *pulsarSubscriber) Teardown() error {
	if err := k.consumer.Close(); err != nil {
		return err
	}
	k.consumer = nil
	k.BaseTeardown()
	return nil
}
