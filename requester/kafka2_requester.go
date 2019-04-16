package requester

import (
	"log"

	"github.com/vwdsrc/wrench"
	"github.com/vwdsrc/wrench/config"
	"github.com/Shopify/sarama"
	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
)

// Kafka2ConnectorFactory implements ConnectorFactory by creating a Requester
// which publishes messages to Kafka and waits to consume them.
type Kafka2ConnectorFactory struct {
	URLs  []string
	Topic string
}

// GetPublisher returns a new Publisher, called for each Benchmark connection.
func (k *Kafka2ConnectorFactory) GetPublisher(num uint64) wrench.Publisher {
	r := &kafka2Publisher{
		urls:  k.URLs,
		topic: k.Topic,
	}
	r.ID = num
	return r
}

// kafka2Publisher implements Publisher by publishing a message to Kafka and
// waiting to consume it.
type kafka2Publisher struct {
	wrench.BasePublisher
	urls     []string
	topic    string
	producer kafka.Producer
}

// Setup prepares the Publisher for benchmarking.
func (k *kafka2Publisher) Setup() error {

	conf := kafka.NewBrokerConf("test-client")
	conf.AllowTopicCreation = true

	broker, err := kafka.Dial(k.urls, conf)
	if err != nil {
		return err
	}

	producer := broker.Producer(kafka.NewProducerConf())

	k.producer = producer

	return nil
}

// Request performs a synchronous request to the system under test.
func (k *kafka2Publisher) Publish(payload *[]byte) error {
	msg := &proto.Message{Value: *payload}

	if _, err := k.producer.Produce(k.topic, 0, msg); err != nil {
		log.Fatalf("cannot produce message to %s:%d: %s", k.topic, 0, err)
	}
	return nil
}

// Teardown is called upon benchmark completion.
func (k *kafka2Publisher) Teardown() error {
	k.producer = nil
	return nil
}

// GetSubscriber returns a new Subscriber, called for each Benchmark connection.
func (k *Kafka2ConnectorFactory) GetSubscriber(num uint64) wrench.Subscriber {
	r := &kafka2Subscriber{
		urls:  k.URLs,
		topic: k.Topic,
	}
	r.ID = num
	return r
}

// kafka2Subscriber implements Subscriber by publishing a message to Kafka and
// waiting to consume it.
type kafka2Subscriber struct {
	wrench.BaseSubscriber
	urls              []string
	topic             string
	consumer          sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
}

// Setup prepares the Subscriber for benchmarking.
func (k *kafka2Subscriber) Setup(opts *config.Options) error {
	consumer, err := sarama.NewConsumer(k.urls, nil)
	if err != nil {
		return err
	}
	partitionConsumer, err := consumer.ConsumePartition(k.topic, 0, sarama.OffsetNewest)
	if err != nil {
		consumer.Close()
		return err
	}

	k.consumer = consumer
	k.partitionConsumer = partitionConsumer

	k.BaseSetup(opts)

	go func() {
		for {
			msg, more := <-k.partitionConsumer.Messages()
			if more {
				k.RecordLatency(&msg.Value)
			} else {
				return
			}
		}
	}()

	return nil
}

// Teardown is called upon benchmark completion.
func (k *kafka2Subscriber) Teardown() error {
	if err := k.partitionConsumer.Close(); err != nil {
		return err
	}
	if err := k.consumer.Close(); err != nil {
		return err
	}
	k.partitionConsumer = nil
	k.consumer = nil
	k.BaseTeardown()
	return nil
}
