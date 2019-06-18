package connector

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/vwdsrc/wrench"
	"github.com/vwdsrc/wrench/config"
)

const (
	TimestampLength = 8
)

var initConfigOnce sync.Once
var saramaConfig *sarama.Config

type KafkaKeyProvider interface {
	GetKey(payload *[]byte) sarama.Encoder
}

type defaultKafkaKeyProvider struct{}

func (d *defaultKafkaKeyProvider) GetKey(payload *[]byte) sarama.Encoder {
	return nil
}

func GetDefaultKafkaKeyProvider() KafkaKeyProvider {
	return &defaultKafkaKeyProvider{}
}

// KafkaConnectorFactory implements ConnectorFactory by creating a Requester
// which publishes messages to Kafka and waits to consume them.
type KafkaConnectorFactory struct {
	BaseConnectorFactory
	Topic       string
	Version     string
	Compression string
	Replication bool
	Partitions  int32
	KeyProvider KafkaKeyProvider
}

// initConfig initializes the commonly used config object of all producer/consumer instances
func (k *KafkaConnectorFactory) initConfig() {
	initConfigOnce.Do(func() {
		if k.KeyProvider == nil {
			k.KeyProvider = GetDefaultKafkaKeyProvider()
		}
		c := sarama.NewConfig()
		if k.Version == "latest" {
			c.Version = sarama.MaxVersion
		} else {
			var err error
			c.Version, err = sarama.ParseKafkaVersion(k.Version)
			if err != nil {
				panic(err)
			}
		}
		switch k.Compression {
		case "gzip":
			c.Producer.Compression = sarama.CompressionGZIP
		case "snappy":
			c.Producer.Compression = sarama.CompressionSnappy
		case "lz4":
			c.Producer.Compression = sarama.CompressionLZ4
		default:
			c.Producer.Compression = sarama.CompressionNone
		}
		saramaConfig = c

		if k.Replication || k.Partitions > 1 {
			b := sarama.NewBroker(k.URLs[0] + ":9092")
			if err := b.Open(saramaConfig); err != nil {
				panic(err)
			}
			defer b.Close()

			rf := int16(1)
			if k.Replication {
				rf = int16(len(k.URLs))
			}

			td := make(map[string]*sarama.TopicDetail)
			td["benchmark"] = &sarama.TopicDetail{
				NumPartitions:     k.Partitions,
				ReplicationFactor: rf,
			}
			ctReq := &sarama.CreateTopicsRequest{
				Version:      2,
				TopicDetails: td,
				Timeout:      time.Minute,
				ValidateOnly: false,
			}
			ctResp, err := b.CreateTopics(ctReq)
			if err != nil {
				panic(err)
			}
			if ctResp.TopicErrors["benchmark"].Err != sarama.ErrNoError {
				panic(ctResp.TopicErrors["benchmark"])
			}
		}
	})
}

// GetPublisher returns a new Publisher, called for each Benchmark connection.
func (k *KafkaConnectorFactory) GetPublisher(num uint64) wrench.Publisher {
	k.initConfig()
	return &kafkaPublisher{
		BasePublisher: wrench.BasePublisher{ID: num},
		urls:          []string{k.GetNextPublisherIP() + ":9092"},
		topic:         k.Topic,
		keyProvider:   k.KeyProvider,
	}
}

// kafkaPublisher implements Publisher by publishing a message to Kafka and
// waiting to consume it.
type kafkaPublisher struct {
	wrench.BasePublisher
	urls        []string
	topic       string
	keyProvider KafkaKeyProvider
	producer    sarama.AsyncProducer
}

// Setup prepares the Publisher for benchmarking.
func (k *kafkaPublisher) Setup() error {
	producer, err := sarama.NewAsyncProducer(k.urls, saramaConfig)
	if err != nil {
		return err
	}

	k.producer = producer

	return nil
}

// Request performs a synchronous request to the system under test.
func (k *kafkaPublisher) Publish(payload *[]byte) error {
	msg := &sarama.ProducerMessage{
		Topic: k.topic,
		Key:   k.keyProvider.GetKey(payload),
		Value: sarama.ByteEncoder(*payload),
	}

	select {
	case k.producer.Input() <- msg:
	case err := <-k.producer.Errors():
		return err
	}
	return nil
}

// Teardown is called upon benchmark completion.
func (k *kafkaPublisher) Teardown() error {
	if err := k.producer.Close(); err != nil {
		return err
	}
	k.producer = nil
	return nil
}

// GetSubscriber returns a new Subscriber, called for each Benchmark connection.
func (k *KafkaConnectorFactory) GetSubscriber(num uint64) wrench.Subscriber {
	k.initConfig()
	return &kafkaSubscriber{
		BaseSubscriber: wrench.BaseSubscriber{ID: num},
		urls:           []string{k.GetNextSubscriberIP() + ":9092"},
		topic:          k.Topic,
	}
}

// kafkaSubscriber implements Subscriber by publishing a message to Kafka and
// waiting to consume it.
type kafkaSubscriber struct {
	wrench.BaseSubscriber
	urls               []string
	topic              string
	consumer           sarama.Consumer
	partitionConsumers []sarama.PartitionConsumer
}

// Setup prepares the Subscriber for benchmarking.
func (k *kafkaSubscriber) Setup(opts *config.Options) error {
	consumer, err := sarama.NewConsumer(k.urls, saramaConfig)
	if err != nil {
		return err
	}
	partitions, err := consumer.Partitions(k.topic)
	if err != nil {
		return err
	}
	k.partitionConsumers = make([]sarama.PartitionConsumer, 0)
	for _, part := range partitions {
		partitionConsumer, err := consumer.ConsumePartition(k.topic, part, sarama.OffsetNewest)
		if err != nil {
			consumer.Close()
			return err
		}
		k.partitionConsumers = append(k.partitionConsumers, partitionConsumer)
	}

	k.consumer = consumer

	k.BaseSetup(opts)

	k.startReceiving()

	return nil
}

func (k *kafkaSubscriber) startReceiving() {
	for _, pc := range k.partitionConsumers {
		go func(pc sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				k.RecordLatency(&msg.Value)
			}
		}(pc)
	}
}

// Teardown is called upon benchmark completion.
func (k *kafkaSubscriber) Teardown() error {
	for _, pc := range k.partitionConsumers {
		if err := pc.Close(); err != nil {
			return err
		}
	}
	if err := k.consumer.Close(); err != nil {
		return err
	}
	k.partitionConsumers = nil
	k.consumer = nil
	k.BaseTeardown()
	return nil
}
