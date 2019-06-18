package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/vwdsrc/wrench"
	"github.com/vwdsrc/wrench/config"
	"github.com/vwdsrc/wrench/connector"
)

func checkErr(e error) {
	if e != nil {
		panic(e)
	}
}

func getConnectorFactory(o *config.Options) wrench.ConnectorFactory {
	urls := strings.Split(o.Hosts, ",")
	switch o.Broker {
	case "kafka":
		cf := &connector.KafkaConnectorFactory{
			BaseConnectorFactory: connector.BaseConnectorFactory{URLs: urls},
			Topic:                o.Topic,
			Replication:          o.Replication,
			Version:              o.KafkaVersion,
			Compression:          o.Compression,
			Partitions:           int32(o.Partitions),
		}
		return cf

	case "kafka2":
		return &connector.Kafka2ConnectorFactory{
			URLs:  urls,
			Topic: o.Topic,
		}

	case "nats":
		return &connector.NATSConnectorFactory{
			BaseConnectorFactory: connector.BaseConnectorFactory{URLs: urls},
			Subject:              o.Topic,
			NumSubjects:          o.NumTopics,
		}

	case "nats-streaming":
		return &connector.NATSStreamingConnectorFactory{
			BaseConnectorFactory: connector.BaseConnectorFactory{URLs: urls},
			Subject:              o.Topic,
			ClientID:             "me",
		}

	case "redis":
		return &connector.RedisPubSubConnectorFactory{
			BaseConnectorFactory: connector.BaseConnectorFactory{URLs: urls},
			Channel:              o.Topic,
		}

	case "noop":
		return &connector.NOOPConnectorFactory{}

	default:
		panic("unsupported broker: " + o.Broker)
	}
}

func loadConfig(configFile string, o *config.Options) error {
	_, err := toml.DecodeFile(configFile, o)
	return err
}

func main() {
	var configFile string
	o := config.DefaultOptions()

	flag.StringVar(&configFile, "configFile", "", "TOML file with configuration for this run. If this option is used all other options are ignored.")

	flag.StringVar(&o.Broker, "broker", config.DefaultBroker, "Type of msg bus to connect. Currently supported values are \"kafka\", \"redis\", \"nats\" and \"nats-streaming\"")
	flag.StringVar(&o.KafkaVersion, "kafkaVersion", config.DefaultKafkaVersion, "Version of Kafka to connect to. Default value \"latest\" will use latest known version of underlying connector library.")
	flag.StringVar(&o.Compression, "compression", config.DefaultCompression, "Type of compression to be used in producer if available. Currently only works with Kafka.")
	flag.StringVar(&o.Hosts, "hosts", config.DefaultHosts, "Comma-separated hostnames to test against")
	flag.StringVar(&o.Topic, "topic", config.DefaultTopic, "The topic used on the message bus")
	flag.BoolVar(&o.Replication, "replication", false, "Enable replication")
	flag.Int64Var(&o.Partitions, "partitions", config.DefaultPartitionCount, "Number of partitions per topic")
	flag.UintVar(&o.DurationInSecs, "duration", config.DefaultDurationInSecs, "Duration of benchmark in seconds")

	flag.Int64Var(&o.RequestRate, "requestRate", config.DefaultRequestRate, "Messages/second to be sent. 0 will send no requests, -1 runs full throttle.")
	flag.Uint64Var(&o.NumPubs, "numPubs", config.DefaultNumPubs, "Publisher connections to split msg/sec upon")
	flag.Uint64Var(&o.NumSubs, "numSubs", config.DefaultNumSubs, "Subscriber connections")
	flag.Uint64Var(&o.NumTopics, "numTopics", config.DefaultNumTopics, "Shard data on that many topics")
	flag.Uint64Var(&o.PayloadSize, "payloadSize", config.DefaultPayloadSize, "Size of message payload")
	flag.Uint64Var(&o.Burst, "burst", 0, "Burst rate for limiter. Only relevant if requestRate > 0")
	flag.StringVar(&o.SubIP, "subIp", "", "The ip address where subscribers are running")
	flag.StringVar(&o.SubPort, "subPort", "", "The port on which subscribers are listening")
	flag.StringVar(&o.RequestRateFile, "requestRateFile", "", "File in CSV format containing duration and requestRate.")
	flag.Uint64Var(&o.RequestRateMultiplier, "requestRateMultiplier", config.DefaultRequestRateMultiplier, "Multiply all request rates by a constant factor.")
	flag.Uint64Var(&o.RequestTimeMultiplier, "requestTimeMultiplier", config.DefaultRequestTimeMultiplier, "Multiply all request rate durations by a constant factor.")
	flag.StringVar(&o.DataMode, "dataMode", config.DefaultDataMode, "Data mode to fill the packets. Can be \"zero\" (padding with zero bytes up to \"payloadSize\"), \"random\" or whatever user-implemented RecordProvider exists.")

	flag.Parse()

	if configFile != "" {
		checkErr(loadConfig(configFile, o))
	} else {
		f, err := os.Create("config.toml")
		checkErr(err)
		t := toml.NewEncoder(f)
		checkErr(t.Encode(o))
		checkErr(f.Close())
	}

	//s.topic = s.broker + "_" + strconv.FormatUint(s.requestRate, 10) + "_" + strconv.FormatUint(s.numPubs, 10)

	r := getConnectorFactory(o)
	o.Duration = time.Duration(o.DurationInSecs) * time.Second

	b := wrench.NewBenchmark(r, o)

	smry, err := b.Run()
	checkErr(err)

	fmt.Println(smry)
	if o.NumPubs > 0 {
		smry.WriteRequestRatesAndThroughput("publishrates-" + o.Topic + ".csv")
	}
	if o.NumSubs > 0 {
		outputFilename := o.Broker + "_" + "_" + strconv.FormatUint(o.NumSubs, 10) + ".txt"
		checkErr(smry.GenerateLatencyDistribution(wrench.Logarithmic, outputFilename))
	}
}
