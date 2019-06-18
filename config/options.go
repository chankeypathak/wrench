package config

import (
	"time"
)

const (
	DefaultBroker                = "kafka"
	DefaultKafkaVersion          = "latest"
	DefaultCompression           = "none"
	DefaultHosts                 = "localhost"
	DefaultTopic                 = "benchmark"
	DefaultPartitionCount        = 1
	DefaultDurationInSecs        = 30
	DefaultRequestRate           = 1000000
	DefaultNumPubs               = 10
	DefaultNumSubs               = 10
	DefaultNumTopics             = 1
	DefaultPayloadSize           = 90
	DefaultRequestRateMultiplier = 1
	DefaultRequestTimeMultiplier = 1
	DefaultDataMode              = "random"
)

// Options assembles all user defined options in one struct
type Options struct {
	// User controlled options
	Broker                string
	KafkaVersion          string
	Compression           string
	Hosts                 string
	DurationInSecs        uint
	Topic                 string
	Replication           bool
	Partitions            int64
	NumPubs               uint64
	NumSubs               uint64
	NumTopics             uint64
	RequestRate           int64
	PayloadSize           uint64
	Burst                 uint64
	RequestRateFile       string
	RequestRateMultiplier uint64
	RequestTimeMultiplier uint64
	SubIP                 string
	SubPort               string
	DataMode              string
	CustomOptions         map[string]string
	// Derived options
	Duration       time.Duration       `toml:"-"`
	ClockSkew      int64               `toml:"-"`
	RequestRates   []PublishRateChange `toml:"-"`
	AvgRequestRate float64             `toml:"-"`
	RecordProvider RecordProvider      `toml:"-"`
	Records        [][]byte            `toml:"-"`
}

// DefaultOptions returns an Options instance with
// all values set to default
func DefaultOptions() *Options {
	return &Options{
		Broker:                DefaultBroker,
		KafkaVersion:          DefaultKafkaVersion,
		Compression:           DefaultCompression,
		Hosts:                 DefaultHosts,
		Topic:                 DefaultTopic,
		Partitions:            DefaultPartitionCount,
		DurationInSecs:        DefaultDurationInSecs,
		RequestRate:           DefaultRequestRate,
		NumPubs:               DefaultNumPubs,
		NumSubs:               DefaultNumSubs,
		NumTopics:             DefaultNumTopics,
		PayloadSize:           DefaultPayloadSize,
		RequestRateMultiplier: DefaultRequestRateMultiplier,
		RequestTimeMultiplier: DefaultRequestTimeMultiplier,
		DataMode:              DefaultDataMode,
		CustomOptions:         make(map[string]string),
	}
}
