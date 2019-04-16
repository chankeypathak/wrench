package config

import (
	"time"
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
	Duration       time.Duration `toml:"-"`
	ClockSkew      int64
	RequestRates   []PublishRateChange
	AvgRequestRate float64
	Records        [][]byte `toml:"-"`
	//ParsedRecords  [][]byte `toml:"-"`
}

// DefaultOptions returns an Options instance with
// all values set to default
func DefaultOptions() *Options {
	return &Options{
		Broker:                "kakfa",
		KafkaVersion:          "latest",
		Compression:           "none",
		Hosts:                 "localhost",
		Topic:                 "benchmark",
		Partitions:            1,
		DurationInSecs:        30,
		RequestRate:           1000000,
		NumPubs:               10,
		NumSubs:               10,
		NumTopics:             1,
		PayloadSize:           90,
		RequestRateMultiplier: 1,
		RequestTimeMultiplier: 1,
		DataMode:              "random",
		CustomOptions:         make(map[string]string),
	}
}
