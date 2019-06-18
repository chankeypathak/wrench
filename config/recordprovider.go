package config

type RecordProvider interface {
	// GetRecords returns a slice of byte-slices containing the records
	// to be sent in a round-robin fashion
	GetRecords(o *Options) [][]byte
	// InjectTimestamp adds a timestamp into the record
	InjectTimestamp(msg []byte, timestamp int64) []byte
	// GetTimestamp extracts latency from a received message
	GetTimestamp(msg []byte) int64
}
