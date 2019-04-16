package config

import "time"

// publishRateChange contains information about when to switch to which request rate
type PublishRateChange struct {
	Duration    time.Duration
	RequestRate int64
}
