package wrench

import (
	"context"
	"encoding/binary"
	"math"
	"strconv"
	"time"

	"github.com/vwdsrc/wrench/config"
	"golang.org/x/time/rate"
)

// defaultBurst is the default burst setting for rate limiter
const defaultBurst = 1000

// publisherConnection performs a system benchmark by issuing requests at a
// specified rate and capturing the latency distribution.
type publisherConnection struct {
	publisher           Publisher
	fullThrottle        bool
	successTotal        uint64
	errorTotal          uint64
	bytesTotal          uint64
	elapsed             time.Duration
	limiter             *rate.Limiter
	rateChangeDone      chan struct{}
	measuredThroughputs []float64
	counter             int
	recordCount         int
	config.Options
}

// newPublisherConnection creates a connectionBenchmark which runs a system
// benchmark using the given Publisher and Options.
func newPublisherConnection(publisher Publisher, opts *config.Options) *publisherConnection {
	return &publisherConnection{
		publisher:    publisher,
		fullThrottle: (len(opts.RequestRates) == 1 && opts.RequestRates[0].RequestRate == -1),
		Options:      *opts,
	}
}

// setup prepares the publisherConnection for running and inits the underlying Publisher.
func (c *publisherConnection) setup() error {
	c.successTotal = 0
	c.errorTotal = 0
	c.measuredThroughputs = make([]float64, len(c.RequestRates))
	if c.Options.Records != nil {
		c.recordCount = len(c.Options.Records)
	}
	return c.publisher.Setup()
}

func (c *publisherConnection) initLimiter(r config.PublishRateChange) {
	requestRate := r.RequestRate
	if requestRate > 0 {
		b := int(c.Burst)
		if b == 0 {

			// burst is at least 1 - otherwise it is the lesser of defaultBurst and 10% of requestRate
			b = int(math.Max(1, math.Min(float64(requestRate)*0.1, float64(defaultBurst))))
		}
		limit := rate.Every(time.Duration(int64(time.Second) / requestRate))
		if c.limiter != nil && c.limiter.Burst() == b {
			c.limiter.SetLimit(limit)
		} else {
			c.limiter = rate.NewLimiter(limit, int(b))
		}
	} else if requestRate == -1 {
		c.limiter = rate.NewLimiter(rate.Inf, defaultBurst)
	} else if requestRate == 0 {
		c.limiter = rate.NewLimiter(rate.Every(r.Duration), 1)
	}
}

// teardown cleans up any benchmark resources.
func (c *publisherConnection) teardown() error {
	return c.publisher.Teardown()
}

// run the benchmark and return the result. Result contains an error if
// something went wrong along the way.
func (c *publisherConnection) run() *result {
	var err error
	if c.fullThrottle {
		c.elapsed, err = c.runFullThrottle()
	} else {
		c.elapsed, err = c.runRateLimited()
	}
	return &result{summary: c.summarize(), err: err}
}

func (c *publisherConnection) generatePayload(payload int64) *[]byte {
	var b []byte
	if c.Options.Records != nil {
		r := c.Options.Records[c.counter]
		c.counter = (c.counter + 1) % c.recordCount
		if c.Options.DataMode == "parsed" {
			tsj := "{\"timestamp\":" + strconv.FormatInt(int64(payload), 10) + "," + string(r)[1:]
			//tsj := "{\"timestamp\":" + strconv.FormatInt(int64(payload), 10) + ",\"a\":1}"
			b = []byte(tsj)
		} else {
			b = make([]byte, len(r))
			copy(b, r)
			binary.LittleEndian.PutUint64(b, uint64(payload))
		}
	} else {
		b = make([]byte, c.PayloadSize)
		binary.LittleEndian.PutUint64(b, uint64(payload))
	}
	return &b
}

func (c *publisherConnection) getStartMsg() *[]byte {
	pubsPerTopic := c.NumPubs / c.NumTopics
	incrementLimitID := c.NumPubs % c.NumTopics
	if c.publisher.GetID() < incrementLimitID {
		pubsPerTopic++
	}
	msg := -10000*int64(pubsPerTopic) - int64(c.publisher.GetID())
	return c.generatePayload(msg)
}

func (c *publisherConnection) getStopMsg() *[]byte {
	return c.generatePayload(-1 * int64(c.publisher.GetID()+1))
}

func (c *publisherConnection) startRequestRateCycling() {
	r := c.RequestRates[0]
	c.rateChangeDone = make(chan struct{})
	c.initLimiter(r)
	go func() {
		<-time.After(r.Duration)
		requestsCount := c.successTotal + c.errorTotal
		c.measuredThroughputs[0] = float64(requestsCount) / r.Duration.Seconds()
		prevCount := requestsCount
		size := len(c.RequestRates)
		for i := 1; i < size; i++ {
			r = c.RequestRates[i]
			c.initLimiter(r)
			<-time.After(r.Duration)

			requestsCount = c.successTotal + c.errorTotal
			c.measuredThroughputs[i] = float64(requestsCount-prevCount) / r.Duration.Seconds()
			prevCount = requestsCount
		}
		close(c.rateChangeDone)
	}()
}

// runRateLimited runs the benchmark by attempting to issue the configured
// number of requests per second.
func (c *publisherConnection) runRateLimited() (time.Duration, error) {
	if err := c.publisher.Publish(c.getStartMsg()); err != nil {
		return 0, err
	}
	c.startRequestRateCycling()
	var (
		ctx   = context.Background()
		start = time.Now()
		stop  = time.After(c.Duration)
	)
	for {
		select {
		case <-stop:
			d := time.Since(start)
			if err := c.publisher.Publish(c.getStopMsg()); err != nil {
				return 0, err
			}
			<-c.rateChangeDone
			return d, nil
		default:
		}

		l := c.limiter
		b := l.Burst()
		l.WaitN(ctx, b)
		if l != c.limiter {
			continue
		}
		for i := 0; i < b; i++ {
			payload := c.generatePayload(time.Now().UnixNano())
			c.bytesTotal += uint64(len(*payload))
			if err := c.publisher.Publish(payload); err != nil {
				c.errorTotal++
			} else {
				c.successTotal++
			}
		}
	}
}

// runFullThrottle runs the benchmark without a limit on requests per second.
func (c *publisherConnection) runFullThrottle() (time.Duration, error) {
	if err := c.publisher.Publish(c.getStartMsg()); err != nil {
		return 0, err
	}
	var (
		start = time.Now()
		stop  = time.After(c.Duration)
	)
	for {
		select {
		case <-stop:
			d := time.Since(start)
			if err := c.publisher.Publish(c.getStopMsg()); err != nil {
				return 0, err
			}
			return d, nil
		default:
		}

		if err := c.publisher.Publish(c.generatePayload(time.Now().UnixNano())); err != nil {
			c.errorTotal++
		} else {
			c.successTotal++
		}
	}
}

// summarize returns a Summary of the last benchmark run.
func (c *publisherConnection) summarize() *Summary {
	return &Summary{
		Pub: &PubSummary{
			SuccessTotal:                c.successTotal,
			ErrorTotal:                  c.errorTotal,
			TimeElapsed:                 c.elapsed,
			TotalAvgThroughput:          float64(c.successTotal+c.errorTotal) / c.elapsed.Seconds(),
			AvgThroughputPerPublishRate: c.measuredThroughputs,
			AvgPublishRate:              uint64(c.AvgRequestRate),
			BytesTotal:                  c.bytesTotal,
			BytesAvgThroughput:          float64(c.bytesTotal) / c.elapsed.Seconds(),
		},
	}
}
