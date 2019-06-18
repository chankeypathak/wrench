package wrench

import (
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/vwdsrc/wrench/config"
	"github.com/vwdsrc/wrench/latency"
	"github.com/vwdsrc/wrench/utils"
)

const (
	minRecordableLatencyNS = int64(time.Millisecond)
	maxRecordableLatencyNS = int64(time.Hour)
	sigFigs                = 5
	latencyWriteBuffer     = 50 * 1024 * 1024
)

// SubscriberFactory creates new Subscriber instances
type SubscriberFactory interface {
	// GetSubscriber returns a new Subscriber
	GetSubscriber(number uint64) Subscriber
}

// Subscriber reads messages from the system under test.
type Subscriber interface {
	// setup prepares the Subscriber for benchmarking.
	Setup(opts *config.Options) error

	// Start receiving
	CheckForCompletion() error

	// Teardown is called upon benchmark completion.
	Teardown() error

	// Summarize the results
	Summarize() *Summary
}

// Latency holds data about a latency in µs at a given offset in µs
type Latency struct {
	Offset  uint32
	Latency uint32
}

// BaseSubscriber contains all shared methods for Subscriber implementations
type BaseSubscriber struct {
	ID               uint64
	successHistogram *hdrhistogram.Histogram
	latencies        []Latency
	received         uint64
	bytesTotal       uint64
	startNanos       int64
	start            time.Time
	stopIds          []int64
	elapsed          time.Duration
	latencyWriter    latency.LatencyWriter
	once             sync.Once
	opts             *config.Options
	mu               sync.Mutex
}

// BaseSetup will initialize all fields of BaseSubscriber struct
func (s *BaseSubscriber) BaseSetup(o *config.Options) {
	s.opts = o
	s.successHistogram = hdrhistogram.New(minRecordableLatencyNS, maxRecordableLatencyNS, sigFigs)

	s.latencies = make([]Latency, 0)
	s.stopIds = make([]int64, 0)
	var err error
	s.latencyWriter, err = latency.NewFileWriter("rawlatencies-"+strconv.FormatUint(s.ID, 10)+".dat.snappy", true)
	utils.CheckErr(err)
}

func (s *BaseSubscriber) handleSignalMessage(payload int64) {
	if payload < -1000 {
		s.once.Do(func() {
			numPubs := int64((-payload) / 10000)
			s.stopIds = make([]int64, numPubs)
			s.start = time.Now()
			s.startNanos = s.start.UnixNano()
		})
	} else {
		index := ((-payload) - 1) % int64(len(s.stopIds))
		s.stopIds[index] = payload
	}
}

// RecordLatency will calculate the latency from the timestamp inside the provided byte-slice
// and put it to hdrhistogram. It also returns the calculated latency for further usage.
func (s *BaseSubscriber) RecordLatency(msg *[]byte) {
	now := time.Now()
	sent := s.opts.RecordProvider.GetTimestamp(*msg)
	if sent < 0 {
		s.handleSignalMessage(sent)
		return
	}
	nowNano := now.UnixNano()
	latency := nowNano - sent - s.opts.ClockSkew
	s.mu.Lock()
	err := s.successHistogram.RecordValue(latency)
	s.mu.Unlock()
	utils.CheckErr(err)
	atomic.AddUint64(&s.received, 1)
	atomic.AddUint64(&s.bytesTotal, uint64(len(*msg)))
	s.latencyWriter.WriteSimple(uint32((nowNano-s.startNanos)/minRecordableLatencyNS), uint32(latency/minRecordableLatencyNS))
}

// BaseTeardown should be called from Subscriber impl's teardown method to finish tasks in BaseSubscriber
func (s *BaseSubscriber) BaseTeardown() {
	s.latencyWriter.Close()
}

// CheckForCompletion checks if all messages have been fetched
func (s *BaseSubscriber) CheckForCompletion() error {
	halfdur := s.opts.Duration / 2
	maxdur := time.Hour
	tickdur := halfdur
	if maxdur < halfdur {
		tickdur = maxdur
	}
	t := time.NewTicker(tickdur)
	for {
		time.Sleep(time.Second)
		select {
		case <-t.C:
			log.Println(s.ID, "received msgs:", atomic.LoadUint64(&s.received), "(not final)")
		default:
		}
		if len(s.stopIds) > 0 {
			allFound := true
			for _, value := range s.stopIds {
				if value == 0 {
					allFound = false
					s.elapsed = time.Since(s.start)
					break
				}
			}
			if allFound {
				t.Stop()
				log.Println(s.ID, "totally received", atomic.LoadUint64(&s.received), "messages")
				return nil
			}
		}
	}
}

// Summarize can/should be called to create a Summary object
func (s *BaseSubscriber) Summarize() *Summary {
	received := atomic.LoadUint64(&s.received)
	return &Summary{
		Sub: &SubSummary{
			ReceivedTotal:      received,
			TimeElapsed:        s.elapsed,
			AvgReceiveRate:     float64(received) / s.elapsed.Seconds(),
			SuccessHistogram:   s.successHistogram,
			BytesTotal:         s.bytesTotal,
			BytesAvgThroughput: float64(s.bytesTotal) / s.elapsed.Seconds(),
		},
	}
}
