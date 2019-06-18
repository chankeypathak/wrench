// package wrench provides a generic framework for performing latency benchmarks.
package wrench

import (
	"encoding/csv"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/vwdsrc/wrench/config"
	"github.com/vwdsrc/wrench/recordprovider"
	"github.com/vwdsrc/wrench/utils"
)

// Benchmark performs a system benchmark by attempting to issue requests at a
// specified rate and capturing the latency distribution. The request rate is
// divided across the number of configured connections.
type Benchmark struct {
	publishers  []*publisherController
	subscribers []*subscriberController
	config.Options
}

// ConnectorFactory combines PublisherFactory and SubscriberFactory
type ConnectorFactory interface {
	PublisherFactory
	SubscriberFactory
}

// result of a single run.
type result struct {
	err     error
	summary *Summary
}

func initRequestRates(o *config.Options) {
	requestRates := make([]config.PublishRateChange, 0)
	var totalDuration time.Duration
	var totalRequests int64

	// If no requestRateFile is given create one publishRateChange instance from command line options
	if o.RequestRateFile == "" {
		r := config.PublishRateChange{
			Duration:    o.Duration * time.Duration(o.RequestRateMultiplier),
			RequestRate: o.RequestRate * int64(o.RequestRateMultiplier) / int64(o.NumPubs),
		}
		requestRates = append(requestRates, r)
		totalDuration = r.Duration
		totalRequests = r.RequestRate * int64(o.NumPubs) * int64(r.Duration.Seconds())
	} else {
		f, err := os.Open(o.RequestRateFile)
		defer f.Close()
		utils.CheckErr(err)
		c := csv.NewReader(f)
		records, err := c.ReadAll()
		utils.CheckErr(err)
		for _, record := range records {
			duration, err := strconv.ParseUint(record[0], 10, 64)
			utils.CheckErr(err)
			requestRate, err := strconv.ParseInt(record[1], 10, 64)
			utils.CheckErr(err)

			r := config.PublishRateChange{
				Duration:    time.Duration(duration*o.RequestTimeMultiplier) * time.Second,
				RequestRate: requestRate * int64(o.RequestRateMultiplier) / int64(o.NumPubs),
			}
			requestRates = append(requestRates, r)
			totalDuration += r.Duration
			totalRequests += r.RequestRate * int64(o.NumPubs) * int64(r.Duration.Seconds())
		}
	}
	o.Duration = totalDuration
	o.AvgRequestRate = float64(totalRequests) / totalDuration.Seconds()
	o.RequestRates = requestRates
}

func initSourceData(o *config.Options) (config.RecordProvider, [][]byte) {
	provider := recordprovider.GetProvider(o.DataMode)
	if provider == nil {
		panic("Unsupported DataMode " + o.DataMode)
	}
	return provider, provider.GetRecords(o)
}

// NewBenchmark creates a Benchmark which runs a system benchmark using the
// given ConnectorFactory. The requestRate argument specifies the number of
// requests per second to issue. This value is divided across the number of
// connections specified, so if requestRate is 50,000 and connections is 10,
// each connection will attempt to issue 5,000 requests per second. A zero
// value permanently pauses requests and -1 disables rate limiting entirely.
// The duration argument specifies how long to run the benchmark.
func NewBenchmark(factory ConnectorFactory, o *config.Options) *Benchmark {

	if o.NumPubs > 0 {
		initRequestRates(o)
		o.RecordProvider, o.Records = initSourceData(o)
	}

	err := DetermineClockSkew(o)
	utils.CheckErr(err)

	publishers := make([]*publisherController, o.NumPubs)
	for i := uint64(0); i < o.NumPubs; i++ {
		publishers[i] = newPublisherController(factory.GetPublisher(i), o)
	}

	subscribers := make([]*subscriberController, o.NumSubs)
	for i := uint64(0); i < o.NumSubs; i++ {
		subscribers[i] = newSubscriberConnection(factory.GetSubscriber(i), o)
	}

	return &Benchmark{
		publishers:  publishers,
		subscribers: subscribers,
		Options:     *o,
	}
}

// Run the benchmark and return a summary of the results. An error is returned
// if something went wrong along the way.
func (b *Benchmark) Run() (*Summary, error) {
	var (
		start      = make(chan struct{})
		pubResults = make(chan *result, b.NumPubs)
		subResults = make(chan *result, b.NumSubs)
		wgPub      sync.WaitGroup
		wgSub      sync.WaitGroup
	)

	// Prepare publishers
	for _, publisher := range b.publishers {
		if err := publisher.setup(); err != nil {
			return nil, err
		}
		wgPub.Add(1)
		go func(pc *publisherController) {
			<-start
			pubResults <- pc.run()
			pc.teardown()
			wgPub.Done()
		}(publisher)
	}

	// Prepare subscribers
	for _, subscriber := range b.subscribers {
		if err := subscriber.setup(); err != nil {
			return nil, err
		}
		wgSub.Add(1)
		go func(sc *subscriberController) {
			<-start
			subResults <- sc.result()
			sc.teardown()
			wgSub.Done()
		}(subscriber)
	}

	log.Println("Starting benchmark")
	// Start benchmark
	close(start)

	// Wait for publishers to send all messages
	wgPub.Wait()
	if b.NumPubs > 0 {
		log.Println("Pubs finished")
	}

	// Sum up all sent messages
	var totalSuccess uint64
	var totalFailed uint64
	for _, publisher := range b.publishers {
		totalSuccess += publisher.successTotal
		totalFailed += publisher.errorTotal
	}
	if totalSuccess > 0 {
		log.Println("Published", totalSuccess, "messages")
	}
	if totalFailed > 0 {
		log.Println("Failed to publish", totalFailed, "messages")
	}

	// Wait for subscribers to read all messages
	wgSub.Wait()
	if b.NumSubs > 0 {
		log.Println("Subs finished")
	}

	var summary *Summary

	// Merge results
	if b.NumPubs > 0 {
		result := <-pubResults
		if result.err != nil {
			return nil, result.err
		}
		summary = result.summary
		for i := uint64(1); i < b.NumPubs; i++ {
			result = <-pubResults
			if result.err != nil {
				return nil, result.err
			}
			summary.merge(result.summary)
		}
		summary.Pub.NumPubs = b.NumPubs
		summary.Pub.PublishRates = b.Options.RequestRates
		log.Println("Pub results merged")
	}

	if b.NumSubs > 0 {
		for i := uint64(0); i < b.NumSubs; i++ {
			result := <-subResults
			if result.err != nil {
				return nil, result.err
			}
			if summary == nil {
				summary = result.summary
			} else {
				summary.merge(result.summary)
			}
		}
		summary.Sub.NumSubs = b.NumSubs
		log.Println("Sub results merged")
	}

	return summary, nil
}
