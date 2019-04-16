package wrench

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/vwdsrc/wrench/config"
	"github.com/vwdsrc/wrench/utils"
	"github.com/codahale/hdrhistogram"
)

// PubSummary holds all statistic summary data for publishers
type PubSummary struct {
	NumPubs                     uint64
	AvgPublishRate              uint64
	SuccessTotal                uint64
	ErrorTotal                  uint64
	TimeElapsed                 time.Duration
	TotalAvgThroughput          float64
	PublishRates                []config.PublishRateChange
	AvgThroughputPerPublishRate []float64
	BytesTotal                  uint64
	BytesAvgThroughput          float64
}

// String returns a stringified version of the Summary.
func (s *PubSummary) String() string {
	return fmt.Sprintf(
		"{Connections: %d, AvgPublishRate: %d, RequestTotal: %d, SuccessTotal: %d, ErrorTotal: %d, TimeElapsed: %s, Throughput: %.2f/s, BytesTotal: %d, Bytes Throughput: %.2fb/s}",
		s.NumPubs, s.AvgPublishRate, (s.SuccessTotal + s.ErrorTotal), s.SuccessTotal, s.ErrorTotal, s.TimeElapsed, s.TotalAvgThroughput, s.BytesTotal, s.BytesAvgThroughput)
}

// merge the other PubSummary into this one.
func (s *PubSummary) merge(o *PubSummary) {
	if o.TimeElapsed > s.TimeElapsed {
		s.TimeElapsed = o.TimeElapsed
	}
	s.SuccessTotal += o.SuccessTotal
	s.BytesTotal += o.BytesTotal
	s.BytesAvgThroughput += o.BytesAvgThroughput
	s.ErrorTotal += o.ErrorTotal
	s.TotalAvgThroughput += o.TotalAvgThroughput
	if s.AvgThroughputPerPublishRate == nil {
		s.AvgThroughputPerPublishRate = o.AvgThroughputPerPublishRate
	} else if o.AvgThroughputPerPublishRate != nil {
		for i := 0; i < len(s.AvgThroughputPerPublishRate); i++ {
			s.AvgThroughputPerPublishRate[i] += o.AvgThroughputPerPublishRate[i]
		}
	}
}

// SubSummary holds all statistic summary data for subscribers
type SubSummary struct {
	NumSubs            uint64
	ReceivedTotal      uint64
	TimeElapsed        time.Duration
	AvgReceiveRate     float64
	SuccessHistogram   *hdrhistogram.Histogram
	BytesTotal         uint64
	BytesAvgThroughput float64
}

// String returns a stringified version of the Summary.
func (s *SubSummary) String() string {
	return fmt.Sprintf(
		"{Connections: %d, AvgReceiveRate: %.2f/s, ReceivedTotal: %d, TimeElapsed: %s, BytesTotal: %d, Bytes Throughput: %.2fb/s}",
		s.NumSubs, s.AvgReceiveRate, s.ReceivedTotal, s.TimeElapsed, s.BytesTotal, s.BytesAvgThroughput)
}

// merge the other SubSummary into this one.
func (s *SubSummary) merge(o *SubSummary) {
	s.ReceivedTotal += o.ReceivedTotal
	s.BytesTotal += o.BytesTotal
	s.BytesAvgThroughput += o.BytesAvgThroughput
	if o.TimeElapsed > s.TimeElapsed {
		s.TimeElapsed = o.TimeElapsed
	}
	s.AvgReceiveRate += o.AvgReceiveRate
	if s.SuccessHistogram == nil {
		s.SuccessHistogram = o.SuccessHistogram
	} else if o.SuccessHistogram != nil {
		s.SuccessHistogram.Merge(o.SuccessHistogram)
	}
}

// Summary contains the results of a Benchmark run.
type Summary struct {
	Pub *PubSummary
	Sub *SubSummary
}

// String returns a stringified version of the Summary.
func (s *Summary) String() string {
	return fmt.Sprintf("{Pub: %s, Sub: %s}", s.Pub, s.Sub)
}

// GenerateLatencyDistribution generates a text file containing the specified
// latency distribution in a format plottable by
// http://hdrhistogram.github.io/HdrHistogram/plotFiles.html. Percentiles is a
// list of percentiles to include, e.g. 10.0, 50.0, 99.0, 99.99, etc. If
// percentiles is nil, it defaults to a logarithmic percentile scale. If a
// request rate was specified for the benchmark, this will also generate an
// uncorrected distribution file which does not account for coordinated
// omission.
func (s *Summary) GenerateLatencyDistribution(percentiles Percentiles, file string) error {
	return generateLatencyDistribution(s.Sub.SuccessHistogram, percentiles, file)
}

// GenerateErrorLatencyDistribution generates a text file containing the specified
// latency distribution (of requests that resulted in errors) in a format plottable by
// http://hdrhistogram.github.io/HdrHistogram/plotFiles.html. Percentiles is a
// list of percentiles to include, e.g. 10.0, 50.0, 99.0, 99.99, etc. If
// percentiles is nil, it defaults to a logarithmic percentile scale. If a
// request rate was specified for the benchmark, this will also generate an
// uncorrected distribution file which does not account for coordinated
// omission.
//func (s *Summary) GenerateErrorLatencyDistribution(percentiles Percentiles, file string) error {
//    return generateLatencyDistribution(s.ErrorHistogram, s.UncorrectedErrorHistogram, s.RequestRate, percentiles, file)
//}

// WriteRequestRatesAndThroughput writes a CSV file containing the request rate schedule and the actual measured throughputs
func (s *Summary) WriteRequestRatesAndThroughput(file string) {
	f, err := os.Create(file)
	defer f.Close()
	utils.CheckErr(err)
	c := csv.NewWriter(f)
	c.Write([]string{"Duration", "PublishRate/Connection", "Connections", "Expected Throughput", "AvgThroughput"})

	numPubs := strconv.FormatUint(s.Pub.NumPubs, 10)
	for i := 0; i < len(s.Pub.PublishRates); i++ {
		r := s.Pub.PublishRates[i]
		c.Write([]string{
			strconv.FormatUint(uint64(r.Duration.Seconds()), 10),
			strconv.FormatInt(r.RequestRate, 10),
			numPubs,
			strconv.FormatUint(s.Pub.NumPubs*uint64(r.RequestRate), 10),
			strconv.FormatFloat(s.Pub.AvgThroughputPerPublishRate[i], 'f', 2, 64),
		})
	}
	c.Flush()
}

func getOneByPercentile(percentile float64) float64 {
	if percentile < 100 {
		return 1 / (1 - (percentile / 100))
	}
	return float64(10000000)
}

func generateLatencyDistribution(histogram *hdrhistogram.Histogram, percentiles Percentiles, file string) error {
	if percentiles == nil {
		percentiles = Logarithmic
	}

	// Generate uncorrected distribution.
	f, err := os.Create("uncorrected_" + file)
	if err != nil {
		return err
	}
	defer f.Close()

	f.WriteString("Value    Percentile    TotalCount    1/(1-Percentile)\n\n")
	totalCount := histogram.TotalCount()
	for _, percentile := range percentiles {
		value := float64(histogram.ValueAtQuantile(percentile)) / float64(time.Millisecond)
		oneByPercentile := getOneByPercentile(percentile)
		countAtPercentile := int64(((percentile / 100) * float64(totalCount)) + 0.5)
		_, err := f.WriteString(fmt.Sprintf("%f    %f        %d            %f\n",
			value, percentile/100, countAtPercentile, oneByPercentile))
		if err != nil {
			return err
		}
	}

	return nil
}

// merge the other Summary into this one.
func (s *Summary) merge(o *Summary) {
	if s.Sub == nil {
		s.Sub = o.Sub
	} else if o.Sub != nil {
		s.Sub.merge(o.Sub)
	}

	if s.Pub == nil {
		s.Pub = o.Pub
	} else if o.Pub != nil {
		s.Pub.merge(o.Pub)
	}
}
