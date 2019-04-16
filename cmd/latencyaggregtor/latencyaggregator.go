package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"

	"github.com/codahale/hdrhistogram"
	"github.com/vwdsrc/wrench/latency"
)

const sigfigs = 3

type percentile float64

var percentiles = []percentile{
	10,
	25,
	50,
	75,
	90,
	95,
	99,
	99.9,
	99.99,
	99.999,
}

type settings struct {
	aggregation uint64
	verbose     bool
	summary     bool
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func getHeader(verbose bool) []string {
	s := []string{"Second", "Min", "Avg", "Max", "TotalCount"}
	if verbose {
		for _, p := range percentiles {
			s = append(s, strconv.FormatFloat(float64(p), 'f', -1, 64)+"%")
		}
	}
	return s
}

func getValue(h *hdrhistogram.Histogram, sec uint64, s *settings) []string {
	v := []string{
		strconv.FormatUint(sec*s.aggregation, 10),
		strconv.FormatInt(h.Min(), 10),
		strconv.FormatFloat(h.Mean(), 'f', 2, 64),
		strconv.FormatInt(h.Max(), 10),
		strconv.FormatInt(h.TotalCount(), 10),
	}
	if s.verbose {
		for _, p := range percentiles {
			v = append(v, strconv.FormatInt(h.ValueAtQuantile(float64(p)), 10))
		}
	}
	return v
}

func getSortedKeys(stats map[int64]*hdrhistogram.Histogram) []int {
	var keys []int
	for k := range stats {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	return keys
}

func difference(keys, mergeeKeys []int) []int {
	m := make(map[int]bool)
	for _, v := range keys {
		m[v] = true
	}
	result := make([]int, 0)
	for _, v := range mergeeKeys {
		if !m[v] {
			result = append(result, v)
		}
	}
	return result
}

func merge(stats, mergee map[int64]*hdrhistogram.Histogram, summary *hdrhistogram.Histogram) {
	if summary != nil {
		s2 := mergee[-1]
		delete(mergee, -1)
		summary.Merge(s2)
	}
	keys := getSortedKeys(stats)
	mergeeKeys := getSortedKeys(mergee)

	for _, k := range keys {
		sec := int64(k)
		if s := mergee[sec]; s != nil {
			stats[sec].Merge(s)
		}
	}
	d := difference(keys, mergeeKeys)
	for _, k := range d {
		sec := int64(k)
		stats[sec] = mergee[sec]
	}
}

func processFile(s *settings, filename string, wg *sync.WaitGroup, result chan map[int64]*hdrhistogram.Histogram) {
	var r io.Reader
	f, err := os.Open(filename)
	check(err)
	defer f.Close()
	r = bufio.NewReader(f)

	st := make(map[int64]*hdrhistogram.Histogram)
	var summary *hdrhistogram.Histogram
	if s.summary {
		summary = hdrhistogram.New(0, 3600000, sigfigs)
	}

	lr, _ := latency.NewReader(func(ol *latency.OffsetAndLatency) error {
		offset := uint64(ol.Offset)
		latency := int64(ol.Latency)

		// For now exclude offsets that are higher than one day.
		if offset > 86400*1000 {
			log.Printf("Found suspect offset: %d", offset)
			return nil
		}

		i := int64(offset / (1000 * s.aggregation))

		h := st[i]
		if h == nil {
			h = hdrhistogram.New(0, 3600000, sigfigs)
			st[i] = h
		}

		if err := h.RecordValue(latency); err != nil {
			log.Println("Error recording latency", latency, err)
		}
		if summary != nil {
			summary.RecordValue(latency)
		}
		return nil
	})
	lr.ReadByType(r, lr.ConvertFileExtensionToDataType(filename))

	if s.summary {
		st[-1] = summary
	}
	result <- st
	wg.Done()
}

func outputResults(s *settings, stats map[int64]*hdrhistogram.Histogram, summary *hdrhistogram.Histogram) {

	cw := csv.NewWriter(os.Stdout)
	defer cw.Flush()
	cw.Write(getHeader(s.verbose))

	keys := getSortedKeys(stats)

	start := int64(keys[0])

	for _, k := range keys {
		sec := int64(k)
		h := stats[sec]
		cw.Write(getValue(h, uint64(sec-start), s))
	}
	if summary != nil {
		s.aggregation = 1
		totalRuntime := uint64(keys[len(keys)-1] - keys[0])
		cw.Write([]string{"# Summary"})
		cw.Write(getValue(summary, totalRuntime, s))
	}
}

func main() {
	s := &settings{}

	flag.Uint64Var(&s.aggregation, "aggregation-interval", 1, "Aggregation interval in seconds.")
	flag.Uint64Var(&s.aggregation, "i", 1, "Aggregation interval in seconds.")
	flag.BoolVar(&s.verbose, "verbose", false, "Output more detailed stats.")
	flag.BoolVar(&s.verbose, "v", false, "Output more detailed stats.")
	flag.BoolVar(&s.summary, "summary", false, "Output a summary at the end.")
	flag.BoolVar(&s.summary, "s", false, "Output a summary at the end.")

	flag.Parse()

	files := flag.Args()

	if len(files) == 0 {
		fmt.Println("Usage: latencyaggregator <filename> [<filename>*]")
	}

	result := make(chan map[int64]*hdrhistogram.Histogram, len(files))

	var wg sync.WaitGroup
	for _, file := range files {
		wg.Add(1)
		go processFile(s, file, &wg, result)
	}
	wg.Wait()

	stats := <-result
	var summary *hdrhistogram.Histogram
	if s.summary {
		summary = stats[-1]
		delete(stats, -1)
	}
	for i := 1; i < len(files); i++ {
		merge(stats, <-result, summary)
	}

	outputResults(s, stats, summary)
}
