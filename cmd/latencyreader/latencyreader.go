package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	"github.com/vwdsrc/wrench/latency"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func printUsage(filename string) {
	fmt.Println("Usage:", filename, "<filename> [, <filename>...]")
	fmt.Println()
	fmt.Println("This tool can read binary raw latency files created by benchmark tool and outputs")
	fmt.Println("the decoded information as CSV on stdout.")
	fmt.Println()
	fmt.Println("Supported compressions: none, snappy, gzip")
	fmt.Println("Supported file formats: .dat, .dat.(snappy|gz), .tar (also with compressed files inside), .tar.gz (.tgz)")
}

func outputAsCsv(ol *latency.OffsetAndLatency) error {

	csv := csv.NewWriter(os.Stdout)
	defer csv.Flush()

	return csv.Write([]string{strconv.FormatUint(uint64(ol.Offset), 10), strconv.FormatUint(uint64(ol.Latency), 10)})
}

func convertFile(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	lr, _ := latency.NewReader(outputAsCsv)
	return lr.ReadByType(r, lr.ConvertFileExtensionToDataType(filename))
}

func main() {

	args := os.Args
	if len(args) < 2 {
		printUsage(args[0])
		os.Exit(1)
	}

	if args[1] == "-h" || args[1] == "--help" {
		printUsage(args[0])
		os.Exit(0)
	}

	for i := 1; i < len(args); i++ {
		filename := args[i]
		check(convertFile(filename))
	}
}
