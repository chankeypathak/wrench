# [![Image](wrench.svg)](https://commons.wikimedia.org/wiki/File:HEB_project_flow_icon_03_wrench_and_nuts.svg) wrench - Workload-optimized and Re-engineered bench #
###### Icon attribution: heb@Wikimedia Commons (mail) [[CC BY-SA 3.0](https://creativecommons.org/licenses/by-sa/3.0)] ######

This is a fork of https://github.com/tylertreat/bench. For changes see [here](CHANGES.md).

Code to run benchmarks against several Message Queues. Currently supported are the following providers
- Kafka (two different client libraries)
- NATS
- NATS Streaming
- Redis
- No-Op - it does just test the performance of the tool itself.

For easier usage this code ships with several executables that can be found in the `cmd` folder. The most important one is `benchmark` to execute just that.

## benchmark ##
### Usage ###
```
$ benchmark --help
Usage of benchmark:
  -broker string
        Type of msg bus to connect. Currently supported values are "kafka", "redis", "nats" and "nats-streaming" (default "kafka")
  -burst uint
        Burst rate for limiter. Only relevant if requestRate > 0
  -compression string
        Type of compression to be used in producer if available. Currently only works with Kafka. (default "none")
  -configFile string
        TOML file with configuration for this run. If this option is used all other options are ignored.
  -dataMode string
        Data mode to fill the packets. Can be "zero" (padding with zero bytes up to "payloadSize"), "random", "real" (default. Requires also --dfeedFile to be defined) and "parsed". (default "real")
  -duration uint
        Duration of benchmark in seconds (default 30)
  -hosts string
        Comma-separated hostnames to test against (default "localhost")
  -kafkaVersion string
        Version of Kafka to connect to. Default value "latest" will use latest known version of underlying connector library. (default "latest")
  -numPubs uint
        Publisher connections to split msg/sec upon (default 10)
  -numSubs uint
        Subscriber connections (default 10)
  -numTopics uint
        Shard data on that many topics (default 1)
  -partitions int
        Number of partitions per topic (default 1)
  -payloadSize uint
        Size of message payload (default 90)
  -replication
        Enable replication
  -requestRate int
        Messages/second to be sent. 0 will send no requests, -1 runs full throttle. (default 1000000)
  -requestRateFile string
        File in CSV format containing duration and requestRate.
  -requestRateMultiplier uint
        Multiply all request rates by a constant factor. (default 1)
  -requestTimeMultiplier uint
        Multiply all request rate durations by a constant factor. (default 1)
  -subIp string
        The ip address where subscribers are running
  -subPort string
        The port on which subscribers are listening
  -topic string
        The topic used on the message bus (default "benchmark")
```

To run a benchmark at a fixed rate for a fixed amount of time use
```
benchmark --broker <broker> --requestRate <message rate> --duration <duration in seconds>
```
to run at a request rate in a CSV file (format: duration in seconds,messages per second) use
```
benchmark --broker <broker> --requestRateFile <csv file>
```

It is possible to run only publishers (`--numSubs 0`) or only subscribers (`--numPubs 0`). If they are run on separate machines it is necessary to provide `--subPort <port>` to the subscriber process and `--subIp <ip of subscriber machine> --subPort <port>` to the producer process so that they can handle clock skew. The subscriber process will then open a port on the given port and waits for the producer process to publish its clock. Failure to do so may result in benchmark crash.

For convenience all configuration parameters can be provided in a [TOML](https://github.com/toml-lang/toml) file via `--configFile` parameter. If no config file is provided it will (over-)write a config file with the current settings for easier repeatability.

## latencyreader ##
The `benchmark` process that runs the subscribers will create a raw latency file (binary format) for each subscriber. To convert these binary files into an easier-to-handle CSV-format `latencyreader` can be used
```
latencyreader <filename(s)>
```
The tool can handle an arbitrary number of input files and converts them to CSV.

## latencyaggregator ##
This tool can either take the output of `latencyreader` or read the files directly themselves and creates aggregates over time.

### Usage ###
```
$ latencyaggregator --help
Usage of latencyaggregator:
  -aggregation-interval uint
        Aggregation interval in seconds. (default 1)
  -i uint
        Aggregation interval in seconds. (default 1)
  -s    Output a summary at the end.
  -summary
        Output a summary at the end.
  -v    Output more detailed stats.
  -verbose
        Output more detailed stats.
```


### Citation ###
Please use the following reference if you want to cite this project:

---

Manuel Coenen, Christoph Wagner, Alexander Echler and Sebastian Frischbier.
2019. Poster: Benchmarking Financial Data Feed Systems. In DEBS
’19: The 13th ACM International Conference on Distributed and Event-based
Systems (DEBS ’19), June 24–28, 2019, Darmstadt, Germany. ACM, New York,
NY, USA, 2 pages. https://doi.org/10.1145/3328905.3332506

---
