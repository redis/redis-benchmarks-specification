## Benchmark specifications goal

The Redis benchmarks specification describes the cross-language/tools requirements and expectations to foster performance and observability standards around redis related technologies. 

Members from both industry and academia, including organizations and individuals are encouraged to contribute. 

Currently, the following members actively support this project:

- [Redis Labs](https://redislabs.com/): providing steady-stable infrastructure platform to run the benchmark suite. Supporting the active development of this project with the company.


## Directory layout

* `setups`
  * [`platforms`](./setups/platforms/): contains the standard platforms considered to provide steady stable results, and to represent common deployment targets.
  * [`topologies`](./setups/topologies/): contains the standard deployment topologies definition with the associated minimum specs to enable the topology definition.
* [`test_suites`](./test-suites/): contains the benchmark suites definitions, specifying the target redis topology, the tested commands, the benchmark utility to use (the client), and if required the preloading dataset steps.
* [`validator`](./validator/): contains the benchmark specifications validator utility
* [`build_agent`](./build_agent/): contains the benchmark build agent utility that receives an event indicating a new build variant, generates the required redis binaries to test, and triggers the benchmark run on the listening agents.
* [`benchmark_coordinator`](./benchmark_coordinator/): contains the coordinator utility that listens for benchmark suite run requests and setups the required steps to spin the actual benchmark topologies and to trigger the actual benchmarks.


## Scope 

This repo aims to provide Redis related benchmark standards and methodologies for:

- Management of benchmark data and specifications across different setups

- Running benchmarks and recording results

- Exporting performance results in several formats (CSV, RedisTimeSeries, JSON)

- **[SOON]** Finding on-cpu, off-cpu, io, and threading performance problems by attaching profiling tools/probers ( perf (a.k.a. perf_events), bpf tooling, vtune )

- **[SOON]** Finding performance problems by attaching telemetry probes

Current supported benchmark tools:

- [redis-benchmark](https://github.com/redis/redis)
- [SOON][memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark)
- [SOON][redis-benchmark-go](https://github.com/filipecosta90/redis-benchmark-go)
- [SOON][redis-benchmark-go](https://github.com/filipecosta90/redis-benchmark-go)


# Contributing guidelines

## Adding new test suites

TBD

## Adding new topologies

TBD

## Adding new test platforms

TBD

# License

redis-benchmark-specifications is distributed under the Apache 2 license - see [LICENSE](LICENSE)