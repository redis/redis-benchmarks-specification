
[![codecov](https://codecov.io/gh/filipecosta90/redis-benchmarks-specification/branch/main/graph/badge.svg?token=GS64MV1H4W)](https://codecov.io/gh/filipecosta90/redis-benchmarks-specification)
[![CI tests](https://github.com/filipecosta90/redis-benchmarks-specification/actions/workflows/tox.yml/badge.svg)](https://github.com/filipecosta90/redis-benchmarks-specification/actions/workflows/tox.yml)
[![PyPI version](https://badge.fury.io/py/redis-benchmarks-specification.svg)](https://badge.fury.io/py/redis-benchmarks-specification)
## Benchmark specifications goal

The Redis benchmarks specification describes the cross-language/tools requirements and expectations to foster performance and observability standards around redis related technologies. 

Members from both industry and academia, including organizations and individuals are encouraged to contribute. 

Currently, the following members actively support this project:

- [Redis Ltd.](https://redis.com/): providing steady-stable infrastructure platform to run the benchmark suite. Supporting the active development of this project within the company.


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

## Architecture diagram

```                                                                                    
                                                                                      
                                                                                      
┌──────────────────────────────────────┐                                              
│1) gh.com/redis/redis update          │                                              
│   - git_repo: github.com/redis/redis │                                              
│   - git_hash: 459c3a                 │                                              
│   - git_branch: unstable             │                                              
└─────────────────┬────────────────────┘                                              
                  │                                                                   
                  │      ┌───────────────────────────────────┐                        
                  │      │HTTP POST                          │                        
                  └──────┤<domain>/api/gh/redis/redis/commit │──┐                     
                         └───────────────────────────────────┘  │                     
                                                                │                     
                                                                ▼                     
                                          ┌──────────────────────────────────────────┐
                                          │2) api                                    │
                                          │  - Converts the HTTP info into an stream │
                                          │entry                                     │
                                          │  - XADD stream:redis:redis:commit <...>  │
                                          │                                          │
                                          └─────────────────────┬────────────────────┘
                                                                │                     
                                                                │                     
                                                                │                     
                                                                │ ┌────┐              
                    .─────────────────────────────────────.     │ │push│              
        ┌─────┐ ┌ ▶(  2.1 ) stream of build events         )◀───┘ └────┘              
        │pull │     `─────────────────────────────────────'                           
        └─────┘ │                                                                     
                                                                                      
                │                       ┌────────────────────────────────────────────┐
                 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│2.2) build_agent                            │
                                        │   - based on setup platforms               │
                                        │   - build different required redis         │
                                        │artifacts                                   │
                                        └───────────────────────┬────────────────────┘
                                                                │                     
                                                                │                     
                                                                │ ┌────┐              
                   .─────────────────────────────────────.      │ │push│              
        ┌─────┐ ─▶(   2.3 ) stream of artifact benchmarks )◀────┘ └────┘              
        │pull ││   `─────────────────────────────────────'                            
        └─────┘                                                                       
               │                                                                      
                                        ┌────────────────────────────────────────────┐
               │                        │                                            │
                                        │3) benchmark_coordinator                    │
               │                        │   - based on test-suites and setups:       │
                                        │      - Trigger env setup                   │
               └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│      - 3.1 ) Trigger topology setup        │
                                        │      - 3.2 ) Run benchmarks                │
                                        │      - Record results into datasinks       │
                                        │                                            │
                                        └────────────────────────────────────────────┘
```                                              

In a very brief description, github.com/redis/redis upstream changes trigger an HTTP API call containing the
relevant git information. 

The HTTP request is then converted into an event ( tracked within redis ) that will trigger multiple build variants requests based upon the distinct platforms described in [`platforms`](redis_benchmarks_specification/setups/platforms/). 

As soon as a new build variant request is received, the build agent ([`redis-benchmarks-spec-builder`](https://github.com/filipecosta90/redis-benchmarks-specification/tree/main/redis_benchmarks_specification/__builder__/)) prepares the artifact(s) and proceeds into adding an artifact benchmark event so that the benchmark coordinator ([`redis-benchmarks-spec-sc-coordinator`](https://github.com/filipecosta90/redis-benchmarks-specification/tree/main/redis_benchmarks_specification/__self_contained_coordinator__/))  can deploy/manage the required infrastructure and DB topologies, run the benchmark, and export the performance results.
## Directory layout

### Specifications 

  The following is a high level status report for currently available specs.

* `redis_benchmarks_specification`
  * [`test-suites`](https://github.com/filipecosta90/redis-benchmarks-specification/tree/main/redis_benchmarks_specification/test-suites/): contains the benchmark suites definitions, specifying the target redis topology, the tested commands, the benchmark utility to use (the client), and if required the preloading dataset steps.
  
* `redis_benchmarks_specification/setups`
  * [`platforms`](https://github.com/filipecosta90/redis-benchmarks-specification/tree/main/redis_benchmarks_specification/setups/platforms/): contains the standard platforms considered to provide steady stable results, and to represent common deployment targets.
  * [`topologies`](https://github.com/filipecosta90/redis-benchmarks-specification/tree/main/redis_benchmarks_specification/setups/topologies/): contains the standard deployment topologies definition with the associated minimum specs to enable the topology definition.
  * [`builders`](https://github.com/filipecosta90/redis-benchmarks-specification/tree/main/redis_benchmarks_specification/setups/builders/): contains the build environment variations, that enable to build Redis with different compilers, compiler flags, libraries, etc...

### Spec tool implementations

  The following is a high level status report for currently available spec implementations.

* **STATUS: Experimental** [`redis-benchmarks-spec-api`](https://github.com/filipecosta90/redis-benchmarks-specification/tree/main/redis_benchmarks_specification/__api__/) : contains the API that translates the POST HTTP request that was triggered by github.com/redis/redis upstream changes, and fetches the relevant git/source info and coverts it into an event ( tracked within redis ).

* **STATUS: Experimental** [`redis-benchmarks-spec-builder`](https://github.com/filipecosta90/redis-benchmarks-specification/tree/main/redis_benchmarks_specification/__builder__/): contains the benchmark build agent utility that receives an event indicating a new build variant, generates the required redis binaries to test, and triggers the benchmark run on the listening agents.
* **STATUS: Experimental** [`redis-benchmarks-spec-sc-coordinator`](https://github.com/filipecosta90/redis-benchmarks-specification/tree/main/redis_benchmarks_specification/__self_contained_coordinator__/): contains the coordinator utility that listens for benchmark suite run requests and setups the required steps to spin the actual benchmark topologies and to trigger the actual benchmarks.


## Contributing guidelines

### Adding new test suites

TBD

### Adding new topologies

TBD

### Adding new test platforms

TBD

## License

redis-benchmark-specifications is distributed under the Apache 2 license - see [LICENSE](LICENSE)