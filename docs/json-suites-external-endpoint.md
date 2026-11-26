# Running the JSON suites against an external endpoint

The JSON suites (every test-suite whose file name contains `-json-`, 98 at the time of writing) can be run by the
client runner against an endpoint you do not control: a managed service that speaks the Redis protocol and serves
the RedisJSON commands, where you cannot restart the server, load a module, use `DEBUG`, `CONFIG SET` or place an
RDB file.

What makes them portable:

* **Client-side datasets.** Every dataset is written by the client: either a memtier preload (`preload_tool`) or
  `init_commands` that `JSON.SET` the exact document text at the root of each key (RedisJSON's benchmark documents,
  which RedisJSON's own specs load from RDB files, and the 1,000 NCBI RefSNP records of the
  `50Kkeys-json-ncbi-refsnp` family), plus `COPY` where a document cannot go through a memtier argument (the NCBI
  records, copied to 50,000 keys, and the 153 KB issue-674 document, copied to 1,000). In the jsonsl-1 preload,
  backticks and `$` before a name are written as `\u0060`/`\u0024` JSON escapes, so the shell of
  `--benchmark_local_install` and the shlex split of the container path read the argument alike; the stored values
  are unchanged. Nothing is read from a file on the server.
* **Keyspaces larger than the cache.** Every suite works on at least 10,000 keys (the two issue-674 suites on 1,000
  keys of an 831 KB document), sized so that its dataset, measured as `used_memory` growth on redis:8.2.11, is
  about 190 to 600 MB: larger than the last-level cache of current server CPUs, so a command reads a document that
  is not cache resident, as on a production keyspace. A one-key suite measures a document that sits in L2 or L3.
  Each suite's description and `dataset_description` state its key count and footprint.
* **Idempotent preloads, no exact keyspace check.** A preload rewrites whole documents, so a rerun restores the
  dataset whatever earlier suites did to it. No JSON suite carries `check: keyspacelen` (the runner compares the
  endpoint's total key count, which fails as soon as another suite's keys are present); suites with a memtier
  preload check instead, as their first init command, that the last preloaded key exists. The suites therefore run
  one after another on a shared database with or without `--flushall_on_every_test_start`.
* **Canary.** The last init command of every suite issues the measured command once; an error reply (a JSONPath
  the server rejects, a command the endpoint does not allow, a module configuration it lacks) aborts that suite
  instead of reporting a throughput number for error replies. The runner then continues with the next suite.
* **No server configuration.** The client runner does not apply `configuration-parameters`; it reads `INFO` and
  `COMMAND`, and tries `CONFIG RESETSTAT` and `CONFIG GET io-threads` for its report. An endpoint that refuses
  `CONFIG` answers both with an error that the runner tolerates; they are the only error replies a passing suite
  leaves in the endpoint's `INFO errorstats` (two per suite).

## One command

Install the package (`pip install redis-benchmarks-specification`, or `poetry install` in a checkout) and a
`memtier_benchmark` built with TLS support, then:

```bash
cd "$CERT_DIR"   # the directory that holds the endpoint's CA certificate, ca.crt (TLS only, see below)
redis-benchmarks-spec-client-runner \
  --db_server_host "$HOST" --db_server_port "$PORT" --db_server_password "$PASSWORD" \
  --tls --cacert ca.crt \
  --tests-regexp '/(?![^/]*(?:eval|dump|debug-memory|auto-create))[^/]*-json-[^/]*\.yml$' \
  --flushall_on_every_test_start \
  --benchmark_local_install --memtier-bin-path "$(command -v memtier_benchmark)" \
  --override-memtier-test-time 30 \
  --client_aggregated_results_folder ./json-results --platform-name my-endpoint
```

* TLS: the runner copies the `--cacert` (and `--cert`/`--key`) file into a temporary directory and hands memtier
  only its file name. With `--benchmark_local_install` memtier runs in the runner's current directory, so start the
  runner from the directory that holds the CA file and pass the bare file name, as above; otherwise memtier fails with
  "Failed to load CA certificate file" and the suite is skipped. `--tls --tls-skip-verify` instead of `--cacert`
  encrypts without verifying the endpoint's certificate. Drop the TLS flags for a plain-TCP endpoint.
* Without `--benchmark_local_install` the runner starts memtier in the `redislabs/memtier_benchmark:edge` container
  (Docker on the client host).
* `--override-memtier-test-time` shortens the 120-second (180 for the two issue-674 suites) measured phases; omit it
  for full-length runs. Suites bounded by a request count (see below) ignore it.
* `--flushall_on_every_test_start` gives every suite an empty database. The suites do not need it for correctness
  (see above), but without it the endpoint keeps every earlier suite's keys: the widened suites alone write 22 key
  families of about 190 to 830 MB each, about 9.4 GB if none is flushed, on top of the `load` suites' 1.2 GB. Pass
  it unless the endpoint has room for about 11 GB.
* The regular expression is matched against each suite's path; the lookahead keeps it on the file name. To run the
  whole JSON set, use `--tests-regexp '/[^/]*-json-[^/]*\.yml$'`.

Validated end to end (every suite, through this runner, 5-second measured phases) against a `redis-server` with the
RedisJSON module configured like a managed endpoint: `requirepass`, `enable-debug-command no`,
`enable-module-command no`, `CONFIG` and `MODULE` renamed away: all suites over TLS (`--tls-skip-verify`) with
`--flushall_on_every_test_start`, all over plain TCP without it (one shared database), and three over TLS with the
`--cacert` form above. Every suite completed
with its canary accepted and no error reply other than the runner's two `CONFIG` attempts; the two `auto-create`
suites stop at their canary unless the module configuration is enabled, and pass when it is. That validation ran
before the keyspaces were widened to 10,000 keys and more. After the widening, every widened suite was re-checked
against redis:8.2.11 with `sh -c`, as `--benchmark_local_install` runs memtier, but not through the runner itself:
its preload (shrunk to 300 keys; the 20,000-key yelp, 500,000-key pass-100 and 5,000,000-key num preloads also at full size), its full init commands, its stored
document compared with the original text stored by `JSON.SET`, and a 2-second client run.

## Suites that need more than the JSON commands

| Needs | Suites (file-name token) | Notes |
|---|---|---|
| `EVAL` (scripting) | `eval` (5 suites: two `arrinsert-*-eval-reset` ports of RedisJSON specs, three `eval-dump-restore-replace`) | `JSON.ARRINSERT`, `JSON.SET`, `DUMP` and `RESTORE` run from the script; the keys are declared, so they route on a sharded endpoint |
| `DUMP` | `dump` (4 DUMP suites, plus the 3 `eval-dump-restore-replace`) | |
| `RESTORE` | `eval-dump-restore-replace` (3) | the payload is the endpoint's own `DUMP` inside the script, so no RDB or module encoding version is pinned |
| `JSON.DEBUG` | `debug-memory` (2) | `JSON.DEBUG MEMORY` |
| module configuration `json-auto-create-deep-paths yes` | `auto-create` (2, `memtier_benchmark-1Bkeys-load-json-*`) | default is `no`; the endpoint operator must enable it, otherwise the canary aborts the suite. `JSON.MSET` writes two keys per request: standalone endpoints only |
| `COPY` | `ncbi-refsnp` (3) and the two issue-674 `test-get-recursive-descent` suites, in their init commands | a core command since Redis 6.2; copies a RedisJSON value through the module's copy callback |
| `FLUSHALL` | none, unless `--flushall_on_every_test_start` / `--flushall_on_every_test_end` is passed | |

The default regular expression above excludes every row of this table. To add some back, remove their token from the
lookahead, for example `(?:debug-memory|auto-create)` to run the `EVAL`, `DUMP` and `RESTORE` suites as well.

## Memory and duration

* Every suite that preloads holds about 190 to 600 MB (measured on redis:8.2.11, stated per suite), the two
  issue-674 suites 831 MB; the `load` suites grow to their stated key range during the run (the
  `10Kkeys-load-json-set-fulldoc-api-replies-q5` suite up to 10,000 documents of 77 KB, about 1.3 GB). Request at
  least 1 GB more than the suite's dataset on the endpoint.
* Preloads are memtier runs at pipeline 50 from one connection: on a local redis-server 20,000 yelp documents load in
  about 4 s, 500,000 pass-100 documents in about 3 s and the 5,000,000-key suites in about 23 s; on a remote endpoint
  add the transfer of the dataset (up to about 760 MB of request text for the 20,000-key yelp suites).
* Bounded by a request count rather than by time: `200Kkeys-json-path-sonde-foo-arrappend-geojson-feature`
  (1,000,000 appends spread over 200,000 arrays; RedisJSON's spec appends to one array) and the two `auto-create`
  suites.
* The `50Kkeys-json-ncbi-refsnp-*` suites write their 1,000 records and 49,000 copies with one round trip each
  before the measured phase, and the issue-674 suites their document and 999 copies; on a remote endpoint that takes
  50,000 (respectively 1,000) round-trip times.
* **Requests larger than 16 KB measure the client, not the endpoint** with the memtier builds this set was validated
  with: memtier turns Nagle's algorithm on (`TCP_NODELAY` 0) and writes such a request in two parts, so every
  request waits for the endpoint's delayed ACK (about 40 ms on Linux) and the suite reports roughly
  connections / 40 ms. Over TLS the same happens from about 1.2 KB. Affected: the whole-document `JSON.SET` suites of
  the 18 KB, 38 KB and 77 KB documents (and, over TLS, of the 1.2 to 12 KB ones) and the EVAL suites whose script
  text is long. Their numbers become meaningful once memtier sets `TCP_NODELAY`; the commands and datasets are
  correct as they are.
* Measure from a client host close to the endpoint; the suites report throughput for their stated client population
  (connections, threads, pipeline), so client-side limits and network latency are part of the number.
