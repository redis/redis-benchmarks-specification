# RDB save and load benchmarks

Version 0.3.47 adds a 24-test value-size sweep and a random-payload BGSAVE
control. Run these through the self-contained coordinator on qualified disk
runners, using explicit test and target-platform selection.

## Value-size sweep

The `memtier_benchmark-rdb-{save,load}-{size}B-{random,repeated}` tests vary
value size while keeping key names at 16 bytes (`rdbsize:` plus eight digits).
Each size has both payload types and both operations:

| Value bytes | Keys |
| --- | --- |
| 16 | 5,000,000 |
| 64 | 5,000,000 |
| 256 | 2,000,000 |
| 1,024 | 1,000,000 |
| 4,096 | 256,000 |
| 16,384 | 64,000 |

Random payloads provide an incompressible control. Repeated filler provides a
highly compressible extreme, rather than representative app data. The preload
uses the pinned memtier image, four threads, ten clients, partitioned sequential
keys, and pipeline 50. Each key count is divisible by the 40 preload clients.
The changing dataset sizes mean cross-size timings are not per-byte comparisons.

Each measurement makes **one request**, on one connection with pipeline one:

- `SAVE`: synchronous serialization and writing, including main-thread blocking.
- `DEBUG RELOAD NOSAVE NOFLUSH`: reload a newly written RDB into an empty database.

For load, the coordinator saves and synchronously flushes the database before
the timed request. Compression and checksums are enabled. Automatic saves, AOF,
expiry, replicas, and concurrent writes are absent. Debug commands must be enabled
on the isolated benchmark server.

The primary metric is `ALL_STATS.Totals.Latency`, in **milliseconds; lower is
better**. Trigger independent repetitions (at least three per revision and
platform). Compare the same test on the same CPU, filesystem, and storage tier.
Do not substitute throughput or use a test-time override. Profiling is disabled
for these single-command measurements.

### Correctness checks

`dbconfig.rdb_checks` enables coordinator-owned setup and validation. It specifies
`operation` (`save` or `load`), `keyspacelen`, `value_size`, and `sample_keys`.
Before timing, the coordinator verifies the exact key count and sampled value
lengths, retains the sampled bytes, prepares the load if requested, and resets
command statistics. After timing, it requires exactly one successful SAVE or
DEBUG command, the expected key count, unchanged sampled bytes, and, for load,
matching loaded/expired counters. Any failed check prevents metric export.
Validation uses native commands and does not require Lua.

The checks support one standalone primary. Use the self-contained coordinator;
the legacy client-runner does not implement this setup protocol.

### Storage interpretation

Configure the coordinator's data directory on the intended disk and verify the
server mount before running. Record the filesystem and storage tier alongside
results. The load file was just written and no cache drop occurs: this is **not a
cold-storage read benchmark**. SAVE completion does not establish durable fsync
latency. Neither endpoint measures BGSAVE fork pause or foreground tail latency.

## BGSAVE controls

`memtier_benchmark-12Mkeys-string-1KiB-bgsave-duration` is the existing repeated
payload test. `memtier_benchmark-12Mkeys-string-1KiB-random-bgsave-duration` adds
the matched random payload test. Both preload 12 million 1 KiB values and wait
for the background save to finish before exporting `RdbLastBgsaveTimeSec` and
`RdbLastForkUsec`. The former is whole-second duration, the latter fork time in
microseconds. Whole-second quantization limits small-change conclusions.

## Deployment and selection

Install `redis-benchmarks-specification==0.3.47` on an idle, reserved disk runner,
then restart its coordinator so it discovers the packaged YAML files. Preserve
its disk data directory and explicit-only routing. Select the value-size family
with `memtier_benchmark-rdb-(save|load)-.*` or individual test names. The two
BGSAVE controls are selected separately. Verify completed-test counts and fresh
TimeSeries samples after each trigger; an acknowledged stream alone is not proof
that any test ran.

### Enforced local-disk runner restriction

All 24 SAVE/load cases and both 12M-key BGSAVE controls declare
`required-runner-capabilities: [local-disk]`. The coordinator filters these out
before adding pending work unless its startup configuration includes
`--runner-capabilities local-disk`. The default capability set is empty.
Broad suites, exact test selection, and stream `target_platform` fields do not
grant capabilities and cannot bypass this restriction. The legacy client-only
runner always excludes these tests because it cannot qualify server storage.

Enable this capability only on dedicated local-disk benchmark runners after
verifying that their data directory is on local instance storage. Keep
`--explicit-only`, the storage-specific platform identity, and the verified
data directory. Do not enable it on general-purpose, profiler, or network-volume
runners. Capability configuration describes an operator-qualified machine; it
does not automatically detect disk locality. The runner heartbeat reports its
configured capabilities for deployment verification.
