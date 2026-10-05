# Full-size initial-sync qualification, 2026-09-21

All four cells pass the predeclared CV <= 5% repeatability budget on one x86 AWS m7i.metal-24xl runner. This establishes single-revision repeatability with an EBS-backed data mount. Supplementary I/O observations show a storage throughput constraint in random/disk-backed loading. These results do not qualify a CPU/serialization regression gate, establish a performance improvement, or qualify other storage paths and runners.

Each attempt starts fresh primary and replica processes and loads 20 million 1024-byte string values. Exact key counts and full-dataset digests match. Every duration was exported and read back at its unique sample timestamp through both revision and branch series.

| Payload | Replica load | Formal n | Median (s) | Sample SD (s) | CV (%) | Min–max (s) |
|---|---|---:|---:|---:|---:|---:|
| random | disabled | 5 | 187.886 | 0.060 | 0.032 | 187.784–187.943 |
| random | on-empty-db | 5 | 27.147 | 0.406 | 1.489 | 26.995–28.011 |
| repeated | disabled | 5 | 68.549 | 0.088 | 0.128 | 68.464–68.658 |
| repeated | on-empty-db | 5 | 27.193 | 0.167 | 0.616 | 26.847–27.246 |

The [machine-readable report](fullsync-qualification-20260921.json) retains all 21 raw timings, original attempt statuses, cohort membership, digests, source/binary identities and limitations. Twenty fully observed attempts form the formal cohort. The first random/disabled attempt remains diagnostic, and all six timings in that cell also pass the CV budget.

Qualification used [#576](https://github.com/redis/redis-benchmarks-specification/pull/576) at `88b4f0bd1e0fcf82b93a91585636c9df625cce14` with the four specs from [#577](https://github.com/redis/redis-benchmarks-specification/pull/577) at `83fc17033cf3942c2b13fdb0bf0485a660b43650`. The timing implementation is unchanged; correctness and export observation hooks execute outside its measured interval.
The host has an Intel Xeon Platinum 8488C, 96 logical CPUs, and Linux
6.14.0-1010-aws. Primary and replica use distinct physical cores (logical CPUs
8 and 13); the client uses CPUs 9-12. Data files use an ext4 host bind mount on
a 256 GiB EBS device. Networking is local to the host. The client image, server
binary SHA256, source revisions and resolved configuration are in the JSON report.
Build metadata attributes the server to Redis commit
`16e021b465fbbc73d5f070b4da5ce9f5ee4baea1`; INFO reports git_sha1=0 and dirty=1
for the archive build, so commit provenance comes from the cached build metadata.

No swap or cgroup OOM event was observed. Sampled process monitoring found no
competing benchmark activity. Container memory limits were unset: the `80g`
request was not tested as an enforced cap. Whole-attempt memory observations
include file cache, digest checks and the subsequent GET workload.

The first attempt encountered a string/bytes error in observation code after
successful export. Its original failed status is retained, with independent
read-back of the exact original sample. That attempt is diagnostic because
process sampling started after its sync. An extra random/disabled attempt was
predeclared to obtain five fully monitored runs; all six timings also satisfy
the variability budget. No duration was excluded based on its value.

Post-timing DEBUG DIGEST checks exceeded the ordinary replication timeout in
the original twenty attempts. Equal digests establish dataset identity, not
post-digest replication continuity. The extra attempt temporarily extended the
timeout only after timing ended, then restored it before GET. Neither the later
GET results nor their profiles describe initial synchronization.

The disk-backed random-data trace has sustained writes near 125 MiB/s with
queueing, while streaming-load traces have negligible physical I/O. These are
late-added whole-device observations; the observed phase includes replica
startup outside the V2 timer and omits boundary intervals. They support a
storage constraint in the random/disk-backed cell, not attribution of every
policy difference to EBS. Compression and checksum negotiation also differ.

Track the controlled storage comparison in
[testing-infrastructure#162](https://github.com/redis-performance/testing-infrastructure/issues/162).
Use the same machine with EBS and local NVMe, preserve configuration and cache
policy, measure whether storage waiting decreases, and qualify repeatability
separately. A relevant code comparison is still required to demonstrate CPU
regression sensitivity. Other architectures, cross-host networking and other
storage paths remain unqualified by this run.
