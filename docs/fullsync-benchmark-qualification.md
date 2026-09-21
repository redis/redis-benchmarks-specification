# Initial full-sync benchmark qualification

These four specs hold key count, value size, topology, transfer channel and
primary configuration constant while varying payload entropy and replica load
policy. Repeated and random bytes are extreme controls. Neither represents
moderately compressible application data.

`ReplicationFullSyncSecondsV2` starts immediately before an explicit `REPLICAOF`
on a ready, empty process and ends after observing a usable replication link.
It includes handshake, configured diskless delay, RDB transfer and loading.
Startup is excluded. Observation delay includes the 50 ms polling interval, INFO response latency,
and any loading-related timeout/reconnect gaps. Deadline expiry, unexpected
full-sync counts and key-count mismatches fail the run.
Do not combine this metric with historical `ReplicationFullSyncSeconds` samples.

`on-empty-db` can change compression and checksum negotiation as well as the
loading path. Compare it as a complete policy. The primary sends disklessly in
both arms; `disabled` means the receiver stages the RDB on disk. The subsequent
GET workload is outside the sync window. Its throughput, latency and profiles
must not be presented as measurements of the initial sync.

Before accepting a performance result:

1. Record server commit, build options, allocator, coordinator version, client
   image digest, resolved server configuration, CPU model/affinity, kernel,
   storage/filesystem, memory limit, swap activity and network topology. Use the
   same immutable binaries and client image throughout the comparison.
2. Run on a dedicated runner with enough memory for both datasets and process
   overhead. Reject swapping, competing jobs, restarts or failed saves/syncs.
   Do not compare Docker overlay storage with a bind-mounted filesystem.
3. Validate exact primary and replica key counts and matching dataset digests
   outside the measured window. Run both payloads and both load policies.
4. Collect at least five independent fresh primary/replica runs per cell and
   revision, interleaving baseline/candidate order. Retain every attempt,
   including failures; give repetitions unique sample identities so storage
   deduplication cannot collapse them. Do not select the fastest run.
5. Report raw durations, median, standard deviation, coefficient of variation
   and range. Require baseline CV <= 5% on each cell as an initial qualification
   budget. If a cell fails, investigate the environment or workload before
   increasing repetitions; do not discard outliers to make it pass.
6. Accept a claimed change only when the paired effect exceeds the measured
   baseline variation and timer uncertainty, with a confidence interval that
   excludes zero. Five runs are a minimum, not a guarantee of precision. Keep
   architectural/storage strata separate and disclose regressions in any cell.
7. Verify actual exported TimeSeries samples, their units, revision and run
   identities. A successful trigger or YAML schema check is insufficient.

The specs remain unqualified until this protocol has been run on each target
runner. Small-scale argument and digest checks establish functionality only.
They are not evidence for a speedup or an acceptable noise floor.

Separate follow-ups are needed for moderately compressible fixtures,
bandwidth/RTT control, concurrent replica fan-out, writes during synchronization,
and profiling of the initial sync phase. The current coordinator starts replicas
serially and starts its load client and profilers after initial synchronization.
