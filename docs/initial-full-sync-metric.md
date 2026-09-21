# Initial full-sync metric transition

`ReplicationFullSyncSecondsV2` replaces `ReplicationFullSyncSeconds` for newly
collected results. The old timer started after a five-second container startup
wait, potentially missing the entire sync, and emitted the timeout as a duration.
Historical samples cannot be corrected by adding a constant offset.

The new interval starts immediately before `REPLICAOF` on a ready, empty process
and ends after INFO reports a replica with an up link and no initial sync in
progress. It includes handshake, configured diskless-sync delay, transfer and
loading, but excludes container startup. Observation delay includes the 50 ms poll interval, INFO latency, and any
loading-related timeout/reconnect gaps. It is not an exact server-side event
timestamp; repeated probe timeouts can increase uncertainty beyond 50 ms.

Deadline expiry and connection failures fail the test instead of exporting a duration.
INFO timeouts or busy-loading replies during RDB loading retry within the original
deadline. The startup argv retains `--replicaof no one`, preserving the replica
marker used by process-role classifiers while deferring replication.
The primary must report exactly one full sync. When a preload declares a key
count, both primary and replica counts must match. Network reads have a one-second
socket bound; a stalled read may extend failure handling beyond the sync deadline
by that bound, but cannot create a successful sample after the deadline.

Compare only V2 samples collected with the same configuration and runner.
Replicas still start and synchronize serially. The reported aggregate is the
maximum individual duration, not concurrent fan-out wall time. Benchmark clients
and profilers still start after initial synchronization; their results describe
the later workload. This change makes no performance improvement claim.
