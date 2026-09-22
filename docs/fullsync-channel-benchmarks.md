# Full-sync RDB-channel coverage

These two specifications add the direct child-to-replica RDB channel to the
20M-key random 1KiB full-sync workload. The channel setting is explicit because
changing it changes both the transfer path and where CPU work occurs. Preserve
the parent-forwarding specifications as separate controls.

Run these cases only against server builds supporting `repl-rdb-channel`.
An older build rejects the parameter at startup; it is deliberately not silently
skipped or measured with a different transfer path. The compiler variant name
does not guarantee that the selected server commit supports this setting.

The two replica loading policies (`disabled` and `on-empty-db`) are complete
configuration comparisons. Diskless loading may negotiate compression/checksum
bypass even when those settings are enabled in the parent's configuration.
Configuration alone is insufficient evidence of the operations performed.

## Coordinator prerequisite

These specifications require the corrected full-sync timing boundaries and exact
primary/replica preload count checks from #576. Do not accept numbers generated
by the earlier wait-only timing path. Confirm the deployed coordinator commit,
not just the package version. Data-directory selection also requires the
coordinator support introduced in #581.

## Qualification

Before comparing a source change:

- Record exact server, coordinator and client image identities, architecture,
  CPU placement and filesystem/storage type. Keep the selected storage fixed.
- Verify all 20M keys on both instances and matching logical contents. The
  preload must finish before replication begins.
- Verify the primary log reports transfer to replica sockets. Record the
  actual compression/checksum negotiation; parent configuration alone is
  insufficient.
- Record whether dictionary rehashing is still active after preload. A cohort
  crossing that transition is not a settled-dataset comparison. Stabilize
  outside timing or report the transition explicitly.
- Run at least five retained observations per condition in alternating order.
  Report all attempts, variability and paired effects; investigate order trends
  instead of excluding them after seeing the result.
- Profile the actual transfer interval and distinguish parent, child and replica
  CPU. The 30-second GET phase runs after full sync and cannot supply those
  profiles.
- Record physical reads/writes and memory pressure. Loading a just-received RDB
  from page cache is not a cold-storage restore benchmark.

The specifications reserve enough memory for both instances and use a pinned
client image. Random values are an incompressible control, not a representative
compression-ratio benchmark. Moderately compressible data, constrained networks,
foreground-write latency, multi-replica fan-out and slow-replica behavior require
separate workloads; these two files do not claim that coverage.
