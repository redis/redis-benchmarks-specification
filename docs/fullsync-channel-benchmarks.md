# Full-sync RDB-channel coverage

These two specifications add the direct child-to-replica RDB channel
(`repl-rdb-channel: 'yes'`) to the 20M-key random 1KiB full-sync workload. They
are the random cells of the parent-forwarding matrix in #577 with only the
channel setting flipped, so the transfer path is the single changed variable.
Preserve the #577 specifications as separate controls.

Run these cases only against server builds that recognize `repl-rdb-channel`
(Redis 8.0 or later). An older build rejects the parameter at startup; the run
fails rather than being skipped or measured with a different transfer path.

## Procedure

Qualification, repeatability and reporting rules are shared with the parent
matrix: follow `docs/fullsync-benchmark-qualification.md` from #577 (identity
recording, exact key counts on both instances, five retained observations in
alternating order, rehash-transition caveat, phase-specific profiling, memory
reservation). Do not duplicate or fork that checklist here.

Channel-specific additions:

- Verify the primary log reports transfer to the replica sockets by the child,
  and record the actual compression/checksum negotiation; configuration alone
  is insufficient evidence of the operations performed.
- Profile parent, child and replica CPU separately; the channel moves work
  between them.
- Random values are an incompressible control, not a representative
  compression-ratio benchmark.

## Prerequisites and limits

- Requires the corrected full-sync timing boundaries and exact primary/replica
  count checks from #576. Do not accept numbers from the earlier wait-only
  timing path; confirm the deployed coordinator commit, not just the package
  version. Data-directory selection also requires the support from #581.
- The coordinator's 600 second sync deadline is not spec-settable. The
  disk-backed cell measured a 94.6 second median on local NVMe; slower storage
  leaves a smaller margin, and a deadline expiry is a failed run, not a sample.
