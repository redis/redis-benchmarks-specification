# Benchmark Encoding Coverage

Summary of which Redis internal data structure encodings are exercised by the benchmark test suites.

## Default Redis Encoding Thresholds

| Type | Compact Encoding | Threshold | Large Encoding |
|------|-----------------|-----------|----------------|
| **String** | EMBSTR | ≤44 bytes | RAW (>44B), INT (numeric) |
| **Hash** | LISTPACK | ≤128 entries AND ≤64B/value | HASHTABLE |
| **Set** | INTSET | all integers, ≤128 entries | LISTPACK (≤128), HASHTABLE |
| **Sorted-Set** | LISTPACK | ≤128 entries AND ≤64B/value | SKIPLIST + HASHTABLE |
| **List** | LISTPACK | ≤128 entries AND ≤64B | QUICKLIST |
| **Stream** | RAX + LISTPACK | (single encoding) | — |
| **Geo** | ZSET | (same as sorted-set) | — |
| **HLL** | Sparse / Dense | (auto-promoted) | — |

Config parameters: `hash-max-listpack-entries 128`, `hash-max-listpack-value 64`,
`set-max-intset-entries 128`, `set-max-listpack-entries 128`,
`zset-max-listpack-entries 128`, `zset-max-listpack-value 64`,
`list-max-listpack-size -2`.

## Coverage Matrix

| Type | Compact Tests | Large Tests | Total | Gaps |
|------|--------------|-------------|-------|------|
| **String** | 31 (EMBSTR) | 69 (RAW) + 7 (INT) | 107 | — |
| **Hash** | 42 (LISTPACK) | 22 (HASHTABLE) | 64 | No transition test |
| **Set** | 4 (INTSET) + 11 (LISTPACK) | 14 (HASHTABLE) | 29 | No transition test |
| **Sorted-Set** | 29 (LISTPACK) | 7 (SKIPLIST) | 36 | No transition test |
| **List** | 10 (LISTPACK) | 18 (QUICKLIST) | 28 | No transition test |
| **Stream** | 19 (RAX+LP) | — | 19 | Single encoding |
| **Geo** | 11 (via ZSET) | — | 11 | — |
| **HLL** | 3 | — | 3 | — |

### How encodings are determined from test specs

- **Hash**: field count and value size in the test name/config determine encoding.
  - `5-fields-with-10B-values` → LISTPACK (5 < 128 entries, 10B < 64B/value)
  - `50-fields-with-100B-values` → HASHTABLE (100B > 64B hash-max-listpack-value)
  - `50-fields-with-10B-values` → LISTPACK (50 < 128, 10B < 64B)

- **Set**: test name indicates element type and count.
  - `intset-with-100-elements` → INTSET (all integers, 100 < 128)
  - `set-200K-elements` → HASHTABLE (200K >> 128)

- **Sorted-Set**: element count determines encoding.
  - `100-elements` → LISTPACK (100 < 128)
  - `256K-elements` → SKIPLIST + HASHTABLE

- **String**: data size in the test name.
  - `10B`, `32B` → EMBSTR (≤44B)
  - `100B`, `512B`, `1KiB`, `4KiB` → RAW
  - `incr`, `incrby`, `incrbyfloat` → INT encoding

### Known Gaps

1. **No encoding transition benchmarks** — no test forces a mid-benchmark conversion
   from compact to large encoding (e.g., HSET adding field 129 to a 128-field hash,
   converting LISTPACK→HASHTABLE).
2. **No mixed-encoding workload** — no test exercises both encodings of the same type
   in the same keyspace (e.g., some hashes as listpack, others as hashtable).

### Guidelines for new specs

Every benchmark YAML `description` field should note which encoding the test
exercises. Example:

```yaml
description: ... Encoding: listpack (5 fields x 10B values, below
  hash-max-listpack-entries 128 and hash-max-listpack-value 64).
```
