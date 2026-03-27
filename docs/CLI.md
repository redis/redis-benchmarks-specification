# CLI Reference

## redis-benchmarks-spec-cli

### Trigger benchmarks

```bash
redis-benchmarks-spec-cli --tool trigger \
    --redis_host <host> --redis_port <port> --redis_pass <pass> \
    --use-branch --branch <branch> --last_n 1 \
    --gh_org <org> --gh_repo <repo>
```

| Flag | Default | Description |
|------|---------|-------------|
| `--use-branch` | - | Iterate over git commits on a branch |
| `--use-tags` | - | Iterate over git tags |
| `--branch` | `unstable` | Branch to use |
| `--last_n` | `1` | Number of commits to trigger |
| `--gh_org` | `redis` | GitHub organization |
| `--gh_repo` | `redis` | GitHub repository |
| `--redis_repo` | - | Local path to redis repo (skip clone) |
| `--git_hash` | - | Comma-separated list of specific commit hashes |
| `--arch` | `amd64` | Target architecture (`amd64`, `arm64`) |
| `--platform` | - | Only trigger on a specific platform |
| `--tests-regexp` | `.*` | Regex filter for test names |
| `--tests-groups-regexp` | `.*` | Regex filter for test group names |
| `--command-regex` | `.*` | Regex filter for tested commands |
| `--deployment-name-regexp` | `.*` | Regex filter for topologies/deployments. Only matching topologies will execute. E.g. `'oss-standalone-0[48]-io-threads'` |
| `--tests-priority-lower-limit` | `0` | Min priority |
| `--tests-priority-upper-limit` | `100000` | Max priority |
| `--build_command` | - | Custom build command. E.g. `"sh -c 'make -j'"` |
| `--build_artifacts` | - | Build artifacts to produce |
| `--pull-request` | - | PR number to attach results to |
| `--dry-run` | `false` | Show what would be triggered without triggering. Includes topology breakdown: `N tests x M topologies = X benchmark runs` |
| `--wait-build` | `false` | Wait for build to complete |
| `--wait-benchmark` | `false` | Wait for benchmark execution to complete (implies `--wait-build`). Shows per-platform progress with ETA |
| `--wait-benchmark-timeout` | `-1` | Timeout in seconds for `--wait-benchmark`. `-1` = forever |

**Examples:**

```bash
# Trigger last commit on a branch, wait for everything
redis-benchmarks-spec-cli --tool trigger \
    --use-branch --branch unstable --last_n 1 \
    --wait-benchmark --wait-benchmark-timeout 7200

# Only run IO-thread topologies for a specific test
redis-benchmarks-spec-cli --tool trigger \
    --use-branch --branch unstable --last_n 1 \
    --tests-regexp 'memtier_benchmark-1Mkeys-string-setget2000c.*' \
    --deployment-name-regexp 'oss-standalone-0[48]-io-threads'

# Dry-run: see topology breakdown
redis-benchmarks-spec-cli --tool trigger \
    --use-branch --branch unstable --last_n 1 \
    --deployment-name-regexp 'oss-standalone$' \
    --dry-run
```

---

### Admin CLI

```bash
redis-benchmarks-spec-cli --tool admin --admin-command <command> [options]
```

#### `health` — Runner health table

Shows version, architecture, status, uptime, heartbeat age, active filters, and current work for every runner. Requires runners to be running version 0.2.64+ (with heartbeat support).

```bash
redis-benchmarks-spec-cli --tool admin --admin-command health
```

```
PLATFORM                                      ARCH   VERSION    STATUS    BEAT   UPTIME   FILTERS                   CURRENT WORK
--------------------------------------------- ------ ---------- --------- ------ -------- ------------------------- ----------------------------------------
arm-aws-m8g.metal-24xl                        arm64  0.2.64     running   10s    3d12h    exclusive-hw              1774518430559-0 (mt-1Mkeys-load-hash...)
arm-gcp-c4a-standard-48                       arm64  0.2.63     DOWN      5m     1d2h     tests=.*hash.*            1774446537117-0 (mt-100Kkeys-hash...)
x86-aws-m7i.metal-24xl                        amd64  0.2.64     waiting   10s    5d0h     -
```

STATUS values: `running` (executing tests), `waiting` (idle, waiting for work), `DOWN` (heartbeat stale > 120s)

#### `summary` — Fleet overview

```bash
redis-benchmarks-spec-cli --tool admin --admin-command summary
```

#### `work` — Active work table

Shows all running/pending/building work across the fleet with source info.

```bash
redis-benchmarks-spec-cli --tool admin --admin-command work [--platform <name>]
```

```
AGE      STATUS    PROGRESS     FAIL ETA    PLATFORM                       SOURCE                                                  RUNNING TEST
-------- --------- ------------ ---- ------ ------------------------------ ------------------------------------------------------- --------------------
1.4h ago RUNNING   48/60 (80%)       ~20m   arm-m8g.metal-24xl             tezc/redis hinted-hash cf2a2ee7de by fco@host (cli)    mt-1Mkeys-load-hash...
21.3h    RUNNING   247/297 (83%)   1 ~259m  arm-m8g.metal-24xl             unstable f9b9140f10                                     mt-3Mkeys-get-1KiB...
3.4d     BUILDING  -                        builders-cg-arm64              cc1002dd20 PR#14907
```

STATUS values: `RUNNING` (tests executing), `PENDING` (in runner queue), `QUEUED` (claimed but not started), `BUILDING` (waiting for build)

#### `runners` — Per-runner detail

```bash
redis-benchmarks-spec-cli --tool admin --admin-command runners
```

#### `builders` — Per-builder detail

```bash
redis-benchmarks-spec-cli --tool admin --admin-command builders
```

#### `queues` — Benchmark runs per platform

```bash
redis-benchmarks-spec-cli --tool admin --admin-command queues --platform <name>
```

```
STREAM ID                 AGE      PROGRESS       STATUS       COMMIT
------------------------- -------- -------------- ------------ ----------------------------------------
1774518430559-0           1.4h ago 48/60          RUNNING *    tezc/redis hinted-hash cf2a2ee7de
1774446476636-0           21.4h    297/297        DONE (6F)    unstable f9b9140f10
1774233585132-0           3.4d     297/297        DONE (6F)    YangboLong:fix_cache_line cc1002dd20 PR#14907
```

`RUNNING *` = this runner is actively processing this stream right now

#### `status` — Detailed run status

```bash
redis-benchmarks-spec-cli --tool admin --admin-command status --stream-id <id>
```

#### `cancel` — Cancel a benchmark run

Flushes pending tests and signals the coordinator to break out of its test loop via Redis key.

```bash
# Cancel all pending tests for a run
redis-benchmarks-spec-cli --tool admin --admin-command cancel --stream-id <id>

# Cancel a specific test
redis-benchmarks-spec-cli --tool admin --admin-command cancel --stream-id <id> --tests-regexp <exact-test-name>
```

#### `skip` — Skip all work on a runner

ACKs all pending stream messages, flushes test queues, signals coordinator reset, and resets consumer group to latest. The runner keeps running and picks up only new work.

```bash
redis-benchmarks-spec-cli --tool admin --admin-command skip --platform arm-gcp-c4a-standard-48
```

#### `reset` — Nuclear reset

Destroys and recreates the runner's consumer group. Use when a runner is completely stuck.

```bash
redis-benchmarks-spec-cli --tool admin --admin-command reset --platform arm-gcp-c4a-standard-48
```

---

## redis-benchmarks-compare

### Compare benchmarks

```bash
redis-benchmarks-compare \
    --baseline-branch unstable \
    --comparison-hash <hash> \
    --deployment_name oss-standalone \
    --running_platform arm-aws-m8g.metal-24xl
```

### New flags

| Flag | Description |
|------|-------------|
| `--list-deployments` | Discover available deployment names from TimeSeries for the given filters. Prints list and exits |
| `--deployment-name-regexp` | Filter deployment names by regex when using `--compare-by-env` |
| `--deployment_name` (comma-separated) | Auto-enables cross-topology comparison. E.g. `oss-standalone,oss-standalone-04-io-threads` |
| `--compare-by-env` | Compare same test across different topologies |

**Examples:**

```bash
# Discover what deployments have data
redis-benchmarks-compare --list-deployments \
    --baseline-branch unstable \
    --running_platform arm-aws-m8g.metal-24xl

# Cross-topology comparison (auto-enables --compare-by-env)
redis-benchmarks-compare \
    --deployment_name "oss-standalone,oss-standalone-04-io-threads,oss-standalone-08-io-threads" \
    --baseline-branch unstable \
    --comparison-hash <hash> \
    --test memtier_benchmark-1Mkeys-string-setget2000c-1KiB-pipeline-10

# Dynamic topology discovery via regex
redis-benchmarks-compare --compare-by-env \
    --deployment-name-regexp "oss-standalone.*io-threads" \
    --baseline-branch unstable \
    --comparison-hash <hash> \
    --test memtier_benchmark-1Mkeys-string-setget2000c-1KiB-pipeline-10
```
