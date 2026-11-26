#!/usr/bin/env bash
# Run a subset of the test suites through the client runner against a server container and assert the outcome.
#
#   utils/ci/check_suites_against_server.sh <image> <skip|run> <tests-regexp> [server args...]
#
# skip : the server lacks the tested commands (e.g. OSS Redis or Valkey without RedisJSON); every matched suite
#        must be skipped by the runner's command-support check and none may run to completion.
# run  : the server implements the tested commands; every matched suite must run, with zero error replies from
#        the server (memtier reports `-ERR` replies on stderr but still exits 0, so the runner cannot see them).
#
# Requires docker, python3, redis-cli (or the redis:7.2-alpine image, used as a fallback) and the runner on PATH
# (`redis-benchmarks-spec-client-runner`, e.g. via `poetry run`). Uses host networking.
set -euo pipefail
image=$1; mode=$2; regexp=$3; shift 3
port=${PORT:-6399}
test_time=${TEST_TIME:-5}
name=rbs-compat-$$
suites_dir=${SUITES_DIR:-redis_benchmarks_specification/test-suites}
log=${LOG:-compat-$(echo "$image" | tr '/:' '__').log}

cli() { if command -v redis-cli >/dev/null 2>&1; then redis-cli -p "$port" "$@"; else docker run --rm --network host redis:7.2-alpine redis-cli -p "$port" "$@"; fi; }
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker run -d --name "$name" --network host "$image" "$@" >/dev/null
for _ in $(seq 1 120); do cli ping 2>/dev/null | grep -q PONG && break; sleep 0.5; done
cli ping | grep -q PONG || { echo "server $image did not answer PING on port $port"; docker logs "$name" | tail -20; exit 1; }
echo "server: $image, port $port, modules: $(cli module list 2>/dev/null | tr '\n' ' ' | cut -c1-200)"

# Same engine and semantics as the runner (Python re.search on the suite's path), so look-aheads work.
expected=$(python3 - "$suites_dir" "$regexp" <<'PYEOF'
import os, re, sys
d, rx = sys.argv[1], re.compile(sys.argv[2])
print(sum(1 for f in os.listdir(d) if f.endswith(".yml") and rx.search(os.path.join(d, f))))
PYEOF
)
[ "$expected" -gt 0 ] || { echo "no suite in $suites_dir matches $regexp"; exit 1; }

set +e
redis-benchmarks-spec-client-runner --db_server_host 127.0.0.1 --db_server_port "$port" \
  --tests-regexp "$regexp" --override-memtier-test-time "$test_time" --flushall_on_every_test_start \
  ${RUNNER_EXTRA_ARGS:-} > "$log" 2>&1
rc=$?
set -e

# Only WARNING-level and printed lines are relied on: the runner's INFO lines are not always emitted.
skipped=$(grep -c 'due to unsupported commands' "$log" || true)      # WARNING from the COMMAND gate
completed=$(grep -c '^# Results for .* test-case' "$log" || true)   # printed once per test that ran to the end
error_replies=$(grep -c 'handle error response' "$log" || true)     # memtier stderr, relayed in the container output
tracebacks=$(grep -c 'Traceback' "$log" || true)
echo "runner exit=$rc matched_suites=$expected skipped_unsupported=$skipped completed=$completed error_replies=$error_replies tracebacks=$tracebacks"

fail=0
# The runner exits 0 even when a test fails, so the exit code only catches crashes; the counters below are the gate.
[ "$rc" -eq 0 ] || { echo "FAIL: runner exit code $rc"; fail=1; }
[ "$tracebacks" -eq 0 ] || { echo "FAIL: $tracebacks Python traceback(s) in the runner log"; fail=1; }
case $mode in
  skip)
    [ "$skipped" -eq "$expected" ] || { echo "FAIL: expected all $expected suites skipped as unsupported, got $skipped"; fail=1; }
    [ "$completed" -eq 0 ] || { echo "FAIL: $completed suite(s) ran on a server that lacks the commands"; fail=1; } ;;
  run)
    [ "$skipped" -eq 0 ] || { echo "FAIL: $skipped suite(s) skipped as unsupported on a server that should run them"; fail=1; }
    [ "$completed" -eq "$expected" ] || { echo "FAIL: expected $expected suites to run to completion, got $completed (a suite whose init-command canary or keyspace check failed is not counted)"; grep -E 'ResponseError|Exception|ERR ' "$log" | grep -v 'handle error response' | sort | uniq -c | sort -rn | head -5; fail=1; }
    [ "$error_replies" -eq 0 ] || { echo "FAIL: the server returned $error_replies error replies during the benchmarks"; grep -oE 'handle error response: .{0,80}' "$log" | sort | uniq -c | sort -rn | head -5; fail=1; } ;;
  *) echo "mode must be skip or run"; exit 2 ;;
esac
if [ "$fail" -ne 0 ]; then echo "--- last runner log lines"; tail -30 "$log"; exit 1; fi
echo "OK: $mode mode, $expected suite(s) behaved as expected against $image"
