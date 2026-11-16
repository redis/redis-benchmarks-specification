"""Opt-in server counter deltas around the measured client phase.

These are phase costs (including connection setup), not commandstats execution
latencies. Keeping both snapshots avoids attributing preload CPU to GETs.
"""

import math


def validate_client_memory_runner(config):
    options = config.get("client-memory-metrics")
    if options is None:
        return
    if not isinstance(options, dict):
        raise ValueError("client-memory-metrics must be a mapping")
    if options.get("coordinator-only", False):
        raise ValueError(
            "This client-memory workload requires a coordinator-managed fresh server"
        )


def snapshot_client_memory_metrics(config, redis_conns):
    options = config.get("client-memory-metrics")
    if options is None:
        return None
    if not isinstance(options, dict) or len(redis_conns) != 1:
        raise ValueError(
            "client-memory-metrics requires options and one standalone server"
        )
    for field in ("min-evicted-clients", "max-evicted-clients", "maxmemory-clients"):
        value = options.get(field)
        if value is not None and (type(value) is not int or value < 0):
            raise ValueError(f"{field} must be a nonnegative integer")
    if options.get("min-evicted-clients", 0) > options.get(
        "max-evicted-clients", math.inf
    ):
        raise ValueError("Invalid client eviction bounds")
    try:
        expected_limit = options.get("maxmemory-clients")
        if expected_limit is not None:
            actual_limit = redis_conns[0].config_get("maxmemory-clients")[
                "maxmemory-clients"
            ]
            if int(actual_limit) != expected_limit:
                raise ValueError("Server maxmemory-clients does not match the workload")
        info = redis_conns[0].info("all")
        cpu = info["used_cpu_user_main_thread"]
        calls = info.get("cmdstat_get", {}).get("calls", 0)
        evicted = info["evicted_clients"]
        run_id = info["run_id"]
    except Exception as exc:
        raise RuntimeError("Cannot snapshot client-memory metrics") from exc
    if not run_id or not all(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
        for value in (cpu, calls, evicted)
    ):
        raise ValueError("Invalid client-memory counter snapshot")
    return {"run_id": run_id, "cpu": cpu, "calls": calls, "evicted": evicted}


def inject_client_memory_metrics(config, results, before, after):
    options = config.get("client-memory-metrics")
    if options is None:
        return
    if before is None or after is None or before["run_id"] != after["run_id"]:
        raise ValueError("Missing client-memory snapshot or Redis restarted during run")
    delta = {key: after[key] - before[key] for key in ("cpu", "calls", "evicted")}
    if any(value < 0 for value in delta.values()) or delta["calls"] == 0:
        raise ValueError("Client-memory counters reset or no GET commands completed")
    minimum = options.get("min-evicted-clients", 0)
    maximum = options.get("max-evicted-clients")
    if delta["evicted"] < minimum or (
        maximum is not None and delta["evicted"] > maximum
    ):
        raise ValueError("Client eviction count is outside the required range")
    totals = results["ALL STATS"]["Totals"]
    totals.update(
        {
            "ClientPhaseUserCpuSeconds": delta["cpu"],
            "ClientPhaseGetCalls": delta["calls"],
            "ClientPhaseUserCpuSecondsPerGet": delta["cpu"] / delta["calls"],
            "ClientPhaseEvictedClients": delta["evicted"],
        }
    )
