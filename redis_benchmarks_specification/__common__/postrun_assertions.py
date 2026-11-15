"""Opt-in server assertions that must pass before benchmark results are exported."""

import math

from jsonpath_ng import parse


def _milliseconds(value):
    if (
        type(value) not in (int, float)
        or value <= 0
        or value > 2**53 - 1
        or not math.isfinite(value)
        or int(value) != value
    ):
        raise ValueError("postrun_lua requires positive integer runtime milliseconds")
    return int(value)


def validate_postrun_lua(benchmark_config, redis_conns, results_dict):
    """Pass the measured start/end milliseconds as ARGV[1]/ARGV[2] to Lua.

    Only a single memtier client and standalone server are supported: selecting a
    helper client's merged JSON or checking a lagging replica would validate the
    wrong measurement. Scripts must return integer 1, or raise an assertion error.
    """
    scripts = benchmark_config.get("dbconfig", {}).get("postrun_lua")
    if scripts is None:
        return
    if isinstance(scripts, str):
        scripts = [scripts]
    if (
        not isinstance(scripts, list)
        or not scripts
        or any(not isinstance(script, str) or not script.strip() for script in scripts)
    ):
        raise ValueError("postrun_lua must be a nonempty script or list of scripts")
    if (
        "clientconfigs" in benchmark_config
        or benchmark_config.get("clientconfig", {}).get("tool") != "memtier_benchmark"
        or benchmark_config.get("redis-topologies") != ["oss-standalone"]
        or len(redis_conns) != 1
    ):
        raise ValueError(
            "postrun_lua requires one standalone server and memtier client"
        )

    try:
        stats = results_dict["ALL STATS"]
        runtime = stats["Runtime"]
        if runtime["Time unit"] != "MILLISECONDS":
            raise ValueError("postrun_lua requires runtime in MILLISECONDS")
        start = _milliseconds(runtime["Start time"])
        finish = _milliseconds(runtime["Finish time"])
        if finish <= start:
            raise ValueError("postrun_lua requires an increasing measured runtime")
        if _milliseconds(runtime["Total duration"]) != finish - start:
            raise ValueError("postrun_lua requires consistent runtime boundaries")
        if runtime.get("Interrupted", "false") not in (False, "false"):
            raise ValueError("postrun_lua cannot validate an interrupted run")
        if runtime.get("Unanswered requests", 0) != 0:
            raise ValueError("postrun_lua cannot validate unanswered requests")
        totals = stats["Totals"]
        for key in ("Errors", "Errors/sec", "Connection Errors"):
            if totals.get(key, 0) != 0:
                raise ValueError("postrun_lua cannot validate a run with client errors")
    except (KeyError, TypeError) as exc:
        raise ValueError(
            "postrun_lua requires complete memtier ALL STATS runtime"
        ) from exc

    metrics = (
        benchmark_config.get("exporter", {})
        .get("redistimeseries", {})
        .get("metrics", [])
    )
    for metric in metrics:
        path = next(iter(metric)) if isinstance(metric, dict) else metric
        matches = parse(path).find(results_dict)
        if len(matches) != 1 or type(matches[0].value) not in (int, float):
            raise ValueError(f"postrun_lua requires the declared numeric metric {path}")
        if not math.isfinite(matches[0].value):
            raise ValueError(f"postrun_lua requires a finite metric {path}")

    for script in scripts:
        try:
            result = redis_conns[0].execute_command("EVAL", script, 0, start, finish)
        except Exception as exc:
            # The coordinator tolerates ConnectionError during metric collection.
            # Assertions are mandatory, so an unavailable server must fail the run.
            raise ValueError("postrun_lua assertion could not be completed") from exc
        if type(result) is not int or result != 1:
            raise ValueError("postrun_lua assertion must return integer 1")
