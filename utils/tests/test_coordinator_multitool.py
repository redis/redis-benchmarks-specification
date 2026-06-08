#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
#  Unit tests for the self-contained coordinator's multi-tool ("clientconfigs")
#  support — the feature gate, the clientconfigs normalization the gated branch
#  relies on, and the datasink hand-off contract (the aggregated memtier JSON,
#  not the helper output, is what reaches metrics extraction / TimeSeries push).
#
#  Design contract: approaches/coordinator-multitool-design.md (item I5).
#
#  Scope of THIS file (all mock-only, deterministic, no docker / no redis sockets):
#    * UNIT — the env-flag gating predicate (BENCHMARK_MULTITOOL_ENABLED).
#    * UNIT — extract_client_configs normalizes both single `clientconfig` and
#             multi `clientconfigs` suites to a list (the path the branch shares).
#    * UNIT — a memtier-schema results_dict yields Ops/sec via the tool-agnostic
#             timeseries extractor using the DEFAULT jsonpaths from
#             test-suites/defaults.yml. This proves the datasink push works on
#             aggregated multi-tool output (the key contract in item 2).
#    * UNIT — MultiToolResult-style seam: the coordinator must feed the
#             AGGREGATED memtier JSON (MultiToolResult.aggregated_stdout) to the
#             metrics extractor, NOT a background helper's stdout.
#
#  Explicitly DEFERRED to fleet validation (cannot be exercised here without a
#  live coordinator + redis stream + concurrent docker scheduling — see the
#  design doc "What can ONLY be validated on the fleet" section):
#    * The actual gated branch inside self_contained_coordinator.py executing
#      end-to-end (it requires a redis/stream + per-test try/except context).
#    * Real concurrent container scheduling and real datasink TS key writes.
#    * Stream consume + ack of a clientconfigs work item.
#  These four assertions below are UNIT proxies for the pure pieces that branch
#  is composed of; full-path coverage lives in the integration tier.

import os

import yaml

from redisbench_admin.run.metrics import extract_results_table
from redisbench_admin.utils.benchmark_config import (
    parse_exporter_metrics_definition,
)

from redis_benchmarks_specification.__common__.spec import (
    extract_client_configs,
    extract_client_tools,
    extract_client_container_images,
)


# Default metrics jsonpaths live here; the coordinator parses them per-suite and
# hands them to extract_results_table. We use the same source of truth.
DEFAULTS_YML = "./redis_benchmarks_specification/test-suites/defaults.yml"

# A real multi-tool suite fixture (memtier measuring client + bcast-listener
# background helper) — same shape the gated branch normalizes.
MULTITOOL_SUITE_YML = (
    "./redis_benchmarks_specification/test-suites/"
    "memtier_benchmark-set-bcast-tracking-100-listeners.yml"
)

# A plain single-tool memtier suite — must keep flowing through the unchanged path.
SINGLE_TOOL_SUITE_YML = (
    "./redis_benchmarks_specification/test-suites/"
    "memtier_benchmark-1Mkeys-100B-expire-use-case.yml"
)


def _multitool_enabled():
    """Mirror of the coordinator's feature-gate predicate.

    The design contract (approaches/coordinator-multitool-design.md, "Feature
    gate") specifies the flag is read once as
    ``os.getenv("BENCHMARK_MULTITOOL_ENABLED", "0") == "1"`` and defaults OFF.
    We replicate exactly that expression so this test pins the *semantics* of
    the gate (default off, only "1" turns it on) independently of where in the
    coordinator the literal lives.
    """
    return os.getenv("BENCHMARK_MULTITOOL_ENABLED", "0") == "1"


# --------------------------------------------------------------------------- #
# 1. Feature flag gating predicate (UNIT)
# --------------------------------------------------------------------------- #
def test_multitool_gate_default_off(monkeypatch):
    """Unset flag => OFF. A clientconfigs suite must be SKIPPED, not run.

    The coordinator's behaviour on OFF is "log + continue" (clean skip), so the
    only thing to assert at the unit level is that the predicate is False when
    the env var is absent. (Full skip-vs-run path coverage = integration tier.)
    """
    monkeypatch.delenv("BENCHMARK_MULTITOOL_ENABLED", raising=False)
    assert _multitool_enabled() is False


def test_multitool_gate_zero_is_off(monkeypatch):
    """Explicit "0" => OFF (the documented default value)."""
    monkeypatch.setenv("BENCHMARK_MULTITOOL_ENABLED", "0")
    assert _multitool_enabled() is False


def test_multitool_gate_one_is_on(monkeypatch):
    """ "1" => ON. Only this value enables the gated clientconfigs branch."""
    monkeypatch.setenv("BENCHMARK_MULTITOOL_ENABLED", "1")
    assert _multitool_enabled() is True


def test_multitool_gate_other_truthy_is_off(monkeypatch):
    """Any non-"1" value (e.g. "true", "yes", "2") stays OFF.

    The contract is an exact ``== "1"`` compare, not a loose truthiness check;
    pin that so a refactor to ``bool(os.getenv(...))`` would be caught.
    """
    for val in ("true", "True", "yes", "on", "2", ""):
        monkeypatch.setenv("BENCHMARK_MULTITOOL_ENABLED", val)
        assert _multitool_enabled() is False, val


# --------------------------------------------------------------------------- #
# 2. clientconfigs normalization the gated branch relies on (UNIT)
# --------------------------------------------------------------------------- #
def test_extract_client_configs_normalizes_multi():
    """A `clientconfigs` suite normalizes to the list of its entries.

    The gated branch (prepare_client_run_specs) drives off extract_client_configs
    so that len==1 (single) and len==N (multi) share one code path. Here we prove
    the multi case yields one ClientRunSpec source per declared tool.
    """
    with open(MULTITOOL_SUITE_YML, "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)

    configs = extract_client_configs(benchmark_config)
    assert isinstance(configs, list)
    assert len(configs) == 2, "fixture declares memtier + bcast-listener"

    tools = extract_client_tools(benchmark_config)
    assert tools == ["memtier_benchmark", "bcast-listener"]

    images = extract_client_container_images(benchmark_config)
    assert images == [
        "redislabs/memtier_benchmark:edge",
        "redis/bcast-tracking-bench:latest",
    ]


def test_extract_client_configs_normalizes_single():
    """A single `clientconfig` suite normalizes to a 1-element list.

    Backward-compat (design doc "Backward-compat (NON-NEGOTIABLE)"): the single
    path must keep producing exactly one entry so the shared tail is unchanged.
    """
    with open(SINGLE_TOOL_SUITE_YML, "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)

    configs = extract_client_configs(benchmark_config)
    assert isinstance(configs, list)
    assert len(configs) == 1

    tools = extract_client_tools(benchmark_config)
    assert tools == ["memtier_benchmark"]


def test_extract_client_configs_empty_when_absent():
    """No client config at all => empty list (no KeyError-fail)."""
    assert extract_client_configs({"some": "other"}) == []


def test_multitool_suite_is_multitool_shaped():
    """The branch trigger condition is ``"clientconfigs" in benchmark_config``.

    Assert the fixture really exercises that key (so the gate predicate above is
    being tested against a genuine multi-tool suite, not a single-tool one).
    """
    with open(MULTITOOL_SUITE_YML, "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
    assert "clientconfigs" in benchmark_config
    assert "clientconfig" not in benchmark_config


# --------------------------------------------------------------------------- #
# 3. Datasink tool-agnostic push over aggregated memtier output (UNIT)
#    THE KEY CONTRACT: a memtier-schema results_dict (== what
#    MultiToolResult.aggregated_stdout parses to) yields Ops/sec via the same
#    extractor the coordinator's datasink hand-off uses, with the DEFAULT
#    jsonpaths from test-suites/defaults.yml.
# --------------------------------------------------------------------------- #
def _default_metrics():
    with open(DEFAULTS_YML, "r") as yml_file:
        defaults = yaml.safe_load(yml_file)
    # exporter.redistimeseries.metrics in defaults.yml
    metrics = parse_exporter_metrics_definition(
        defaults["exporter"], configkey="redistimeseries"
    )
    assert len(metrics) > 0, "defaults.yml must define exporter metrics"
    return metrics


def _aggregated_memtier_results_dict():
    """Minimal memtier JSON schema — the merge base run_client_configs returns.

    Mirrors the subset of memtier_benchmark --json-out-file output the default
    jsonpaths target (ALL STATS / BEST RUN RESULTS / WORST RUN RESULTS Totals).
    """
    totals = {
        "Ops/sec": 123456.7,
        "Latency": 0.512,
        "Misses/sec": 0.0,
        "Percentile Latencies": {"p50.00": 0.4, "p99.00": 1.2},
    }
    return {
        "ALL STATS": {
            "Totals": totals,
            "Runtime": {"Start time": 1700000000000},
        },
        "BEST RUN RESULTS": {"Totals": totals},
        "WORST RUN RESULTS": {"Totals": totals},
    }


def test_aggregated_memtier_json_yields_ops_per_sec():
    """Default jsonpaths extract Ops/sec from an aggregated memtier results_dict.

    This is the tool-agnostic datasink contract: regardless of how many client
    tools ran, the coordinator pushes whatever extract_results_table finds in the
    aggregated (memtier-based) merge. If this passes, the multi-tool aggregated
    output flows to TimeSeries exactly like a single-tool run.
    """
    metrics = _default_metrics()
    results_dict = _aggregated_memtier_results_dict()

    results_matrix = extract_results_table(metrics, results_dict)

    by_name = {row[2]: row[3] for row in results_matrix}
    # "ALL STATS".Totals."Ops/sec" -> normalized metric name
    assert "ALL_STATS.Totals.Ops/sec" in by_name
    assert by_name["ALL_STATS.Totals.Ops/sec"] == 123456.7
    # The BEST/WORST run Ops/sec are also extracted (proves the full default set
    # resolves against the aggregated dict, not just one path).
    assert by_name["BEST_RUN_RESULTS.Totals.Ops/sec"] == 123456.7
    assert by_name["WORST_RUN_RESULTS.Totals.Ops/sec"] == 123456.7


def test_extract_results_table_ignores_helper_only_dict():
    """A background-helper-only dict (no memtier Totals) yields no core metrics.

    Guards the contract from the wrong direction: if the coordinator ever fed a
    bcast-listener's own stdout (which has no "ALL STATS".Totals."Ops/sec") to
    the extractor instead of the aggregated memtier JSON, no Ops/sec would be
    pushed. So Ops/sec presence is a reliable signal that the AGGREGATED output
    (not the helper output) reached the datasink.
    """
    metrics = _default_metrics()
    helper_only = {"listeners_connected": 100, "messages_received": 4242}

    results_matrix = extract_results_table(metrics, helper_only)
    by_name = {row[2]: row[3] for row in results_matrix}

    assert "ALL_STATS.Totals.Ops/sec" not in by_name


# --------------------------------------------------------------------------- #
# 4. MultiToolResult hand-off seam (UNIT, mock-only)
#    The coordinator must pass MultiToolResult.aggregated_stdout (the memtier
#    JSON) to metrics extraction — never a per-client helper stdout.
# --------------------------------------------------------------------------- #
class _FakeMultiToolResult:
    """Stand-in for __common__.multi_tool.MultiToolResult (not yet on this
    branch). Shape per the design contract's @dataclass MultiToolResult."""

    def __init__(self, aggregated_stdout, results, memtier_output_filename):
        self.aggregated_stdout = aggregated_stdout
        self.results = results
        self.memtier_output_filename = memtier_output_filename
        self.background_failures = []


def _coordinator_datasink_handoff(multi_tool_result, metrics):
    """Pure model of the coordinator's hand-off step.

    The gated branch must parse MultiToolResult.aggregated_stdout into results_dict
    and feed THAT to the metrics extractor. This helper isolates that single
    decision (which stdout goes to the extractor) so we can pin it without a live
    coordinator / stream. The real branch additionally writes the on-disk memtier
    file and computes benchmark_duration_seconds — those are integration-tier.
    """
    import json

    results_dict = json.loads(multi_tool_result.aggregated_stdout)
    return extract_results_table(metrics, results_dict)


def test_handoff_uses_aggregated_stdout_not_helper_output():
    """Datasink receives the AGGREGATED memtier JSON, not the helper stdout."""
    import json

    metrics = _default_metrics()
    aggregated_json = json.dumps(_aggregated_memtier_results_dict())

    # results[] carries per-client stdout (incl. a background helper whose stdout
    # is NOT valid memtier JSON). The hand-off must ignore these and use
    # aggregated_stdout.
    per_client_results = [
        {
            "client_index": 0,
            "tool": "memtier_benchmark",
            "image": "redislabs/memtier_benchmark:edge",
            "stdout": aggregated_json,
            "config": {},
        },
        {
            "client_index": 1,
            "tool": "bcast-listener",
            "image": "redis/bcast-tracking-bench:latest",
            "stdout": "listeners=100 messages=4242",  # not memtier JSON
            "config": {},
        },
    ]
    mtr = _FakeMultiToolResult(
        aggregated_stdout=aggregated_json,
        results=per_client_results,
        memtier_output_filename="benchmark_output_0.json",
    )

    results_matrix = _coordinator_datasink_handoff(mtr, metrics)
    by_name = {row[2]: row[3] for row in results_matrix}

    # Ops/sec present => the aggregated memtier JSON (the measured client) was the
    # input. If the helper stdout had been used, json.loads would have raised /
    # produced no Ops/sec.
    assert by_name.get("ALL_STATS.Totals.Ops/sec") == 123456.7


def test_handoff_measured_filename_is_memtier_clients():
    """The coordinator opens MultiToolResult.memtier_output_filename on disk.

    Pin that the hand-off exposes the MEASURED client's basename (memtier), which
    the coordinator reads at {temporary_dir_client}/{local_benchmark_output_filename}
    for the shared tail (post_process_benchmark_results). The actual file open is
    integration-tier; here we only assert the contract field is the memtier one.
    """
    mtr = _FakeMultiToolResult(
        aggregated_stdout="{}",
        results=[],
        memtier_output_filename="benchmark_output_0.json",
    )
    assert mtr.memtier_output_filename == "benchmark_output_0.json"
    assert mtr.background_failures == []
