from redis_benchmarks_specification.__common__.runner import (
    CPU_STATS_SECTION_FILTER,
    collect_redis_metrics,
)


class FakeConn:
    """Minimal stand-in for a redis connection: info(section) -> dict."""

    def __init__(self, sections):
        self.sections = sections

    def info(self, section):
        return self.sections[section]


def test_section_filter_applies_to_integers():
    """Regression test.

    The filter condition used to read

        if collect and type(v) is float or type(v) is int:

    which parses as `(collect and float) or (int)`. Integers therefore bypassed
    the filter completely, and every INFO counter is an integer -- so the filter
    only ever constrained floats.
    """
    conn = FakeConn(
        {
            "stats": {
                "keyspace_hits": 10,
                "instantaneous_ops_per_sec": 99999,
                "total_net_output_bytes": 4096,
            }
        }
    )

    _, _, overall = collect_redis_metrics(
        [conn], ["stats"], {"stats": ["keyspace_hits", "total_net_output_bytes"]}
    )

    assert overall == {
        "stats_keyspace_hits": 10,
        "stats_total_net_output_bytes": 4096,
    }
    # The gauge we excluded must not leak through just because it is an int.
    assert "stats_instantaneous_ops_per_sec" not in overall


def test_no_filter_collects_every_numeric_field():
    conn = FakeConn({"stats": {"a": 1, "b": 2.5, "c": "not-numeric"}})

    _, _, overall = collect_redis_metrics([conn], ["stats"], None)

    assert overall == {"stats_a": 1, "stats_b": 2.5}


def test_cpu_stats_filter_keeps_main_thread_split():
    """The cpu split is what separates in-command cost from io-thread cost."""
    conn = FakeConn(
        {
            "cpu": {
                "used_cpu_sys": 1.5,
                "used_cpu_user": 2.5,
                "used_cpu_sys_main_thread": 0.5,
                "used_cpu_user_main_thread": 1.0,
                "used_cpu_sys_children": 0.0,
                "used_cpu_user_children": 0.0,
            },
            "stats": {
                "total_writes_processed": 1234,
                "instantaneous_input_kbps": 7.7,
            },
        }
    )

    _, _, overall = collect_redis_metrics(
        [conn], ["cpu", "stats"], CPU_STATS_SECTION_FILTER
    )

    assert overall["cpu_used_cpu_sys_main_thread"] == 0.5
    assert overall["cpu_used_cpu_user_main_thread"] == 1.0
    assert overall["stats_total_writes_processed"] == 1234
    assert "stats_instantaneous_input_kbps" not in overall


def test_multi_shard_sums_scalar_fields():
    """Two shards: scalar counters add up, as they do for the other sections."""
    conns = [
        FakeConn({"stats": {"keyspace_hits": 10}}),
        FakeConn({"stats": {"keyspace_hits": 5}}),
    ]

    _, _, overall = collect_redis_metrics(
        conns, ["stats"], {"stats": ["keyspace_hits"]}
    )

    assert overall == {"stats_keyspace_hits": 15}


def test_integers_only_drops_float_gauges_but_keeps_integers():
    """The memory export historically carried every integer field and no float
    (ratios and percentages were dropped by the old, broken filter); integers_only
    preserves exactly that set when the filter itself is removed."""
    conn = FakeConn(
        {
            "memory": {
                "used_memory": 1024,
                "used_memory_rss": 2048,
                "mem_fragmentation_ratio": 1.37,
                "used_memory_peak_perc": 88.5,
                "allocator_frag_ratio": 1.1,
            }
        }
    )

    _, _, overall = collect_redis_metrics([conn], ["memory"], None, integers_only=True)

    assert overall == {"memory_used_memory": 1024, "memory_used_memory_rss": 2048}


def test_integers_only_leaves_dict_valued_fields_alone():
    """integers_only constrains top-level scalars only; commandstats-style dicts
    keep their inner float fields (usec_per_call)."""
    conn = FakeConn(
        {
            "commandstats": {
                "cmdstat_get": {"calls": 10, "usec_per_call": 1.5},
                "total": 7,
                "ratio": 0.5,
            }
        }
    )

    _, _, overall = collect_redis_metrics(
        [conn], ["commandstats"], None, integers_only=True
    )

    assert overall == {
        "commandstats_cmdstat_get_calls": 10,
        "commandstats_cmdstat_get_usec_per_call": 1.5,
        "commandstats_total": 7,
    }


def _run_exporter(**flags):
    """Call exporter_datasink_common with every collaborator mocked; return the
    list of INFO section sets that were collected."""
    from unittest.mock import MagicMock, patch

    from redis_benchmarks_specification.__common__ import runner

    collected = []

    def fake_collect(conns, sections, *args, **kwargs):
        collected.append(list(sections))
        return [], {}, {}

    with patch.object(runner, "timeseries_test_sucess_flow"), patch.object(
        runner, "collect_redis_metrics", side_effect=fake_collect
    ), patch.object(runner, "export_redis_metrics"):
        runner.exporter_datasink_common(
            {},
            0,
            "variant",
            1,
            0,
            MagicMock(),
            True,
            "branch",
            "version",
            {},
            [MagicMock()],
            {},
            "platform",
            "setup",
            "type",
            "test",
            "org",
            "repo",
            "env",
            "topology",
            **flags,
        )
    return collected


def test_exporter_collects_cpu_stats_by_default():
    assert ["cpu", "stats"] in _run_exporter()


def test_memory_only_export_skips_cpu_stats():
    """The memory-comparison (load-only) export must not write cpu-stats series
    under the same test_name as the real benchmark run."""
    collected = _run_exporter(
        collect_commandstats=False,
        collect_memory_metrics=True,
        collect_cpu_stats=False,
    )
    assert collected == [["memory"]]
