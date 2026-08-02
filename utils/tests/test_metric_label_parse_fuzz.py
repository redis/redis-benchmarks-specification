"""Property tests for commandstats/latencystats metric-name label derivation.

`commandstats_latencystats_process_name` runs on the results-export path: it turns a
server-side metric name such as ``commandstats_cmdstat_get_calls`` into the `command`,
`metric` and `shard` labels a datapoint is stored under. Two properties matter and neither
was covered:

* it must never raise — an exception here loses an entire benchmark's metrics;
* it must never invent labels from a name it does not actually describe, because a wrong
  label is worse than a missing one (the datapoint is still written, just misfiled).

Both properties failed before the accompanying fix: three inputs raised `IndexError`, and a
name that merely *contained* the prefix at a non-zero offset was sliced at the wrong offset
and produced confident nonsense (``shard_commandstats_cmdstat_get_calls`` →
``command="dstat"``).

Offline and dependency-light so it runs on every pull request.
"""

import pytest

hypothesis = pytest.importorskip(
    "hypothesis", reason="hypothesis is required for property tests"
)
from hypothesis import given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from redis_benchmarks_specification.__common__.runner import (  # noqa: E402
    commandstats_latencystats_process_name,
)

COMMANDSTATS_PREFIX = "commandstats_cmdstat_"
LATENCYSTATS_PREFIX = "latencystats_latency_percentiles_usec_"
PREFIXES = st.sampled_from([COMMANDSTATS_PREFIX, LATENCYSTATS_PREFIX])

LABELS = ("command", "metric", "shard", "command_and_metric")

# Deliberately hostile: empty strings, bare separators, and the prefix embedded at a
# non-zero offset -- the shapes that used to raise or mis-slice.
NAME_PARTS = st.one_of(
    st.text(
        alphabet="abcdefghijklmnopqrstuvwxyz|-.0123456789_", min_size=0, max_size=12
    ),
    st.sampled_from(["", "_", "__", "get", "client|list", "g", "_shard_", "shard_"]),
)


@settings(max_examples=500, deadline=None)
@given(prefix=PREFIXES, tail=NAME_PARTS)
def test_never_raises_on_any_tail(prefix, tail):
    """Whatever follows the prefix, the parser must not raise."""
    commandstats_latencystats_process_name(prefix + tail, prefix, "oss-standalone", {})


@settings(max_examples=500, deadline=None)
@given(prefix=PREFIXES, lead=NAME_PARTS, tail=NAME_PARTS)
def test_never_raises_on_arbitrary_names(prefix, lead, tail):
    """A name that only *contains* the prefix must not raise either."""
    commandstats_latencystats_process_name(lead + prefix + tail, prefix, "oss", {})


@settings(max_examples=500, deadline=None)
@given(prefix=PREFIXES, lead=NAME_PARTS.filter(bool), tail=NAME_PARTS)
def test_no_labels_derived_when_the_prefix_is_not_at_the_start(prefix, lead, tail):
    """A prefix at a non-zero offset must derive nothing.

    The slice is ``metric_name[len(prefix):]``, which is only meaningful when the prefix is
    at offset 0. Deriving labels from a mid-string match silently misfiles the datapoint.
    """
    name = lead + prefix + tail
    if name.startswith(prefix):  # `lead` itself ended up prefix-shaped; not this case
        return
    labels = {}
    commandstats_latencystats_process_name(name, prefix, "oss-standalone", labels)
    assert labels == {}, f"derived {labels} from a name whose prefix is not at offset 0"


@settings(max_examples=300, deadline=None)
@given(
    prefix=PREFIXES,
    command=st.text(alphabet="abcdefghijklmnopqrstuvwxyz|", min_size=1, max_size=10),
    metric=st.text(
        alphabet="abcdefghijklmnopqrstuvwxyz.0123456789", min_size=1, max_size=10
    ),
)
def test_wellformed_names_round_trip(prefix, command, metric):
    """`<prefix><command>_<metric>` must yield exactly that command and metric."""
    labels = {}
    commandstats_latencystats_process_name(
        f"{prefix}{command}_{metric}", prefix, "oss-standalone", labels
    )
    assert labels["command"] == command
    assert labels["metric"] == metric
    assert labels["shard"] == "1"
    assert labels["command_and_metric"] == f"{command} - {metric}"


@settings(max_examples=300, deadline=None)
@given(
    prefix=PREFIXES,
    command=st.text(alphabet="abcdefghijklmnopqrstuvwxyz|", min_size=1, max_size=8),
    metric=st.text(
        alphabet="abcdefghijklmnopqrstuvwxyz.0123456789", min_size=1, max_size=8
    ),
    shard=st.integers(min_value=0, max_value=999).map(str),
)
def test_shard_suffix_is_split_out(prefix, command, metric, shard):
    """`_shard_<n>` must land in the shard label, not in the metric."""
    labels = {}
    commandstats_latencystats_process_name(
        f"{prefix}{command}_{metric}_shard_{shard}", prefix, "oss-standalone", labels
    )
    assert labels["command"] == command
    assert labels["metric"] == metric
    assert labels["shard"] == shard


@settings(max_examples=200, deadline=None)
@given(prefix=PREFIXES)
def test_partial_names_derive_nothing_rather_than_raising(prefix):
    """The three shapes that used to raise IndexError must now derive nothing."""
    for tail in ("", "g", "getcalls", "_"):
        labels = {}
        commandstats_latencystats_process_name(
            prefix + tail, prefix, "oss-standalone", labels
        )
        assert (
            labels == {}
        ), f"derived {labels} from an incomplete name {prefix + tail!r}"


def test_all_label_keys_are_set_together_or_not_at_all():
    """Labels must be all-or-nothing, so a consumer never sees a half-filled dict."""
    for name, expect_labels in [
        (COMMANDSTATS_PREFIX + "get_calls", True),
        (COMMANDSTATS_PREFIX + "g", False),
        (COMMANDSTATS_PREFIX, False),
        ("shard_" + COMMANDSTATS_PREFIX + "get_calls", False),
    ]:
        labels = {}
        commandstats_latencystats_process_name(
            name, COMMANDSTATS_PREFIX, "oss-standalone", labels
        )
        present = [k for k in LABELS if k in labels]
        assert (
            len(present) == len(LABELS)
        ) is expect_labels, f"{name!r} produced a partial label set: {labels}"
