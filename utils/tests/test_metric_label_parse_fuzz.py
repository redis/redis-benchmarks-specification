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

# hypothesis is a hard import, not pytest.importorskip: it is pinned in
# dev_requirements.txt, which is what tox installs, so a missing dependency is a
# packaging bug. A skip would hide that and let CI report green while this file
# never runs -- the same silent-success failure mode this fix is about.
from hypothesis import given, settings
from hypothesis import strategies as st

from redis_benchmarks_specification.__common__.runner import (
    commandstats_latencystats_process_name,
)

COMMANDSTATS_PREFIX = "commandstats_cmdstat_"
LATENCYSTATS_PREFIX = "latencystats_latency_percentiles_usec_"
PREFIXES = st.sampled_from([COMMANDSTATS_PREFIX, LATENCYSTATS_PREFIX])

LABELS = (
    "command",
    "metric",
    "shard",
    "command_and_metric",
    "command_and_metric_and_setup",
    "command_and_setup",
    "metric_and_shard",
)

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
    """`<prefix><command>_<metric>` must yield exactly that command and metric.

    The command alphabet excludes an interior "_" on purpose: `a_b_c` is genuinely ambiguous
    (command `a_b` + metric `c` vs command `a` + metric `b_c`) and the wire format carries
    nothing to disambiguate it. A leading underscore is unambiguous and is covered by
    test_leading_underscore_command_keeps_its_underscore.
    """
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


@settings(max_examples=200, deadline=None)
@given(
    prefix=PREFIXES,
    command=st.text(alphabet="abcdefghijklmnopqrstuvwxyz.", min_size=1, max_size=8),
    metric=st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=1, max_size=8),
)
def test_leading_underscore_command_keeps_its_underscore(prefix, command, metric):
    """A command named `_foo` must parse as `_foo`, not lose the underscore or be dropped.

    The original implementation split at the first "_" *after* the leading character
    specifically so that a leading underscore survived. A naive `partition("_")` drops such
    commands entirely, which is a silent reclassification -- so this pins the behaviour.
    """
    labels = {}
    commandstats_latencystats_process_name(
        f"{prefix}_{command}_{metric}", prefix, "oss-standalone", labels
    )
    assert labels["command"] == "_" + command
    assert labels["metric"] == metric


@settings(max_examples=300, deadline=None)
@given(prefix=PREFIXES, tail=NAME_PARTS)
def test_no_label_is_ever_empty(prefix, tail):
    """No derived label may be the empty string.

    An empty-valued label is a real tag on a real datapoint, so it misfiles the point just
    as surely as a wrong one -- the failure this whole fix is about. Shapes like
    "get__shard_2" (empty metric) and "get_calls_shard_" (empty shard) reached this.
    """
    labels = {}
    commandstats_latencystats_process_name(
        prefix + tail, prefix, "oss-standalone", labels
    )
    for key in LABELS:
        if key in labels:
            assert labels[key] != "", f"{key} is empty for tail {tail!r}"


def test_stale_labels_are_not_left_behind_on_a_reused_dict():
    """A name this function cannot parse must not leave the previous metric's labels behind.

    `export_redis_metrics` binds one `variant_labels_dict` per variant and reuses it for every
    metric, setting `variant_labels_dict["metric"] = metric_name` immediately before each call.
    So `metric` is the caller's key -- this function does not own it and must not clear it --
    but every label it *derives* has to go, or the metric is exported under the previous
    command's tags. This test mimics that caller exactly.
    """
    derived = tuple(k for k in LABELS if k != "metric")
    labels = {}

    labels["metric"] = COMMANDSTATS_PREFIX + "get_calls"
    commandstats_latencystats_process_name(
        COMMANDSTATS_PREFIX + "get_calls", COMMANDSTATS_PREFIX, "oss-standalone", labels
    )
    assert labels["command"] == "get"

    # next metric of the same variant, prefixed but unparseable
    labels["metric"] = COMMANDSTATS_PREFIX + "g"
    commandstats_latencystats_process_name(
        COMMANDSTATS_PREFIX + "g", COMMANDSTATS_PREFIX, "oss-standalone", labels
    )
    for key in derived:
        assert (
            key not in labels
        ), f"stale {key}={labels.get(key)!r} survived an unparseable name"
    # the caller's own key is untouched
    assert labels["metric"] == COMMANDSTATS_PREFIX + "g"


CORE_UNDERSCORE_COMMANDS = (
    "sort_ro",
    "eval_ro",
    "evalsha_ro",
    "fcall_ro",
    "bitfield_ro",
    "georadius_ro",
    "georadiusbymember_ro",
)


@settings(max_examples=200, deadline=None)
@given(
    command=st.sampled_from(CORE_UNDERSCORE_COMMANDS),
    metric=st.sampled_from(
        ("calls", "usec", "usec_per_call", "rejected_calls", "failed_calls")
    ),
    shard=st.one_of(st.none(), st.integers(1, 9).map(str)),
)
def test_commands_whose_name_contains_underscore_are_not_folded_into_another_command(
    command, metric, shard
):
    """A command whose own name contains "_" must not be attributed to a shorter command.

    Seven core commands end in "_RO". Splitting on the first "_" filed SORT_RO's metrics
    under command="sort" with metric="ro_calls", silently merging them into the real SORT
    series -- the same misfiling this fix is about, but reachable with real command names.
    """
    name = f"{COMMANDSTATS_PREFIX}{command}_{metric}"
    if shard is not None:
        name += f"_shard_{shard}"
    labels = {}
    commandstats_latencystats_process_name(
        name, COMMANDSTATS_PREFIX, "oss-standalone", labels
    )
    assert labels["command"] == command
    assert labels["metric"] == metric
    assert labels["shard"] == (shard or "1")


@settings(max_examples=200, deadline=None)
@given(
    command=st.sampled_from(CORE_UNDERSCORE_COMMANDS + ("get", "set", "client|list")),
    pct=st.sampled_from(("p50", "p99", "p99.9", "p50.00", "p99.99")),
)
def test_latency_percentiles_split_on_the_percentile_not_the_first_underscore(
    command, pct
):
    """Percentile metrics must split on the percentile, so "_RO" stays with the command."""
    labels = {}
    commandstats_latencystats_process_name(
        f"{LATENCYSTATS_PREFIX}{command}_{pct}",
        LATENCYSTATS_PREFIX,
        "oss-standalone",
        labels,
    )
    assert labels["command"] == command
    assert labels["metric"] == pct


@settings(max_examples=200, deadline=None)
@given(
    prefix=PREFIXES,
    command=st.sampled_from(("get", "sort_ro", "client|list")),
    metric=st.sampled_from(("calls", "usec_per_call")),
    setup_name=st.text(
        alphabet="abcdefghijklmnopqrstuvwxyz-0123456789", min_size=1, max_size=20
    ),
)
def test_setup_name_reaches_the_labels_that_carry_it(
    prefix, command, metric, setup_name
):
    """`setup_name` is a parameter whose only purpose is these two labels.

    Two of the seven labels exist solely to carry the topology it names, so ignoring the
    argument would leave every datapoint tagged with the wrong topology while all the
    command/metric assertions still passed.
    """
    labels = {}
    commandstats_latencystats_process_name(
        f"{prefix}{command}_{metric}", prefix, setup_name, labels
    )
    assert labels["command_and_setup"] == f"{command} - {setup_name}"
    assert (
        labels["command_and_metric_and_setup"] == f"{command} - {metric} - {setup_name}"
    )


@settings(max_examples=200, deadline=None)
@given(
    prefix=PREFIXES,
    command=st.sampled_from(("get", "sort_ro")),
    metric=st.sampled_from(("calls", "usec")),
)
def test_prefix_matching_is_case_sensitive(prefix, command, metric):
    """Prefix matching must stay case-sensitive.

    Redis emits these section names in lower case, so an upper-cased name is not one of
    ours and must derive nothing. Every generator alphabet here is lower case, so without
    this assertion a case-insensitive match would go unnoticed.
    """
    labels = {}
    commandstats_latencystats_process_name(
        f"{prefix.upper()}{command}_{metric}", prefix, "oss-standalone", labels
    )
    assert labels == {}, f"derived {labels} from an upper-cased prefix"
