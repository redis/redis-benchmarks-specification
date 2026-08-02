"""Property tests for baseline/comparison selector resolution in the compare tool.

`get_by_strings` picks the single thing each side of a comparison is identified by. Two
defects made it unreliable, and both were silent:

* the error list was appended to only inside each selector's own failure branch, so whichever
  selector was supplied first went unrecorded -- any pair not involving `--*-branch` reported
  "a total of 1" and named the wrong flag;
* the `--comparison-hash` exclusion check was commented out, so four comparison pairs were
  accepted with no error at all and the hash quietly won by being last in the chain. A wrong
  selector here silently compares the wrong two things, which is worse than refusing to run.

Offline; no Redis, no Docker.
"""

import itertools
import logging
import re

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from redis_benchmarks_specification.__compare__.compare import get_by_strings

SUFFIXES = ("branch", "tag", "target_version", "target_branch", "hash")
SIDES = ("baseline", "comparison")
ALL_KWARGS = tuple(f"{side}_{suffix}" for side in SIDES for suffix in SUFFIXES)
# label reported for each suffix, per the flag names in the error text
LABELS = {
    "branch": "branch",
    "tag": "version",
    "target_version": "target+version",
    "target_branch": "target+branch",
    "hash": "hash",
}


def _call(caplog, **kwargs):
    """Invoke get_by_strings with every selector defaulted to None, returning logged text."""
    payload = {name: None for name in ALL_KWARGS}
    payload.update(kwargs)
    # each side needs one selector or it exits for being empty, which is a different test
    if not any(k.startswith("baseline") and payload[k] for k in ALL_KWARGS):
        payload["baseline_branch"] = "BASE"
    if not any(k.startswith("comparison") and payload[k] for k in ALL_KWARGS):
        payload["comparison_branch"] = "CMP"
    with caplog.at_level(logging.ERROR):
        try:
            return get_by_strings(**payload), caplog.text
        except SystemExit:
            return None, caplog.text


@pytest.mark.parametrize("side", SIDES)
@pytest.mark.parametrize("pair", list(itertools.combinations(SUFFIXES, 2)))
def test_every_pair_of_selectors_is_rejected_with_an_accurate_count(caplog, side, pair):
    """Two selectors on one side must exit, naming both, with a count of 2.

    Parametrized over all 10 pairs x both sides: 6 of 10 baseline pairs previously reported
    "a total of 1", and 4 of 10 comparison pairs were not rejected at all.
    """
    a, b = pair
    result, text = _call(caplog, **{f"{side}_{a}": "AAA", f"{side}_{b}": "BBB"})
    assert result is None, f"{side} {a}+{b} was accepted instead of rejected"
    match = re.search(r"total of (\d+): ([^.]*)", text)
    assert match, f"no count in error for {side} {a}+{b}: {text!r}"
    assert int(match.group(1)) == 2, f"{side} {a}+{b} reported {match.group(1)}, not 2"
    named = [n.strip() for n in match.group(2).split(",")]
    assert sorted(named) == sorted(
        [LABELS[a], LABELS[b]]
    ), f"{side} {a}+{b} named {named}"


@pytest.mark.parametrize("side", SIDES)
@pytest.mark.parametrize("suffix", SUFFIXES)
def test_a_single_selector_is_accepted_and_reported_with_its_own_label(
    caplog, side, suffix
):
    """Exactly one selector must be accepted, and its value and label returned."""
    result, _ = _call(caplog, **{f"{side}_{suffix}": "PICKED"})
    assert result is not None, f"{side} {suffix} alone was rejected"
    baseline_str, by_baseline, comparison_str, by_comparison = result
    value, label = (
        (baseline_str, by_baseline)
        if side == "baseline"
        else (comparison_str, by_comparison)
    )
    assert value == "PICKED"
    assert label == LABELS[suffix]


@pytest.mark.parametrize("side", SIDES)
def test_no_selector_on_a_side_is_rejected(caplog, side):
    """A side with nothing supplied must exit rather than compare against nothing."""
    payload = {name: None for name in ALL_KWARGS}
    other = "comparison" if side == "baseline" else "baseline"
    payload[f"{other}_branch"] = "OTHER"
    with caplog.at_level(logging.ERROR):
        with pytest.raises(SystemExit):
            get_by_strings(**payload)
    assert "You need to provide one of" in caplog.text


@settings(max_examples=300, deadline=None)
@given(
    chosen=st.lists(st.sampled_from(SUFFIXES), min_size=1, max_size=5, unique=True),
    side=st.sampled_from(SIDES),
)
def test_any_number_of_selectors_above_one_is_rejected_with_a_matching_count(
    chosen, side
):
    """For any subset of selectors, accept exactly when the subset has size 1.

    Generated rather than enumerated because the count must equal the number supplied for
    every subset size, not only for pairs.
    """
    payload = {name: None for name in ALL_KWARGS}
    other = "comparison" if side == "baseline" else "baseline"
    payload[f"{other}_branch"] = "OTHER"
    for suffix in chosen:
        payload[f"{side}_{suffix}"] = "V"
    records = []
    handler = logging.Handler()
    handler.emit = lambda record: records.append(record.getMessage())
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        if len(chosen) == 1:
            assert get_by_strings(**payload) is not None
        else:
            with pytest.raises(SystemExit):
                get_by_strings(**payload)
            text = "\n".join(records)
            match = re.search(r"total of (\d+):", text)
            assert match and int(match.group(1)) == len(
                chosen
            ), f"{len(chosen)} selectors reported as {match.group(1) if match else None}"
    finally:
        root.removeHandler(handler)
