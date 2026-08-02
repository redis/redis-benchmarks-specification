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
# datasink label each suffix resolves to -- returned as by_str and used as a TSDB filter key
LABELS = {
    "branch": "branch",
    "tag": "version",
    "target_version": "target+version",
    "target_branch": "target+branch",
    "hash": "hash",
}


def _flag(side, suffix):
    """The CLI flag for a selector -- what the error names, as opposed to its datasink label."""
    return "--{}-{}".format(side, suffix.replace("_", "-"))


def _call(caplog, **kwargs):
    """Invoke get_by_strings with every selector defaulted to None.

    Returns (result, message). A rejected call yields (None, the ValueError's message): the
    resolver raises rather than exiting, so that the coordinator daemon -- which calls into this
    module inside an `except Exception` it documents as best-effort -- can log and continue. A
    SystemExit would not be caught by that guard.
    """
    payload = {name: None for name in ALL_KWARGS}
    payload.update(kwargs)
    # Each side needs one selector or it exits for being empty, which is a different test.
    # Gated on `is not None`, not truthiness: "" is a value a caller really passes (to clear
    # the defaulted --baseline-branch), so a truthiness gate would silently overwrite it and
    # make the empty-string semantics untestable.
    if all(payload[k] is None for k in ALL_KWARGS if k.startswith("baseline")):
        payload["baseline_branch"] = "BASE"
    if all(payload[k] is None for k in ALL_KWARGS if k.startswith("comparison")):
        payload["comparison_branch"] = "CMP"
    try:
        return get_by_strings(**payload), ""
    except ValueError as exc:
        return None, str(exc)


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
        [_flag(side, a), _flag(side, b)]
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
    with pytest.raises(ValueError) as excinfo:
        get_by_strings(**payload)
    assert "You need to provide one of" in str(excinfo.value)


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
    if len(chosen) == 1:
        assert get_by_strings(**payload) is not None
        return
    with pytest.raises(ValueError) as excinfo:
        get_by_strings(**payload)
    match = re.search(r"total of (\d+):", str(excinfo.value))
    assert match and int(match.group(1)) == len(
        chosen
    ), f"{len(chosen)} selectors reported as {match.group(1) if match else None}"


@pytest.mark.parametrize("side", SIDES)
def test_an_empty_branch_alongside_a_hash_is_accepted(caplog, side):
    """`--<side>-branch '' --<side>-hash X` must resolve to the hash, not error.

    compare_command_logic defaults --baseline-branch to "unstable", so selecting by hash means
    clearing it with ''. The documented fleet invocations pass that form on both sides, and only
    the baseline was normalized to None before reaching here -- so enforcing the exclusion on a
    bare `is not None` rejected the comparison form that every caller uses.
    """
    other = "comparison" if side == "baseline" else "baseline"
    result, text = _call(
        caplog,
        **{f"{side}_branch": "", f"{side}_hash": "H" * 40, f"{other}_branch": "OTHER"},
    )
    assert result is not None, f"rejected the documented idiom: {text}"
    by_str = result[1] if side == "baseline" else result[3]
    value = result[0] if side == "baseline" else result[2]
    assert (value, by_str) == ("H" * 40, "hash")


@pytest.mark.parametrize("side", SIDES)
def test_every_selector_empty_on_one_side_is_reported_as_empty(caplog, side):
    """All five selectors passed as "" is indistinguishable from passing none of them.

    Otherwise "" would count toward the exclusion and a caller clearing two defaults would be
    told they selected two things.
    """
    payload = {f"{side}_{suffix}": "" for suffix in SUFFIXES}
    other = "comparison" if side == "baseline" else "baseline"
    payload[f"{other}_branch"] = "OTHER"
    result, text = _call(caplog, **payload)
    assert result is None
    assert "You need to provide one of" in text
    assert "mutually exclusive" not in text


@pytest.mark.parametrize("side", SIDES)
def test_an_empty_string_does_not_count_toward_the_exclusion(caplog, side):
    """ "" plus two real selectors reports 2, not 3."""
    other = "comparison" if side == "baseline" else "baseline"
    result, text = _call(
        caplog,
        **{
            f"{side}_branch": "",
            f"{side}_tag": "T",
            f"{side}_hash": "H" * 40,
            f"{other}_branch": "OTHER",
        },
    )
    assert result is None
    expected = "a total of 2: {},{}".format(_flag(side, "tag"), _flag(side, "hash"))
    assert expected in text, text


def test_env_comparison_table_binds_the_hashes_to_the_sides_its_caller_intends():
    """The positional hash arguments must not be transposed.

    compute_env_comparison_table is called with every argument positional. Its signature
    declared comparison_hash before baseline_hash while the caller passes baseline first, so
    each hash bound to the opposite parameter and was forwarded to get_by_strings under the
    swapped name -- transposing the two sides of any --compare-by-env comparison filtered by
    hash. Asserted on the signature because that is where the contract lives; the call site is
    a 40-argument positional list that cannot be exercised without a live datasink.
    """
    import inspect

    from redis_benchmarks_specification.__compare__.compare import (
        compute_env_comparison_table,
    )

    names = list(inspect.signature(compute_env_comparison_table).parameters)
    assert names.index("baseline_hash") < names.index("comparison_hash")
    # the pair the caller supplies immediately before them, to catch a drift in either list
    assert names.index("comparison_target_version") + 1 == names.index("baseline_hash")


def test_every_selector_names_a_flag_the_parser_actually_accepts():
    """_SELECTORS drives the error text, so a selector with no flag advertises a lie.

    Nothing else links the two lists: adding a selector here would name a flag argparse does not
    define, sending the user to a nonexistent option.
    """
    import argparse

    from redis_benchmarks_specification.__compare__.args import create_compare_arguments
    from redis_benchmarks_specification.__compare__.compare import _selector_flags

    parser = create_compare_arguments(argparse.ArgumentParser())
    known = {opt for action in parser._actions for opt in action.option_strings}
    for side in SIDES:
        missing = [f for f in _selector_flags(side) if f not in known]
        assert not missing, f"{side}: error text names undefined flags {missing}"


def test_a_selector_missing_from_the_call_site_is_a_hard_error():
    """The resolver indexes `supplied` rather than using .get().

    With .get(), a selector listed in _SELECTORS but absent from a call-site dict resolves to
    None forever -- unsupplyable, while the error message still offers its flag.
    """
    from redis_benchmarks_specification.__compare__.compare import (
        _SELECTORS,
        _resolve_by,
    )

    complete = {suffix: None for suffix, _ in _SELECTORS}
    complete["branch"] = "BASE"
    for suffix, _ in _SELECTORS:
        partial = {k: v for k, v in complete.items() if k != suffix}
        with pytest.raises(KeyError):
            _resolve_by("baseline", partial)


@pytest.mark.parametrize(
    "func_name", ("compute_env_comparison_table", "compute_regression_table")
)
def test_hash_parameters_are_declared_baseline_first(func_name):
    """Both tables are called with every argument positional, so declaration order is the wiring.

    Each previously declared comparison_hash before baseline_hash. compute_env_comparison_table
    forwarded them baseline-first, so its two sides were transposed outright;
    compute_regression_table forwarded them comparison-first, so the inversions cancelled and it
    was correct by accident -- but a keyword caller, or an edit to either list alone, silently
    swapped the sides. Canonical order in both means neither can drift on its own.
    """
    import inspect

    import redis_benchmarks_specification.__compare__.compare as mod

    names = list(inspect.signature(getattr(mod, func_name)).parameters)
    assert names.index("baseline_hash") < names.index("comparison_hash")


def test_the_hashes_reach_get_by_strings_on_the_side_the_cli_supplied_them():
    """End-to-end wiring check across both hops, read from the source.

    The call sites are 47- and 48-argument positional lists that cannot run without a live
    datasink, so the binding is resolved statically: caller local -> parameter -> forwarded
    argument -> get_by_strings parameter. This is the check that would have caught the
    transposition; a signature-order assertion alone would not, since the forward can move too.
    """
    import ast
    import inspect

    import redis_benchmarks_specification.__compare__.compare as mod

    tree = ast.parse(inspect.getsource(mod))
    funcs = {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}

    def positional_args(inside, callee):
        for node in ast.walk(funcs[inside]):
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == callee:
                assert not node.keywords, f"{inside} -> {callee} mixes keywords"
                return [a.id if isinstance(a, ast.Name) else None for a in node.args]
        raise AssertionError(f"no call to {callee} inside {inside}")

    def params(name):
        args = funcs[name].args
        return [a.arg for a in args.posonlyargs + args.args]

    gbs = params("get_by_strings")
    for table in ("compute_env_comparison_table", "compute_regression_table"):
        outer = dict(
            zip(params(table), positional_args("compare_command_logic", table))
        )
        forwarded = dict(zip(gbs, positional_args(table, "get_by_strings")))
        for side in SIDES:
            local = forwarded[f"{side}_hash"]
            assert outer[local] == f"{side}_hash", (
                f"{table}: get_by_strings.{side}_hash resolves to the caller's "
                f"{outer[local]}, not {side}_hash"
            )


def test_rejection_raises_rather_than_exiting():
    """SystemExit here would kill the coordinator daemon.

    compute_regression_table reaches this resolver, and the coordinator calls it inside an
    `except Exception` whose comment states the regression comment is best-effort so a data-shape
    problem cannot abort the per-test loop. SystemExit does not derive from Exception, so it
    escaped that guard, took the daemon down and left the stream un-ACKed. The CLI still exits 1;
    compare_command_logic translates this.
    """
    payload = {name: None for name in ALL_KWARGS}
    payload["baseline_branch"] = "B"
    payload["baseline_hash"] = "H" * 40
    payload["comparison_branch"] = "C"
    with pytest.raises(ValueError):
        get_by_strings(**payload)
    try:
        get_by_strings(**payload)
    except BaseException as exc:  # noqa: BLE001 - asserting the exception's own type
        assert not isinstance(exc, SystemExit), "SystemExit escapes the daemon's guard"


def test_the_cli_translates_a_selector_error_into_exit_1():
    """compare_command_logic must keep the CLI's exit code despite the resolver only raising."""
    import ast
    import inspect

    import redis_benchmarks_specification.__compare__.compare as mod

    tree = ast.parse(inspect.getsource(mod))
    fn = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "compare_command_logic"
    )
    handlers = [
        h
        for node in ast.walk(fn)
        if isinstance(node, ast.Try)
        for h in node.handlers
        if getattr(h.type, "id", None) == "ValueError"
        and any(
            isinstance(c, ast.Call)
            and getattr(getattr(c.func, "value", None), "id", None) == "sys"
            and getattr(c.func, "attr", None) == "exit"
            for c in ast.walk(h)
        )
    ]
    assert handlers, "no ValueError handler in compare_command_logic calls sys.exit"


def test_no_selector_is_reassigned_between_validation_and_the_table_calls():
    """The up-front validation is only meaningful if it inspects the values the tables receive.

    compare_command_logic validates the selectors once, then calls one of the two table functions
    much later. A reassignment in between would leave the validation checking stale values, and a
    conflict reaching the resolver at runtime would surface as an uncaught ValueError -- a
    traceback rather than the clean exit 1 the validation exists to produce.
    """
    import ast
    import inspect

    import redis_benchmarks_specification.__compare__.compare as mod

    fn = next(
        n
        for n in ast.walk(ast.parse(inspect.getsource(mod)))
        if isinstance(n, ast.FunctionDef) and n.name == "compare_command_logic"
    )
    guards = [
        n
        for n in ast.walk(fn)
        if isinstance(n, ast.Try)
        and any(getattr(h.type, "id", None) == "ValueError" for h in n.handlers)
    ]
    assert len(guards) == 1, "expected exactly one selector-validation guard"
    validated_at = guards[0].end_lineno
    last_table_call = max(
        n.lineno
        for n in ast.walk(fn)
        if isinstance(n, ast.Call)
        and getattr(n.func, "id", "")
        in ("compute_env_comparison_table", "compute_regression_table")
    )
    selectors = {f"{side}_{suffix}" for side in SIDES for suffix in SUFFIXES}
    reassigned = [
        (node.lineno, target.id)
        for node in ast.walk(fn)
        if isinstance(node, (ast.Assign, ast.AugAssign, ast.AnnAssign))
        for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
        if isinstance(target, ast.Name)
        and target.id in selectors
        and validated_at < node.lineno < last_table_call
    ]
    assert not reassigned, f"selectors reassigned after validation: {reassigned}"
