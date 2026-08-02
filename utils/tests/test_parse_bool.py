#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
"""Property tests for boolean decoding of values that have been through a string round-trip.

Booleans reach this package as strings: written to the commits and builds streams with `str()`,
and read from the environment. A bare `bool()` on such a value is wrong in the one direction that
matters, because every non-empty string is truthy -- so `"False"`, `"0"` and `"no"` all decode to
`True` and the flag is stuck on. `bool(b"False")` is `True` as well, which matters because stream
reads are not always decoded first.

Offline; no Redis, no Docker.
"""

import argparse
import itertools
import logging

import pytest
from hypothesis import given
from hypothesis import strategies as st

from redis_benchmarks_specification.__common__.env import (
    _FALSE_STRINGS,
    _TRUE_STRINGS,
    accepted_bool_spellings,
    parse_bool,
)

# Pinned as literals, deliberately NOT derived from the module. The tables decide what every
# boolean flag means, and a test that iterates them only asserts that the lookup reads its own
# table -- moving "off" from one to the other would keep such a suite green.
EXPECTED_TRUE = ("1", "true", "t", "yes", "y", "on")
EXPECTED_FALSE = ("0", "false", "f", "no", "n", "off")


@given(value=st.booleans())
def test_a_bool_survives_the_str_round_trip(value):
    """The property the stream needs and `bool()` does not provide.

    `str(False)` is `"False"`, and `bool("False")` is `True`. This is the whole defect.
    """
    assert parse_bool(str(value), default=None) is value


@given(value=st.booleans())
def test_a_bool_survives_the_round_trip_as_bytes(value):
    """Stream reads are not always decoded, so the bytes form must round-trip too."""
    assert parse_bool(str(value).encode(), default=None) is value


@pytest.mark.parametrize("text", EXPECTED_TRUE)
def test_true_spellings(text):
    assert parse_bool(text) is True
    assert parse_bool(text.upper()) is True
    assert parse_bool(f"  {text}  ") is True


@pytest.mark.parametrize("text", EXPECTED_FALSE)
def test_false_spellings(text):
    assert parse_bool(text) is False
    assert parse_bool(text.upper()) is False
    assert parse_bool(f"  {text}  ") is False


def test_the_module_tables_match_the_pinned_literals():
    """The assertion that makes every other spelling test non-vacuous.

    Every parametrized spelling test above iterates the pinned literals, so without this the module
    could move "off" from one table to the other and the suite would stay green -- it would only be
    asserting that the lookup reads its own table.
    """
    assert _TRUE_STRINGS == EXPECTED_TRUE
    assert _FALSE_STRINGS == EXPECTED_FALSE


def test_true_and_false_spellings_do_not_overlap():
    """A spelling in both tables would make the tables' order decide the answer."""
    assert not set(EXPECTED_TRUE) & set(EXPECTED_FALSE)


@pytest.mark.parametrize("value", [True, False])
def test_an_actual_bool_passes_through(value):
    """Call sites are being converted one at a time, so both forms must work during the overlap."""
    assert parse_bool(value) is value


@pytest.mark.parametrize("default", [True, False])
@pytest.mark.parametrize(
    "value", ["maybe", "yes-ish", "2", "None", "null", b"\xff\xfe"]
)
def test_an_unrecognised_value_returns_the_default(value, default):
    """Returning the default beats guessing, which is what `bool()` already does.

    Includes an undecodable byte string: the previous code called `.decode()` unguarded, so a
    corrupt stream field raised UnicodeDecodeError from inside the consumer loop.
    """
    assert parse_bool(value, default=default) is default


@pytest.mark.parametrize("default", [True, False])
def test_none_returns_the_default(default):
    assert parse_bool(None, default=default) is default


@given(
    value=st.one_of(
        st.booleans(),
        st.sampled_from(EXPECTED_TRUE + EXPECTED_FALSE),
        st.integers(min_value=-3, max_value=3),
        st.none(),
    )
)
def test_parse_bool_always_returns_a_bool(value):
    """Never a truthy string, never None -- callers use the result in `if` and in f-strings."""
    assert isinstance(parse_bool(value, default=False), bool)


# --- the strict form, which is what argparse gets --------------------------------------------


@pytest.mark.parametrize("text", EXPECTED_TRUE + EXPECTED_FALSE)
def test_the_strict_and_defaulting_forms_agree_on_every_spelling(text):
    """Omitting `default` must change only what happens to unrecognised input.

    If the two forms disagreed on a recognised spelling, a flag would mean one thing on the command
    line and another when the same value arrived from a stream.
    """
    assert parse_bool(text) is parse_bool(text, default=None)


@pytest.mark.parametrize("text", ["maybe", "2", "tru", "flase", None])
def test_omitting_the_default_raises_on_unrecognised_input(text):
    """On a command line a typo should be reported, not guessed at.

    ArgumentTypeError specifically: argparse reproduces its message verbatim, whereas a ValueError
    is replaced with "invalid parse_bool value: ..." -- which names an internal function and drops
    the list of spellings the reader needs.
    """
    with pytest.raises(argparse.ArgumentTypeError):
        parse_bool(text)


@pytest.mark.parametrize("text", ["maybe", "2", "tru"])
def test_the_raised_message_names_every_accepted_spelling(text):
    with pytest.raises(argparse.ArgumentTypeError) as excinfo:
        parse_bool(text)
    for spelling in accepted_bool_spellings():
        assert spelling in str(excinfo.value), f"message omits {spelling!r}"


def test_the_cli_no_longer_imports_distutils():
    """distutils was removed from the standard library in 3.12.

    It resolves today only through a setuptools shim, so `from distutils.util import strtobool` at
    module scope made the CLI fail at import time in any 3.12 environment without setuptools --
    which is a hard failure of the entire command, not a degraded flag.
    """
    import inspect

    import redis_benchmarks_specification.__cli__.args as args_mod

    assert "distutils" not in inspect.getsource(args_mod)


def test_use_git_timestamp_flag_parses_false():
    """The flag this defect was found through, end to end via the real parser."""
    import argparse

    from redis_benchmarks_specification.__cli__.args import spec_cli_args

    parser = spec_cli_args(argparse.ArgumentParser())
    assert (
        parser.parse_args(["--use-git-timestamp", "false"]).use_git_timestamp is False
    )
    assert parser.parse_args(["--use-git-timestamp", "true"]).use_git_timestamp is True
    assert parser.parse_args([]).use_git_timestamp is True


def test_every_accepted_spelling_is_lowercase_in_the_tables():
    """Lookup lowercases its input, so an uppercase table entry would be unreachable."""
    for text in itertools.chain(EXPECTED_TRUE, EXPECTED_FALSE):
        assert text == text.lower(), f"{text!r} can never match"


# --- the flags this defect reached ------------------------------------------------------------

COMPARE_BOOL_FLAGS = (
    "--print-regressions-only",
    "--print-improvements-only",
    "--skip-unstable",
    "--verbose",
    "--simple-table",
    "--use_metric_context_path",
)


@pytest.mark.parametrize("flag", COMPARE_BOOL_FLAGS)
@pytest.mark.parametrize(
    "text,expected", [("true", True), ("false", False), ("0", False)]
)
def test_compare_boolean_flags_honour_a_false_value(flag, text, expected):
    """`type=bool` called bool() on the raw argv string, so `--skip-unstable false` meant True.

    These six are all consumed by compare_command_logic, and they are the flags the compare CLI is
    driven with, so a flag that silently inverts changes which rows a comparison prints.
    """
    import argparse

    from redis_benchmarks_specification.__compare__.args import create_compare_arguments

    parser = create_compare_arguments(argparse.ArgumentParser())
    namespace = parser.parse_args([flag, text])
    assert getattr(namespace, flag.lstrip("-").replace("-", "_")) is expected


@pytest.mark.parametrize("flag", COMPARE_BOOL_FLAGS)
def test_compare_boolean_flags_reject_a_typo(flag):
    """A misspelled value must be a usage error, not silently False."""
    import argparse

    from redis_benchmarks_specification.__compare__.args import create_compare_arguments

    parser = create_compare_arguments(argparse.ArgumentParser())
    with pytest.raises(SystemExit):
        parser.parse_args([flag, "flase"])


def test_no_argument_parser_uses_type_bool():
    """`type=bool` is never what anyone means.

    argparse hands the `type` callable the raw argv string, so `type=bool` accepts any non-empty
    value as True -- including "false", "0" and "no". Guarding the whole package because the
    mistake is easy to reintroduce and impossible to see at the call site.
    """
    import pathlib

    import redis_benchmarks_specification

    package = pathlib.Path(redis_benchmarks_specification.__file__).parent
    scanned = list(package.rglob("*.py"))
    # A guard that cannot see the package must fail, not pass. Derived from __file__ because a
    # relative path made this succeed vacuously from any directory but the repo root.
    assert len(scanned) > 20, f"only scanned {len(scanned)} files under {package}"
    offenders = [
        f"{path}:{number}"
        for path in scanned
        for number, line in enumerate(path.read_text().splitlines(), start=1)
        if "type=bool" in line.replace(" ", "")
    ]
    assert not offenders, f"type=bool used at {offenders}"


def test_datasink_push_env_var_honours_a_falsy_value():
    """DATASINK_PUSH_RTS=0 previously enabled pushing, with no way to turn it off.

    It is the default of a --datasink_push_results_redistimeseries store_true flag, so once the
    default reads True the command line cannot override it. Re-imports the module because the value
    is computed at import time.
    """
    import importlib
    import os

    import redis_benchmarks_specification.__common__.env as env_mod

    original = os.environ.get("DATASINK_PUSH_RTS")
    try:
        for raw, expected in (
            ("0", False),
            ("false", False),
            ("no", False),
            ("1", True),
            ("true", True),
        ):
            os.environ["DATASINK_PUSH_RTS"] = raw
            reloaded = importlib.reload(env_mod)
            assert (
                reloaded.DATASINK_RTS_PUSH is expected
            ), f"DATASINK_PUSH_RTS={raw!r} read as {reloaded.DATASINK_RTS_PUSH!r}"
        os.environ.pop("DATASINK_PUSH_RTS")
        assert importlib.reload(env_mod).DATASINK_RTS_PUSH is False
    finally:
        os.environ.pop("DATASINK_PUSH_RTS", None)
        if original is not None:
            os.environ["DATASINK_PUSH_RTS"] = original
        importlib.reload(env_mod)


@pytest.mark.parametrize("value", ["2", "enabled", "maybe", "None"])
def test_an_unrecognised_value_warns_rather_than_falling_back_quietly(caplog, value):
    """A silent fallback would turn a misconfiguration into missing data.

    bool() read every non-empty string as True, so an unrecognised truthy-looking value used to
    enable a flag and now takes the default instead. Where the flag gates whether benchmark results
    are recorded, flipping it off without saying so is worse than either answer.
    """
    with caplog.at_level(logging.WARNING):
        parse_bool(value, default=False)
    assert any(
        "Unrecognised boolean" in record.getMessage() for record in caplog.records
    ), f"no warning for {value!r}"


@pytest.mark.parametrize(
    "value", list(EXPECTED_TRUE) + [f for f in EXPECTED_FALSE if f]
)
def test_a_recognised_value_does_not_warn(caplog, value):
    """Otherwise every normal run logs noise and the real warnings stop being read."""
    with caplog.at_level(logging.WARNING):
        parse_bool(value, default=False)
    assert not [
        r for r in caplog.records if "Unrecognised boolean" in r.getMessage()
    ], f"spurious warning for {value!r}"


def test_the_warning_names_the_accepted_spellings(caplog):
    """A warning that does not say what was expected sends the reader to the source."""
    with caplog.at_level(logging.WARNING):
        parse_bool("enabled", default=False)
    text = " ".join(r.getMessage() for r in caplog.records)
    for spelling in accepted_bool_spellings():
        assert spelling in text, f"warning does not mention {spelling!r}"


@pytest.mark.parametrize("raw", ["2", "enabled", "on-please", "sure"])
def test_an_unrecognised_datasink_setting_cannot_turn_pushing_off(raw):
    """The fix must never be able to stop results reaching the datasink.

    When DATASINK_RTS_PUSH is False the datasink connection is None and nothing is exported, so a
    wrong answer in that direction means benchmark results silently stop being recorded. The old
    bool() read any non-empty string as True; the default here reproduces that, so only recognised
    falsy spellings change meaning. The fleet's actual setting is not visible from here, which is
    exactly why this direction is pinned rather than assumed.
    """
    import importlib
    import os

    import redis_benchmarks_specification.__common__.env as env_mod

    original = os.environ.get("DATASINK_PUSH_RTS")
    try:
        os.environ["DATASINK_PUSH_RTS"] = raw
        assert importlib.reload(env_mod).DATASINK_RTS_PUSH is True
    finally:
        os.environ.pop("DATASINK_PUSH_RTS", None)
        if original is not None:
            os.environ["DATASINK_PUSH_RTS"] = original
        importlib.reload(env_mod)


@pytest.mark.parametrize("text", ["", "   ", "\t"])
def test_an_empty_value_stays_false_on_the_command_line(text):
    """`--verbose "$VERBOSE"` with the variable unset is the shape that produces this.

    bool("") was already False, so turning it into a usage error would be a regression on the one
    out-of-table value a shell produces by accident rather than by typo.
    """
    assert parse_bool(text) is False


@pytest.mark.parametrize("default", [True, False])
@pytest.mark.parametrize("text", ["", "   ", None])
def test_an_empty_value_means_absent_for_a_stream_or_environment_value(text, default):
    """Empty must take the caller's default, not False.

    Several of the flags still to be converted default True, and an absent or empty stream field
    silently flipping them off would reintroduce this same bug class in the other direction.
    """
    assert parse_bool(text, default=default) is default


@pytest.mark.parametrize("value", [2, -1, 2.5, 0])
def test_numbers_are_rejected(value):
    """Accepting ints would contradict the property this function exists to provide.

    parse_bool(2) would have to be True while parse_bool(str(2)) is not, so the round trip the
    module is named for would fail over the int domain.
    """
    with pytest.raises(argparse.ArgumentTypeError):
        parse_bool(value)


def test_the_accepted_spellings_are_exactly_strtobool_s():
    """Pinned against strtobool itself, which this replaces.

    The PR claims the spellings match; this is that claim as a test rather than as prose. Skipped
    only where distutils is genuinely unavailable, which is the situation being fixed.
    """
    strtobool = pytest.importorskip("distutils.util").strtobool
    for text in EXPECTED_TRUE:
        assert bool(strtobool(text)) is True
        assert parse_bool(text) is True
    for text in EXPECTED_FALSE:
        assert bool(strtobool(text)) is False
        assert parse_bool(text) is False
    # and nothing outside that set is accepted by either
    for text in ("2", "maybe", "enabled"):
        with pytest.raises(ValueError):
            strtobool(text)
        with pytest.raises(argparse.ArgumentTypeError):
            parse_bool(text)


def test_the_strict_form_survives_a_module_reload():
    """A module-level object() sentinel silently breaks the strict form after a reload.

    The signature default is captured once at definition time while the identity test reads the
    module global, so a reload rebinds one and not the other -- and parse_bool then treats "no
    default" as "default given" and returns the sentinel instead of raising. Found because the
    environment-variable tests in this file reload the module, which made the number-rejection
    tests pass alone and fail in the suite. Ellipsis is a builtin singleton and cannot drift.
    """
    import importlib

    import redis_benchmarks_specification.__common__.env as env_mod

    importlib.reload(env_mod)
    with pytest.raises(argparse.ArgumentTypeError):
        env_mod.parse_bool("maybe")
    # and the binding captured before the reload must behave the same way
    with pytest.raises(argparse.ArgumentTypeError):
        parse_bool("maybe")


@pytest.mark.parametrize(
    "name,default",
    [("PROFILE", False), ("BENCHMARK_PR_DIFF_SCOPING", True)],
)
def test_the_other_boolean_environment_variables_use_the_same_decoder(name, default):
    """One decoder for the whole module, not four idioms in one file.

    PROFILE was bool(int(...)), which raises at import time on "false" and aborts every entrypoint.
    BENCHMARK_PR_DIFF_SCOPING was a hand-rolled copy of parse_bool accepting a different set -- no
    "t", no "y" -- which is exactly how the spellings drift apart.
    """
    import importlib
    import os

    import redis_benchmarks_specification.__common__.env as env_mod

    attribute = {
        "PROFILE": "PROFILERS_ENABLED",
        "BENCHMARK_PR_DIFF_SCOPING": "BENCHMARK_PR_DIFF_SCOPING",
    }[name]
    original = os.environ.get(name)
    try:
        for raw, expected in (
            ("0", False),
            ("false", False),
            ("off", False),
            ("1", True),
            ("t", True),
        ):
            os.environ[name] = raw
            reloaded = importlib.reload(env_mod)
            assert (
                getattr(reloaded, attribute) is expected
            ), f"{name}={raw!r} read as {getattr(reloaded, attribute)!r}"
        os.environ.pop(name)
        assert getattr(importlib.reload(env_mod), attribute) is default
    finally:
        os.environ.pop(name, None)
        if original is not None:
            os.environ[name] = original
        importlib.reload(env_mod)
