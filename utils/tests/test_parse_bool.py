"""Property tests for boolean decoding of values that have been through a string round-trip.

Booleans reach this package as strings: written to the commits and builds streams with `str()`,
and read from the environment. A bare `bool()` on such a value is wrong in the one direction that
matters, because every non-empty string is truthy -- so `"False"`, `"0"` and `"no"` all decode to
`True` and the flag is stuck on. `bool(b"False")` is `True` as well, which matters because stream
reads are not always decoded first.

Offline; no Redis, no Docker.
"""

import itertools
import logging

import pytest
from hypothesis import given
from hypothesis import strategies as st

from redis_benchmarks_specification.__common__.env import (
    FALSE_STRINGS,
    TRUE_STRINGS,
    parse_bool,
    parse_bool_arg,
)


@given(value=st.booleans())
def test_a_bool_survives_the_str_round_trip(value):
    """The property the stream needs and `bool()` does not provide.

    `str(False)` is `"False"`, and `bool("False")` is `True`. This is the whole defect.
    """
    assert parse_bool(str(value)) is value


@given(value=st.booleans())
def test_a_bool_survives_the_round_trip_as_bytes(value):
    """Stream reads are not always decoded, so the bytes form must round-trip too."""
    assert parse_bool(str(value).encode()) is value


@pytest.mark.parametrize("text", TRUE_STRINGS)
def test_true_spellings(text):
    assert parse_bool(text) is True
    assert parse_bool(text.upper()) is True
    assert parse_bool(f"  {text}  ") is True


@pytest.mark.parametrize("text", FALSE_STRINGS)
def test_false_spellings(text):
    assert parse_bool(text) is False
    assert parse_bool(text.upper()) is False
    assert parse_bool(f"  {text}  ") is False


def test_true_and_false_spellings_do_not_overlap():
    """A spelling in both tables would make the tables' order decide the answer."""
    assert not set(TRUE_STRINGS) & set(FALSE_STRINGS)


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


@given(number=st.integers())
def test_numbers_follow_python_truthiness(number):
    """A stream field written from an int should not become a silent default."""
    assert parse_bool(number) is bool(number)


@given(
    value=st.one_of(
        st.booleans(),
        st.sampled_from(TRUE_STRINGS + FALSE_STRINGS),
        st.integers(min_value=-3, max_value=3),
        st.none(),
    )
)
def test_parse_bool_always_returns_a_bool(value):
    """Never a truthy string, never None -- callers use the result in `if` and in f-strings."""
    assert isinstance(parse_bool(value), bool)


# --- the argparse variant -------------------------------------------------------------------


@pytest.mark.parametrize(
    "text", [t for t in TRUE_STRINGS + FALSE_STRINGS if t]  # "" is rejected, see below
)
def test_parse_bool_arg_accepts_every_spelling_parse_bool_does(text):
    """The two must agree, or a flag means one thing on the CLI and another on the stream."""
    assert parse_bool_arg(text) is parse_bool(text)


@pytest.mark.parametrize("text", ["maybe", "", "2", "tru", "flase"])
def test_parse_bool_arg_rejects_unrecognised_input(text):
    """On a command line a typo should be reported, not guessed at.

    argparse converts ValueError from a `type=` callable into a usage error, so this is what makes
    `--use-git-timestamp flase` fail loudly instead of silently meaning False.
    """
    with pytest.raises(ValueError):
        parse_bool_arg(text)


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
    for text in itertools.chain(TRUE_STRINGS, FALSE_STRINGS):
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

    package = pathlib.Path("redis_benchmarks_specification")
    offenders = [
        f"{path}:{number}"
        for path in package.rglob("*.py")
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
        parse_bool(value)
    assert any(
        "Unrecognised boolean" in record.getMessage() for record in caplog.records
    ), f"no warning for {value!r}"


@pytest.mark.parametrize("value", list(TRUE_STRINGS) + [f for f in FALSE_STRINGS if f])
def test_a_recognised_value_does_not_warn(caplog, value):
    """Otherwise every normal run logs noise and the real warnings stop being read."""
    with caplog.at_level(logging.WARNING):
        parse_bool(value)
    assert not [
        r for r in caplog.records if "Unrecognised boolean" in r.getMessage()
    ], f"spurious warning for {value!r}"


def test_the_warning_names_the_accepted_spellings(caplog):
    """A warning that does not say what was expected sends the reader to the source."""
    with caplog.at_level(logging.WARNING):
        parse_bool("enabled")
    text = " ".join(r.getMessage() for r in caplog.records)
    for spelling in TRUE_STRINGS + tuple(f for f in FALSE_STRINGS if f):
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
