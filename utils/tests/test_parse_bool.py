"""Property tests for boolean decoding of values that have been through a string round-trip.

Booleans reach this package as strings: written to the commits and builds streams with `str()`,
and read from the environment. A bare `bool()` on such a value is wrong in the one direction that
matters, because every non-empty string is truthy -- so `"False"`, `"0"` and `"no"` all decode to
`True` and the flag is stuck on. `bool(b"False")` is `True` as well, which matters because stream
reads are not always decoded first.

Offline; no Redis, no Docker.
"""

import itertools

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
