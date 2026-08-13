#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
"""Property tests for integer settings that arrive as strings.

Every one of these is read at import time, so a bare ``int()`` on a malformed value raises before
any entrypoint runs -- the coordinator, the builder, the runner and the CLI all fail to start, with a
traceback pointing at an import statement rather than at the variable that was wrong. Under a
supervisor that reads as a crash loop with no indication of the cause.

Offline; no Redis, no Docker.
"""

import os
import subprocess
import sys

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from redis_benchmarks_specification.__common__.env import parse_int

# Every integer setting read at import time, with the default each falls back to.
INT_SETTINGS = {
    "GH_REDIS_SERVER_PORT": ("GH_REDIS_SERVER_PORT", 6379),
    "REDIS_AUTH_SERVER_PORT": ("REDIS_AUTH_SERVER_PORT", 6379),
    "REDIS_HEALTH_CHECK_INTERVAL": ("REDIS_HEALTH_CHECK_INTERVAL", 15),
    "REDIS_SOCKET_TIMEOUT": ("REDIS_SOCKET_TIMEOUT", 300),
    "DATASINK_RTS_PORT": ("DATASINK_RTS_PORT", 6379),
    "MAX_PROFILERS": ("MAX_PROFILERS_PER_TYPE", 1),
    "REDIS_BINS_EXPIRE_SECS": ("REDIS_BINS_EXPIRE_SECS", 24 * 7 * 60 * 60),
    "BENCHMARK_PR_MAX_FILES": ("BENCHMARK_PR_MAX_FILES", 100),
}

# Values an operator or a templating system realistically produces, including empty.
HOSTILE = [
    "",
    " ",
    "\t",
    "abc",
    "1.5",
    "0x10",
    "-",
    "+",
    "1e3",
    "None",
    "null",
    "6379 ",
]


def _import_env(**environment):
    """Import the env module in a subprocess and return (returncode, stdout).

    A subprocess because the constants are computed at import time: reloading rebinds them in one
    module while every from-import copy keeps the old value, so a reload cannot show what a fresh
    process would actually see.
    """
    keys = list(INT_SETTINGS)
    clean = {k: v for k, v in os.environ.items() if k not in keys}
    clean.update({k: str(v) for k, v in environment.items()})
    script = (
        "import redis_benchmarks_specification.__common__.env as e;"
        "print(repr({name: getattr(e, attr) for name, (attr, _) in "
        + repr({k: v for k, v in INT_SETTINGS.items()})
        + ".items()}))"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, env=clean
    )
    return result.returncode, result.stdout.strip(), result.stderr


@pytest.mark.parametrize("name", sorted(INT_SETTINGS))
@pytest.mark.parametrize("value", HOSTILE)
def test_no_integer_setting_can_abort_the_import(name, value):
    """The property that matters: a bad value must not stop the process from starting.

    Before this, six of the seven raised ValueError from module scope on any non-numeric value.
    """
    code, _, stderr = _import_env(**{name: value})
    assert code == 0, f"{name}={value!r} aborted the import:\n{stderr[-400:]}"


@pytest.mark.parametrize(
    "name,expected", [(n, d) for n, (_, d) in INT_SETTINGS.items()]
)
@pytest.mark.parametrize("value", ["abc", "", "1.5", "None"])
def test_a_malformed_value_falls_back_to_the_documented_default(name, expected, value):
    """And the fallback must be the documented default, not zero or None."""
    code, out, _ = _import_env(**{name: value})
    assert code == 0
    assert eval(out)[name] == expected


@pytest.mark.parametrize("name", sorted(INT_SETTINGS))
def test_a_valid_value_is_honoured(name):
    """The fallback must not swallow good input."""
    code, out, _ = _import_env(**{name: 4242})
    assert code == 0
    assert eval(out)[name] == 4242


def test_a_non_positive_max_files_falls_back_rather_than_disabling_scoping():
    """It bounds the synchronous GitHub pagination inside the webhook request.

    A non-positive value would make every labelled PR quietly fall back to a full suite, which is
    expensive and invisible.
    """
    for value in ("0", "-1", "-100"):
        code, out, _ = _import_env(BENCHMARK_PR_MAX_FILES=value)
        assert code == 0
        assert eval(out)["BENCHMARK_PR_MAX_FILES"] == 100


# --- the helper itself ------------------------------------------------------------------------


@given(value=st.integers())
def test_an_integer_survives_the_str_round_trip(value):
    """The property the callers need: parse(str(v)) == v."""
    assert parse_int(str(value), default=-1) == value


@given(value=st.integers())
def test_an_actual_int_passes_through(value):
    assert parse_int(value, default=-1) == value


@pytest.mark.parametrize("value", HOSTILE + [None, [], {}, object()])
def test_anything_unparseable_yields_the_default(value):
    if isinstance(value, str) and value.strip().lstrip("+-").isdigit():
        pytest.skip("parseable")
    assert parse_int(value, default=99) == 99


@pytest.mark.parametrize(
    "value,expected", [(b"12", 12), (bytearray(b"7"), 7), (b"\xff", 99)]
)
def test_bytes_are_decoded_and_undecodable_bytes_fall_back(value, expected):
    """Stream and environment reads are not always decoded first."""
    assert parse_int(value, default=99) == expected


@pytest.mark.parametrize("value,expected", [("1_000", 1000), ("٣", 3), (" 42 ", 42)])
def test_the_permissiveness_of_int_is_preserved(value, expected):
    """int() accepts underscores and Unicode digits, and that is deliberately not tightened.

    Pinned because it is surprising, and because narrowing it would reject values that work today.
    """
    assert parse_int(value, default=-1) == expected


def test_a_fallback_names_the_variable(caplog):
    """A warning that does not say which variable was wrong sends the reader to the source."""
    import logging

    with caplog.at_level(logging.WARNING):
        parse_int("abc", default=5, name="SOME_SETTING")
    assert any("SOME_SETTING" in r.getMessage() for r in caplog.records)


def test_a_valid_value_does_not_warn(caplog):
    """Otherwise every normal start logs noise and the real warnings stop being read."""
    import logging

    with caplog.at_level(logging.WARNING):
        parse_int("7", default=5, name="SOME_SETTING")
    assert not [r for r in caplog.records if "SOME_SETTING" in r.getMessage()]


def test_no_module_reads_an_integer_setting_with_a_bare_int_cast():
    """Guards the whole package, since the mistake is invisible at the call site.

    Read from the syntax tree rather than the text: matching `int(os.getenv(...))` as a call node
    survives reformatting and cannot be fooled by the pattern appearing in a comment.
    """
    import ast
    import pathlib

    import redis_benchmarks_specification

    package = pathlib.Path(redis_benchmarks_specification.__file__).parent
    scanned = sorted(package.rglob("*.py"))
    assert len(scanned) > 20, f"only scanned {len(scanned)} files"

    offenders = []
    for path in scanned:
        for node in ast.walk(ast.parse(path.read_text())):
            if (
                not isinstance(node, ast.Call)
                or getattr(node.func, "id", None) != "int"
            ):
                continue
            for argument in node.args:
                if (
                    isinstance(argument, ast.Call)
                    and getattr(argument.func, "attr", None) == "getenv"
                ):
                    offenders.append(f"{path.relative_to(package)}:{node.lineno}")
    assert not offenders, (
        "int(os.getenv(...)) aborts the import on a malformed value; use parse_int: "
        f"{offenders}"
    )
