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

    Includes an undecodable byte string, because the stream decode sites this is intended for call
    `.decode()` unguarded today -- a corrupt field would raise from inside a consumer loop.
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

    It resolves only through a setuptools shim. setuptools is an explicit runtime dependency here,
    so this is hygiene -- dropping a dependency on a deprecated shim -- and not the "the CLI cannot
    start" hazard an earlier version of this PR claimed.
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


def test_the_accepted_spellings_are_strtobool_s_plus_whitespace():
    """Pinned against strtobool itself, which this replaces.

    A superset, not an exact match: surrounding whitespace is tolerated where strtobool raises. Both
    directions are asserted. Skipped where distutils is genuinely unavailable.
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
    # the superset direction: whitespace is tolerated here and rejected there
    for text in (" true ", "\tfalse\n"):
        with pytest.raises(ValueError):
            strtobool(text)
        assert parse_bool(text) is (text.strip() == "true")


def _all_parsers():
    """The parsers that can be built in-process.

    __spec__/args.py is covered separately, in a subprocess. It IS importable once a sibling has
    populated sys.modules -- the cold-interpreter failure of issue #466 is an import-order artifact,
    not an absolute -- but importing it binds the subpackage over the parent's ModuleSpec for the
    rest of the process, and Flask reads `.origin` off that to find an app's root path. Doing it here
    breaks test_app.py whenever the two files run in the same session, in that order.
    """
    import argparse

    from redis_benchmarks_specification.__cli__.args import spec_cli_args
    from redis_benchmarks_specification.__compare__.args import create_compare_arguments

    return {
        "compare": create_compare_arguments(argparse.ArgumentParser()),
        "cli": spec_cli_args(argparse.ArgumentParser()),
    }


def test_no_parser_action_converts_with_builtin_bool():
    """Introspect what argparse will call, not the characters used to name it.

    A text scan for "type=bool" is the obvious guard and the wrong one: it misses
    `builtins.bool`, `eval("bool")`, a functools.partial, an alias, a tab, and -- the one that
    happens without any intent to evade -- a kwarg that black reformats across lines. Reading
    action.type off the built parser catches all of them, and cannot pass vacuously depending on
    the working directory the way a relative path scan did.
    """
    import functools

    offenders = []
    for name, parser in _all_parsers().items():
        for action in parser._actions:
            converter = action.type
            if isinstance(converter, functools.partial):
                converter = converter.func
            if converter is bool:
                offenders.append(f"{name}:{action.option_strings}")
    assert not offenders, f"builtin bool used as an argparse converter: {offenders}"


def test_every_boolean_flag_uses_the_canonical_decoder():
    """Any flag that takes a boolean-looking value must go through parse_bool.

    Otherwise a flag can be added with a hand-rolled converter that accepts a different set, which
    is how the spellings drifted apart inside env.py in the first place.
    """
    known_boolean_flags = {
        "--print-regressions-only",
        "--print-improvements-only",
        "--skip-unstable",
        "--verbose",
        "--simple-table",
        "--use_metric_context_path",
        "--use-git-timestamp",
        "--trigger-unstable-commits",
    }
    seen = set()
    for parser in _all_parsers().values():
        for action in parser._actions:
            for option in action.option_strings:
                if option in known_boolean_flags:
                    seen.add(option)
                    assert (
                        action.type is parse_bool
                    ), f"{option} converts with {action.type!r}, not parse_bool"
    assert (
        seen == known_boolean_flags
    ), f"flags not found in any parser: {known_boolean_flags - seen}"


@pytest.mark.parametrize("parser_name", ["cli"])
@pytest.mark.parametrize(
    "text,expected", [("false", False), ("0", False), ("true", True)]
)
def test_trigger_unstable_commits_honours_a_false_value(parser_name, text, expected):
    """The two flags that were previously guarded by nothing but a text scan.

    Declared in two separate parsers and read nowhere, so the conversion is a no-op today -- but a
    dead flag is exactly where a reintroduced defect goes unnoticed.
    """
    parser = _all_parsers()[parser_name]
    assert (
        parser.parse_args(["--trigger-unstable-commits", text]).trigger_unstable_commits
        is expected
    )


@pytest.mark.parametrize("parser_name", ["cli"])
def test_trigger_unstable_commits_rejects_a_typo(parser_name):
    parser = _all_parsers()[parser_name]
    with pytest.raises(SystemExit):
        parser.parse_args(["--trigger-unstable-commits", "flase"])


@pytest.mark.parametrize("text", ["FALSE", " false ", "\tFalse\n", "OFF"])
def test_the_strict_form_tolerates_case_and_padding(text):
    """The CLI half of the case/padding contract, which was pinned only for the other half."""
    assert parse_bool(text) is False


@pytest.mark.parametrize("value", [bytearray(b"false"), bytearray(b"true")])
def test_bytearray_is_decoded_like_bytes(value):
    """Accepted for the stream sites, so it needs a pin rather than an untested branch."""
    assert parse_bool(value) is (value == bytearray(b"true"))


def _env_probe(script, **env):
    """Run a snippet in a subprocess with the given environment, returning its stdout.

    A subprocess rather than importlib.reload: env.py computes its constants at import time, and
    reloading it rebinds function objects while every module that did a from-import keeps the old
    ones. That makes identity checks order-dependent -- a test can pass alone and fail in the suite,
    which is exactly what happened here -- and it never reaches the from-import copies that are what
    production actually ships.
    """
    import os
    import subprocess
    import sys

    environment = dict(os.environ)
    environment.pop("DATASINK_PUSH_RTS", None)
    environment.update({key: str(value) for key, value in env.items()})
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=environment,
        check=True,
    )
    return result.stdout.strip()


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("0", "False"),
        ("false", "False"),
        ("no", "False"),
        ("off", "False"),
        ("1", "True"),
        ("true", "True"),
    ],
)
def test_datasink_push_env_var_honours_a_falsy_value(raw, expected):
    """DATASINK_PUSH_RTS=0 previously enabled pushing, with no way to turn it off."""
    script = (
        "from redis_benchmarks_specification.__common__.env import DATASINK_RTS_PUSH;"
        "print(DATASINK_RTS_PUSH)"
    )
    assert _env_probe(script, DATASINK_PUSH_RTS=raw) == expected


@pytest.mark.parametrize("raw", ["2", "enabled", "on-please", " "])
def test_an_unrecognised_datasink_setting_cannot_turn_pushing_off(raw):
    """The fix must never be able to stop results reaching the datasink.

    When this is False the datasink connection is None and nothing is exported, so a wrong answer in
    that direction means benchmark results silently stop being recorded. The old bool() read any
    non-empty string as True and the default reproduces that, so only recognised falsy spellings
    change meaning. The fleet's setting is not visible from here, which is why this is pinned.
    """
    script = (
        "from redis_benchmarks_specification.__common__.env import DATASINK_RTS_PUSH;"
        "print(DATASINK_RTS_PUSH)"
    )
    assert _env_probe(script, DATASINK_PUSH_RTS=raw) == "True"


@pytest.mark.parametrize("module", ["__runner__", "__self_contained_coordinator__"])
@pytest.mark.parametrize("raw,expected", [("0", "False"), ("1", "True")])
def test_the_datasink_flag_default_the_parsers_ship_follows_the_env_var(
    module, raw, expected
):
    """Assert the argparse default, not the module constant.

    Both of these do `from ...env import DATASINK_RTS_PUSH` and use it as a store_true default, so
    the constant is copied at import. Asserting the constant would pass while the default the flag
    actually ships was stale -- and the default is the whole reason this value matters, since a
    store_true flag cannot turn itself off.
    """
    factory = (
        "create_client_runner_args"
        if module == "__runner__"
        else "create_self_contained_coordinator_args"
    )
    script = (
        f"from redis_benchmarks_specification.{module}.args import {factory};"
        f"p={factory}('probe');"
        "print(p.parse_args([]).datasink_push_results_redistimeseries)"
    )
    assert _env_probe(script, DATASINK_PUSH_RTS=raw) == expected


@pytest.mark.parametrize(
    "name,attribute,raw,expected",
    [
        ("PROFILE", "PROFILERS_ENABLED", "false", "False"),
        ("PROFILE", "PROFILERS_ENABLED", "off", "False"),
        ("PROFILE", "PROFILERS_ENABLED", "1", "True"),
        ("PROFILE", "PROFILERS_ENABLED", "t", "True"),
        ("BENCHMARK_PR_DIFF_SCOPING", "BENCHMARK_PR_DIFF_SCOPING", "0", "False"),
        ("BENCHMARK_PR_DIFF_SCOPING", "BENCHMARK_PR_DIFF_SCOPING", "t", "True"),
    ],
)
def test_the_other_boolean_environment_variables_use_the_same_decoder(
    name, attribute, raw, expected
):
    """One decoder for the whole module, not four idioms in one file.

    PROFILE was bool(int(...)), which raises at import time on "false" and aborts every entrypoint.
    BENCHMARK_PR_DIFF_SCOPING was a hand-rolled copy accepting a different set -- no "t", no "y" --
    which is exactly how spellings drift apart.
    """
    script = (
        f"from redis_benchmarks_specification.__common__.env import {attribute};"
        f"print({attribute})"
    )
    assert _env_probe(script, **{name: raw}) == expected


def test_the_spec_subpackage_cannot_be_imported_from_a_cold_interpreter():
    """The shape of issue #466, which is why the console script cannot start.

    The package contains a subpackage named __spec__, which shadows the ModuleSpec CPython's import
    machinery puts on the parent package, so importing anything beneath it raises AttributeError.
    That also means the `redis-benchmarks-spec` console script, whose entrypoint is
    redis_benchmarks_specification.__spec__.cli:main, cannot start.

    Broken on every supported version; only the attribute named in the error differs -- 3.10 reports
    submodule_search_locations, 3.12 reports _uninitialized_submodules. So the assertion matches the
    shadowed module rather than either internal name.

    Order-dependent, not absolute: _all_parsers above imports a sibling first and then loads
    __spec__/args.py without trouble. Only a cold interpreter -- which is what a console-script
    entrypoint is -- hits it. Pre-existing and out of scope here; asserted so that fixing it fails
    this test rather than passing unnoticed.
    """
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-c", "import redis_benchmarks_specification.__spec__.args"],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0, "issue #466 appears fixed -- drop this test"
    assert "AttributeError" in result.stderr, result.stderr
    assert "__spec__" in result.stderr, result.stderr


def test_use_git_timestamp_with_an_empty_value():
    """The one flag whose empty-value behaviour changed, pinned rather than left implicit.

    It used strtobool, which raised on "", while the other eight read "" as False. Unified to False.
    Moot in practice: the parsed value is discarded before it reaches the stream (issue #464), so
    this cannot change what any run does today -- but it is a real difference from the base and
    belongs under test rather than in a comment.
    """
    parser = _all_parsers()["cli"]
    assert parser.parse_args(["--use-git-timestamp", ""]).use_git_timestamp is False
    assert parser.parse_args(["--use-git-timestamp", "  "]).use_git_timestamp is False


@pytest.mark.parametrize(
    "raw,expected",
    [("2", "True"), ("-1", "True"), ("01", "True"), ("0", "False"), ("1", "True")],
)
def test_profile_preserves_the_any_non_zero_integer_reading(raw, expected):
    """PROFILE was bool(int(...)), so every non-zero integer meant enabled.

    Narrowing to the literal "1" would flip PROFILE=2 from on to off and silently stop profiling
    artifacts being collected -- the same data-losing direction that DATASINK_PUSH_RTS is protected
    against, so it gets the same policy rather than the opposite one.
    """
    script = (
        "from redis_benchmarks_specification.__common__.env import PROFILERS_ENABLED;"
        "print(PROFILERS_ENABLED)"
    )
    assert _env_probe(script, PROFILE=raw) == expected


@pytest.mark.parametrize("raw", ["false", "off", "no", ""])
def test_profile_no_longer_aborts_the_process_on_a_word(raw):
    """bool(int("false")) raises ValueError at import, taking down every entrypoint."""
    script = (
        "from redis_benchmarks_specification.__common__.env import PROFILERS_ENABLED;"
        "print(PROFILERS_ENABLED)"
    )
    assert _env_probe(script, PROFILE=raw) == "False"


@pytest.mark.parametrize("raw", ["maybe", "enabled", "2"])
def test_pr_diff_scoping_leaves_an_unrecognised_value_disabled(raw):
    """The safety invariant that was asserted only in a comment.

    A True here skips nothing and runs the full suite on every labelled PR, which is the expensive
    direction. The old hand-rolled check read anything outside its set as False, and that must not
    invert -- flipping the default passed the entire suite before this test existed.
    """
    script = (
        "from redis_benchmarks_specification.__common__.env import BENCHMARK_PR_DIFF_SCOPING;"
        "print(BENCHMARK_PR_DIFF_SCOPING)"
    )
    assert _env_probe(script, BENCHMARK_PR_DIFF_SCOPING=raw) == "False"


@pytest.mark.parametrize("value", [b"\xff\xfe", bytearray(b"\xff")])
def test_an_undecodable_value_warns_and_keeps_the_offending_bytes(caplog, value):
    """Blanking it to None erased the value from the message and from the warning.

    An undecodable stream field then read identically to an absent one, which is the diagnostic the
    reader most needs. The docstring promised a warning on every fallback; it was suppressed for
    exactly the two inputs a stream reader can hit.
    """
    with caplog.at_level(logging.WARNING):
        assert parse_bool(value, default=True) is True
    messages = [
        r.getMessage()
        for r in caplog.records
        if "Unrecognised boolean" in r.getMessage()
    ]
    assert messages, "no warning for an undecodable value"
    assert "None" not in messages[0], f"offending bytes erased: {messages[0]}"


def test_an_absent_value_does_not_warn(caplog):
    """None means nothing was supplied, which is the normal case for an unset variable.

    Warning about it would add two lines of noise to every import of this module and train readers
    to ignore the warning that matters. Undecodable bytes are a different case -- something was
    supplied and could not be read -- and those do warn.
    """
    with caplog.at_level(logging.WARNING):
        assert parse_bool(None, default=True) is True
        assert parse_bool(None, default=False) is False
    assert not [
        r for r in caplog.records if "Unrecognised boolean" in r.getMessage()
    ], "warned about an absent value"


@pytest.mark.parametrize(
    "text,expected", [("false", "False"), ("0", "False"), ("true", "True")]
)
def test_the_spec_parser_also_honours_a_false_value(text, expected):
    """Covers the second file this PR edits, in a subprocess and for two reasons.

    It needs a sibling imported first, because importing __spec__ from a cold interpreter fails
    (issue #466). And having imported it, the subpackage is bound over the parent's ModuleSpec for
    the rest of the process, so Flask can no longer find an app's root path -- which breaks
    test_app.py if this runs in the same session. A subprocess gets the coverage without the
    side effect.
    """
    script = (
        "import argparse;"
        "import redis_benchmarks_specification.__common__.env;"
        "from redis_benchmarks_specification.__spec__.args import spec_cli_args;"
        "p=spec_cli_args(argparse.ArgumentParser());"
        f"print(p.parse_args(['--trigger-unstable-commits','{text}']).trigger_unstable_commits)"
    )
    assert _env_probe(script) == expected


def test_the_spec_parser_uses_the_canonical_decoder():
    """Same file, the guard rather than the behaviour. Subprocess for the same reason."""
    script = (
        "import argparse;"
        "import redis_benchmarks_specification.__common__.env as env;"
        "from redis_benchmarks_specification.__spec__.args import spec_cli_args;"
        "p=spec_cli_args(argparse.ArgumentParser());"
        "print(all(a.type is not bool for a in p._actions) and any("
        "a.type is env.parse_bool for a in p._actions))"
    )
    assert _env_probe(script) == "True"
