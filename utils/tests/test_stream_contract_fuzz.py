"""Property-based fuzzing of the stream field contract.

The unit tests in ``test_stream_contract.py`` pin specific known cases. These generate the input
space instead, because the bug class this guards against is *silent*: a payload shape nobody
thought to write a case for produces a default instead of an error, and the only symptom is a
setting that quietly stopped working.

Deliberately dependency-light and offline — no Redis, no Docker, no benchmark fleet — so it can
run on every pull request in seconds and at high example counts nightly. Example count is taken
from ``HYPOTHESIS_MAX_EXAMPLES`` so CI can dial it up without editing the file.
"""

import os

import pytest

hypothesis = pytest.importorskip(
    "hypothesis", reason="hypothesis is required for fuzz tests"
)
from hypothesis import HealthCheck, given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from redis_benchmarks_specification.__common__.stream_contract import (  # noqa: E402
    BUILDS_STREAM_FIELDS,
    COMMITS_STREAM_FIELDS,
    FIELD_ALIASES,
    read_stream_field,
)

MAX_EXAMPLES = int(os.environ.get("HYPOTHESIS_MAX_EXAMPLES", "200"))

# Suppress the too-slow health check: nightly runs use very high example counts deliberately.
FUZZ = settings(
    max_examples=MAX_EXAMPLES,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)

DECLARED = sorted(set(COMMITS_STREAM_FIELDS) | set(BUILDS_STREAM_FIELDS))

# Field names that are plausible-but-wrong: the shape the real bug took.
CONFUSABLE = st.sampled_from(
    [n for f in DECLARED for n in (f, f.replace("tests_", ""), "tests_" + f, f.upper())]
)

# Values a producer could realistically put on a stream, including hostile ones.
VALUES = st.one_of(
    st.integers(min_value=-(2**63), max_value=2**63).map(lambda i: str(i).encode()),
    st.sampled_from(
        [
            b"",
            b" ",
            b"0",
            b"00",
            b"-0",
            b"+1",
            b" 7 ",
            b"1_000",
            b"abc",
            b"1.5",
            b"None",
            b"True",
        ]
    ),
    st.binary(min_size=0, max_size=24),
    st.text(min_size=0, max_size=24).map(
        lambda s: s.encode("utf-8", "surrogatepass") if s.isprintable() else b""
    ),
)


@FUZZ
@given(payload=st.dictionaries(CONFUSABLE.map(str.encode), VALUES, max_size=12))
def test_never_raises_without_a_cast(payload):
    """Reading any field from any payload must not raise when no cast is supplied.

    A read helper that can throw on arbitrary producer input is a poison-pill risk for a
    long-running consumer, so the no-cast path must be total.
    """
    for name in DECLARED:
        value, matched = read_stream_field(payload, name)
        # no exception is the property under test; also pin the return shape
        assert matched is None or isinstance(matched, str)
        assert matched is None or value is not None


@FUZZ
@given(payload=st.dictionaries(CONFUSABLE.map(str.encode), VALUES, max_size=12))
def test_matched_name_is_always_present_in_the_payload(payload):
    """If a match is reported, that exact key must really be in the payload.

    Guards against the helper reporting a canonical name while having matched an alias, which
    would make logs claim something the wire did not carry.
    """
    for name in DECLARED:
        _, matched = read_stream_field(payload, name)
        if matched is not None:
            assert matched.encode() in payload or matched in payload


@FUZZ
@given(
    name=st.sampled_from(sorted(FIELD_ALIASES)),
    canonical_value=st.integers(0, 10**6),
    alias_value=st.integers(0, 10**6),
)
def test_canonical_wins_over_alias_deterministically(
    name, canonical_value, alias_value
):
    """When both spellings are present, the canonical one must win — every time.

    Ambiguous precedence here would make behaviour depend on dict ordering, which is exactly the
    kind of non-determinism that makes a benchmark harness untrustworthy.
    """
    for alias in FIELD_ALIASES[name]:
        payload = {
            name.encode(): str(canonical_value).encode(),
            alias.encode(): str(alias_value).encode(),
        }
        value, matched = read_stream_field(payload, name, cast=int)
        assert matched == name
        assert value == canonical_value


@FUZZ
@given(
    name=st.sampled_from(DECLARED),
    default=st.one_of(st.none(), st.integers(), st.text(max_size=8)),
    other=st.dictionaries(
        st.sampled_from(DECLARED).map(str.encode), VALUES, max_size=6
    ),
)
def test_absent_field_yields_the_default_and_no_match(name, default, other):
    """Absence must be reported as absence, never as a value.

    ``matched is None`` is what lets a caller distinguish "producer said 0" from "producer said
    nothing" — the distinction the original bug erased.
    """
    payload = {k: v for k, v in other.items() if k != name.encode()}
    got, matched = read_stream_field(payload, name, default=default, cast=None)
    if name.encode() not in payload and name not in payload:
        assert matched is None
        assert got == default


@FUZZ
@given(name=st.sampled_from(DECLARED), value=VALUES)
def test_the_cast_path_is_total_too(name, value):
    """The shape both production call sites actually use, which nothing exercised.

    Every caller in the package passes ``cast=int``, and the hostile ``VALUES`` above was only ever
    combined with the no-cast path -- so the one property that mattered at the only two real callers
    was the one not stated. A raising cast is worse than a wrong value here: the enclosing handler
    logs CRITICAL and the caller then ACKs unconditionally, so the whole benchmark run is discarded
    and never retried.
    """
    got, matched = read_stream_field({name.encode(): value}, name, cast=int)
    if matched is not None:
        assert isinstance(got, int)
    else:
        # reported as absent, which is how a caller's `is not None` check degrades to its default
        assert got is None


@FUZZ
@given(name=st.sampled_from(DECLARED), value=VALUES, default=st.integers())
def test_a_failing_cast_yields_the_callers_default(name, value, default):
    """A malformed value must be indistinguishable from a missing one, by design.

    That is what lets the coordinator keep its own configured limit instead of dropping the run.
    """
    got, matched = read_stream_field(
        {name.encode(): value}, name, default=default, cast=int
    )
    assert matched is not None or got == default


@FUZZ
@given(
    name=st.sampled_from(
        sorted(set(FIELD_ALIASES) | {"nonsense", "priority_upper_limit"})
    )
)
def test_an_undeclared_name_is_rejected_rather_than_silently_never_matching(name):
    """Reading by a name no producer writes is a programming error, not a miss.

    A source-text guard cannot cover this -- a single-quoted literal, a keyword argument, a module
    constant, an f-string and a subscripted first argument all defeat any regex while still
    compiling, and each one reinstates the original inert read with CI green. Enforced at runtime
    instead, where the spelling used at the call site cannot hide it.
    """
    import pytest as _pytest

    from redis_benchmarks_specification.__common__.stream_contract import (
        BUILDS_STREAM_FIELDS as _B,
    )
    from redis_benchmarks_specification.__common__.stream_contract import (
        COMMITS_STREAM_FIELDS as _C,
    )

    if name in _C or name in _B:
        read_stream_field({}, name)  # declared: must not raise
    else:
        with _pytest.raises(KeyError):
            read_stream_field({}, name)


@FUZZ
@given(name=st.sampled_from(DECLARED), value=VALUES)
def test_a_key_on_the_wire_is_always_reported_as_matched(name, value):
    """The converse direction, which nothing asserted.

    Every other property checks matched-implies-present. Without present-implies-matched the whole
    lookup can be deleted and the suite stays green: returning ``(default, None)`` unconditionally
    satisfies all of them.
    """
    got, matched = read_stream_field({name.encode(): value}, name)
    assert matched == name, f"key was on the wire but reported absent: {value!r}"
    assert got is not None


@FUZZ
@given(alias_name=st.sampled_from(sorted(FIELD_ALIASES)), value=st.integers(0, 10**6))
def test_an_alias_only_payload_still_matches_and_says_which_spelling_arrived(
    alias_name, value
):
    """Deleting the alias lookup entirely passed all six original properties.

    That lookup is the module's stated compatibility contract -- honouring entries already in flight
    from an older pinned release -- so it should not be silently removable. The reported name must be
    the alias, not the canonical one, or the log claims something the wire did not carry.
    """
    for alias in FIELD_ALIASES[alias_name]:
        got, matched = read_stream_field(
            {alias.encode(): str(value).encode()}, alias_name, cast=int
        )
        assert got == value
        assert matched == alias


@FUZZ
@given(name=st.sampled_from(DECLARED), value=st.binary(min_size=0, max_size=24))
def test_a_decodable_value_is_returned_byte_for_byte(name, value):
    """No silent normalisation on the way through.

    Adding a .strip() inside the helper passed every original property, and would quietly change
    what a producer wrote -- including turning b" " into an empty string.
    """
    try:
        expected = value.decode()
    except UnicodeDecodeError:
        return  # lossy decode is covered separately
    got, matched = read_stream_field({name.encode(): value}, name)
    assert (got, matched) == (expected, name)


@FUZZ
@given(name=st.sampled_from(DECLARED), default=st.text(min_size=1, max_size=6))
def test_an_empty_value_is_a_value_not_an_absence(name, default):
    """b"" means the producer wrote an empty string, which is not the same as writing nothing.

    Collapsing the two reintroduces exactly the present-versus-absent conflation this module exists
    to prevent, and it survived every original property.
    """
    got, matched = read_stream_field({name.encode(): b""}, name, default=default)
    assert matched == name
    assert got == ""


@FUZZ
@given(name=st.sampled_from(DECLARED))
def test_a_bytes_key_takes_precedence_over_a_str_key(name):
    """Pins the precedence rather than leaving it to whichever loop order happens to be written.

    Cannot arise from one redis-py client, but the order was unpinned and flipping it left both
    suites green -- so nothing recorded which value a mixed payload should yield.
    """
    got, matched = read_stream_field({name.encode(): b"1", name: "2"}, name, cast=int)
    assert (got, matched) == (1, name)
