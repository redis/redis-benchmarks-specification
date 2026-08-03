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

hypothesis = pytest.importorskip("hypothesis", reason="hypothesis is required for fuzz tests")
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
    st.sampled_from([b"", b" ", b"0", b"00", b"-0", b"+1", b" 7 ", b"1_000", b"abc", b"1.5", b"None", b"True"]),
    st.binary(min_size=0, max_size=24),
    st.text(min_size=0, max_size=24).map(lambda s: s.encode("utf-8", "surrogatepass") if s.isprintable() else b""),
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
def test_canonical_wins_over_alias_deterministically(name, canonical_value, alias_value):
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
@given(name=st.sampled_from(DECLARED), value=st.integers(-(2**40), 2**40))
def test_int_round_trip(name, value):
    """A producer-written integer must survive the read unchanged, bytes- or str-keyed."""
    for payload in (
        {name.encode(): str(value).encode()},
        {name: str(value)},
    ):
        got, matched = read_stream_field(payload, name, cast=int)
        assert (got, matched) == (value, name)


@FUZZ
@given(
    name=st.sampled_from(DECLARED),
    default=st.one_of(st.none(), st.integers(), st.text(max_size=8)),
    other=st.dictionaries(st.sampled_from(DECLARED).map(str.encode), VALUES, max_size=6),
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
@given(payload=st.dictionaries(st.binary(min_size=1, max_size=20), VALUES, max_size=10))
def test_unrelated_payloads_never_produce_a_match(payload):
    """Random keys must not accidentally satisfy a declared field lookup."""
    for name in DECLARED:
        if name.encode() in payload or name in payload:
            continue
        for alias in FIELD_ALIASES.get(name, ()):
            if alias.encode() in payload or alias in payload:
                break
        else:
            _, matched = read_stream_field(payload, name)
            assert matched is None
