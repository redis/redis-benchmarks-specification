"""Guard the producer/consumer field contract on the commits and builds streams.

These tests are cheap (no Redis, no Docker, no fleet) and exist to catch a specific silent
failure: a consumer reading a stream field name that no producer writes. The ``in testDetails``
idiom makes that a no-op fallback to a default rather than an error, so the only symptom is a
setting that quietly stops working.

The real instance this was written for: the builder writes ``tests_priority_upper_limit`` while
the coordinator read ``priority_upper_limit``, so ``--tests-priority-upper-limit`` never took
effect on any run.
"""

import pathlib
import re

from redis_benchmarks_specification.__common__.stream_contract import (
    BUILDS_STREAM_FIELDS,
    COMMITS_STREAM_FIELDS,
    FIELD_ALIASES,
    KNOWN_UNPRODUCED_CONSUMED_FIELDS,
    read_stream_field,
)

PKG = pathlib.Path(__file__).resolve().parents[2] / "redis_benchmarks_specification"

# `b"name" in testDetails` and `testDetails[b"name"]`
CONSUME_RE = re.compile(
    r'b"([a-z0-9_]+)"\s*in\s+testDetails|testDetails\[\s*b"([a-z0-9_]+)"\s*\]'
)


def _consumer_reads():
    """Map source file -> set of stream field names read directly off a stream entry."""
    found = {}
    for path in PKG.rglob("*.py"):
        if "/tests/" in path.as_posix() or path.name == "stream_contract.py":
            continue  # the declaration itself names fields in prose, not as reads
        names = set()
        for m in CONSUME_RE.finditer(
            path.read_text(encoding="utf-8", errors="replace")
        ):
            names.add(m.group(1) or m.group(2))
        if names:
            found[path.relative_to(PKG).as_posix()] = names
    return found


def _declared():
    declared = set(COMMITS_STREAM_FIELDS) | set(BUILDS_STREAM_FIELDS)
    for canonical, aliases in FIELD_ALIASES.items():
        declared.add(canonical)
        declared.update(aliases)
    return declared


def test_every_consumed_field_is_declared():
    """A field read off a stream must be declared in the contract.

    If this fails you either (a) misspelled a field name, or (b) added a legitimate new field and
    forgot to declare it in ``stream_contract.py``. Both are worth failing for: (b) is how (a)
    stops being detectable.
    """
    declared = _declared()
    undeclared = {
        f: sorted(names - declared)
        for f, names in _consumer_reads().items()
        if names - declared
    }
    assert not undeclared, (
        "stream fields are read but not declared in stream_contract.py: "
        f"{undeclared}. Declare them, or fix the spelling."
    )


# Names of the dicts that become stream payloads.
#
# `fields` is deliberately NOT global: __common__/builder_schema.py uses it for the
# commits-stream entry, but __self_contained_coordinator__ also uses `fields` for the runner
# *heartbeat* hash — a different destination entirely. Treating `fields` as a payload everywhere
# wrongly reports heartbeat-only keys (docker_air_gap, platform) as stream-produced. So it is
# honoured only in the file where it really is a stream payload. If that collision is ever
# cleaned up upstream (renaming the heartbeat dict), this special case can go.
# fields_before / fields_after are the API's commits-stream payloads (__api__/app.py).
# The suffixed form cannot collide with the bare heartbeat `fields`.
PAYLOAD_DICTS = r"build_stream_fields|commit_dict|new_fields|fields_[a-z_]+"
PAYLOAD_DICTS_BY_FILE = {
    "builder_schema.py": r"build_stream_fields|commit_dict|new_fields|fields_[a-z_]+|fields"
}
PRODUCE_RE = re.compile(rf'(?:{PAYLOAD_DICTS})\[\s*"([a-z0-9_]+)"\s*\]\s*=')
PRODUCE_LIT_RE = re.compile(r'^\s*"([a-z0-9_]+)"\s*:', re.M)


def _producer_writes():
    """Field names written into any stream payload dict, anywhere in the package."""
    written = set()
    for path in PKG.rglob("*.py"):
        if "/tests/" in path.as_posix() or path.name == "stream_contract.py":
            continue
        src = path.read_text(encoding="utf-8", errors="replace")
        dicts = PAYLOAD_DICTS_BY_FILE.get(path.name, PAYLOAD_DICTS)
        written |= set(
            re.findall("(?:" + dicts + r')\[\s*"([a-z0-9_]+)"\s*\]\s*=', src)
        )
        # dict-literal payloads, e.g. `build_stream_fields = { "git_hash": ..., }`
        block_re = "(?:" + dicts + r")\s*=\s*\{(.*?)\n\s*\}"
        for blk in re.finditer(block_re, src, re.S):
            written |= {m.group(1) for m in PRODUCE_LIT_RE.finditer(blk.group(1))}
        # NOTE: deliberately no `"x": x,` mirror heuristic here. It matched the runner
        # *heartbeat* hash, which is not a stream payload, and so wrongly reported
        # docker_air_gap as produced. Only dicts that actually become stream entries count.
    return written


def test_unproduced_consumed_fields_match_the_recorded_set():
    """Ratchet on consumed-but-never-written fields.

    A field read off a stream that no producer writes is unreachable: the ``in testDetails``
    check is always False and the consumer silently uses its default. That is exactly how the
    priority cap stayed inert. Known instances are recorded in
    ``KNOWN_UNPRODUCED_CONSUMED_FIELDS`` with an explanation; this test asserts the real set
    matches, so a NEW orphan fails CI and a FIXED one must be removed from the record.

    On failure, near-miss candidates are reported — a consumed name that closely resembles a
    produced name is almost always drift rather than a genuinely new field. That heuristic is
    only applied to *undeclared* names: legitimately-coexisting pairs such as
    ``arch``/``build_arch`` and ``compiler``/``cpp_compiler`` are real distinct fields and must
    not be flagged.
    """
    produced = _producer_writes()
    consumed = set().union(*_consumer_reads().values())
    orphans = consumed - produced

    hints = {}
    for o in sorted(orphans - KNOWN_UNPRODUCED_CONSUMED_FIELDS):
        near = sorted(p for p in produced if p.endswith(o) or o.endswith(p))
        if near:
            hints[o] = near

    assert orphans == set(KNOWN_UNPRODUCED_CONSUMED_FIELDS), (
        f"consumed-but-unproduced stream fields changed.\n"
        f"  now:      {sorted(orphans)}\n"
        f"  recorded: {sorted(KNOWN_UNPRODUCED_CONSUMED_FIELDS)}\n"
        f"  likely name drift (consumed name resembles a produced one): {hints}\n"
        "If you fixed one, remove it from KNOWN_UNPRODUCED_CONSUMED_FIELDS. If you added one, "
        "either write it in a producer or read the name the producer actually writes."
    )


def test_alias_pairs_are_genuinely_near_misses():
    """Every declared alias should be the near-miss it exists to absorb.

    Guards against FIELD_ALIASES quietly becoming a dumping ground for unrelated names.
    """
    for canonical, aliases in FIELD_ALIASES.items():
        for alias in aliases:
            assert canonical.endswith(alias) or alias.endswith(canonical), (
                f"alias {alias!r} for {canonical!r} is not a prefix/suffix variant; "
                "FIELD_ALIASES is for absorbing name drift, not for unrelated synonyms"
            )


def test_priority_limit_is_read_from_the_builder_spelling():
    """Regression test for the inert priority cap.

    The builder writes ``tests_priority_upper_limit``; assert the reader honours exactly that,
    since reading the un-prefixed spelling is what made the cap inert.
    """
    entry = {b"tests_priority_upper_limit": b"7", b"tests_priority_lower_limit": b"2"}
    upper, matched_upper = read_stream_field(
        entry, "tests_priority_upper_limit", cast=int
    )
    lower, matched_lower = read_stream_field(
        entry, "tests_priority_lower_limit", cast=int
    )
    assert (upper, matched_upper) == (7, "tests_priority_upper_limit")
    assert (lower, matched_lower) == (2, "tests_priority_lower_limit")


def test_priority_limit_alias_is_still_honoured():
    """In-flight entries and older producers using the un-prefixed spelling must still work."""
    entry = {b"priority_upper_limit": b"9"}
    value, matched = read_stream_field(entry, "tests_priority_upper_limit", cast=int)
    assert value == 9
    # the matched name is surfaced so an alias hit is visible in the logs, not silently equivalent
    assert matched == "priority_upper_limit"


def test_absent_field_returns_default_and_no_match():
    value, matched = read_stream_field(
        {}, "tests_priority_upper_limit", default=123, cast=int
    )
    assert value == 123
    assert matched is None


def test_str_keyed_entries_are_supported():
    """redis-py returns bytes keys, but decode_responses=True clients return str."""
    value, matched = read_stream_field(
        {"tests_priority_upper_limit": "5"}, "tests_priority_upper_limit", cast=int
    )
    assert (value, matched) == (5, "tests_priority_upper_limit")


def test_builder_writes_the_declared_priority_field():
    """Pin the producer side too, so a rename there fails here rather than in production."""
    builder = (PKG / "__builder__" / "builder.py").read_text(encoding="utf-8")
    assert '"tests_priority_upper_limit": tests_priority_upper_limit' in builder
    assert '"tests_priority_lower_limit": tests_priority_lower_limit' in builder


READ_CALL_RE = re.compile(
    r'read_stream_field\(\s*[A-Za-z_][A-Za-z0-9_]*\s*,\s*"([a-z0-9_]+)"'
)


def test_read_stream_field_callers_use_canonical_names():
    """Call sites must pass the CANONICAL field name, never an alias.

    ``read_stream_field`` resolves aliases in the *arriving* payload — that is its job. But if a
    caller passes an alias as the name to look up, alias resolution does not help: the lookup
    starts from a name no producer writes, and ``FIELD_ALIASES`` has no reverse entry, so the
    default is silently used again. That is the original bug, reintroduced through the new API.

    This test exists because a mutation test proved the earlier checks did NOT catch it: rewriting
    the coordinator's call from ``tests_priority_upper_limit`` to ``priority_upper_limit`` left the
    suite fully green while the cap went inert again.
    """
    canonical = set(COMMITS_STREAM_FIELDS) | set(BUILDS_STREAM_FIELDS)
    alias_only = {
        alias
        for aliases in FIELD_ALIASES.values()
        for alias in aliases
        if alias not in canonical
    }

    offenders = {}
    for path in PKG.rglob("*.py"):
        if "/tests/" in path.as_posix() or path.name == "stream_contract.py":
            continue
        src = path.read_text(encoding="utf-8", errors="replace")
        for name in READ_CALL_RE.findall(src):
            if name in alias_only:
                offenders.setdefault(path.relative_to(PKG).as_posix(), []).append(
                    f"{name!r} is an alias; pass the canonical name instead"
                )
            elif name not in canonical:
                offenders.setdefault(path.relative_to(PKG).as_posix(), []).append(
                    f"{name!r} is not a declared stream field"
                )

    assert (
        not offenders
    ), f"read_stream_field() called with non-canonical names: {offenders}"
