import glob
import json
import os

import yaml

from redis_benchmarks_specification.__common__ import scope as scope_mod
from redis_benchmarks_specification.__common__.scope import (
    load_files_to_groups,
    load_core_specs,
    scope_fields_from_changed_files,
    get_pr_changed_files,
    compute_pr_scope_fields,
)

F2G = load_files_to_groups()
CORE = load_core_specs()

_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(scope_mod.__file__), "..", "..")
)
_SUITES = os.path.join(_REPO_ROOT, "redis_benchmarks_specification", "test-suites")


def _spec_stems_and_groups():
    out = []
    for f in glob.glob(os.path.join(_SUITES, "*.yml")):
        try:
            d = yaml.safe_load(open(f))
        except Exception:
            continue
        if isinstance(d, dict):
            out.append(
                (os.path.basename(f)[: -len(".yml")], d.get("tested-groups") or [])
            )
    return out


# ----------------------------- mapping logic -------------------------------------------


def test_single_type_file_scopes_to_one_anchored_group():
    assert scope_fields_from_changed_files(["src/t_hash.c"], F2G, CORE) == {
        "tests_groups_regexp": "^(?:hash)$"
    }


def test_multiple_type_files_union_anchored():
    f = scope_fields_from_changed_files(["src/t_hash.c", "src/t_zset.c"], F2G, CORE)
    assert f == {"tests_groups_regexp": "^(?:hash|sorted\\-set)$"}


def test_anchoring_prevents_set_matching_sorted_set():
    # the whole reason for anchoring: a 'set' scope must NOT select 'sorted-set'
    import re

    rx = re.compile(
        scope_fields_from_changed_files(["src/t_set.c"], F2G, CORE)[
            "tests_groups_regexp"
        ]
    )
    assert re.search(rx, "set")
    assert not re.search(rx, "sorted-set")
    assert not re.search(rx, "hset")


def test_geo_maps_to_geo_and_sorted_set():
    # geo.c calls into the zset machinery, so it must widen to sorted-set too
    f = scope_fields_from_changed_files(["src/geo.c"], F2G, CORE)
    assert f == {"tests_groups_regexp": "^(?:geo|sorted\\-set)$"}


def test_array_file_is_mapped():
    assert scope_fields_from_changed_files(["src/t_array.c"], F2G, CORE) == {
        "tests_groups_regexp": "^(?:array)$"
    }


def test_non_source_changes_ignored():
    f = scope_fields_from_changed_files(
        ["src/t_hash.c", "tests/unit/type/hash.tcl", ".github/workflows/ci.yml"],
        F2G,
        CORE,
    )
    assert f == {"tests_groups_regexp": "^(?:hash)$"}


def test_broad_file_falls_back_to_core_set():
    f = scope_fields_from_changed_files(["src/networking.c"], F2G, CORE)
    assert "tests_regexp" in f and f["tests_regexp"].startswith("^(?:")
    # core set, not a group filter
    assert "tests_groups_regexp" not in f


def test_mixed_type_and_broad_widens_to_core_set():
    f = scope_fields_from_changed_files(["src/t_hash.c", "src/server.c"], F2G, CORE)
    assert "tests_regexp" in f and "tests_groups_regexp" not in f


def test_unmapped_source_file_falls_back_to_core_set():
    f = scope_fields_from_changed_files(["src/quicklist.c"], F2G, CORE)
    assert "tests_regexp" in f


def test_header_change_is_broad():
    f = scope_fields_from_changed_files(["src/server.h"], F2G, CORE)
    assert "tests_regexp" in f


def test_docs_only_pr_gets_core_set():
    assert "tests_regexp" in scope_fields_from_changed_files(["README.md"], F2G, CORE)


def test_empty_diff_returns_full_suite():
    assert scope_fields_from_changed_files([], F2G, CORE) == {}


def test_empty_core_list_degrades_to_full_suite():
    # if the core list is unavailable, a broad diff must run the full suite, never an
    # empty tests_regexp that would silently run zero benchmarks
    assert scope_fields_from_changed_files(["src/server.c"], F2G, []) == {}


def test_empty_map_degrades_to_full_or_core_never_zero():
    # nothing mapped + a core list -> core set (not an empty groups regex)
    f = scope_fields_from_changed_files(["src/t_hash.c"], {}, CORE)
    assert "tests_regexp" in f


# ----------------------------- drift / corpus guards -----------------------------------


def test_every_mapped_group_exists_in_groups_json():
    valid = set(json.load(open(os.path.join(_REPO_ROOT, "groups.json"))))
    for base, groups in F2G.items():
        for g in groups:
            assert g in valid, "%s -> unknown group %r (not in groups.json)" % (base, g)


def test_every_mapped_group_matches_at_least_one_spec():
    spec_groups = set(g for _, gs in _spec_stems_and_groups() for g in gs)
    for base, groups in F2G.items():
        for g in groups:
            assert g in spec_groups, "%s -> group %r matches zero specs" % (base, g)


def test_every_core_spec_exists_in_the_suite():
    stems = {s for s, _ in _spec_stems_and_groups()}
    for name in CORE:
        assert name in stems, "core spec %r not found in test-suites/" % name


def test_core_specs_cover_every_group_that_has_specs():
    by_stem = dict(_spec_stems_and_groups())
    covered = set(g for name in CORE for g in by_stem.get(name, []))
    all_spec_groups = set(g for _, gs in _spec_stems_and_groups() for g in gs)
    missing = all_spec_groups - covered
    assert not missing, "core set does not cover groups: %s" % sorted(missing)


# ----------------------------- fetch / safety paths ------------------------------------


def test_get_pr_changed_files_returns_none_on_error(monkeypatch):
    class Boom:
        def __init__(self, *a, **k):
            raise RuntimeError("api down")

    monkeypatch.setattr("github.Github", Boom, raising=False)
    assert get_pr_changed_files("tok", "redis", "redis", 1) is None


def test_large_pr_falls_back_to_full_suite(monkeypatch):
    calls = {"get_files": 0}

    class FakePull:
        changed_files = 500

        def get_files(self):  # must not be called for an over-cap PR
            calls["get_files"] += 1
            return []

    class FakeRepo:
        def get_pull(self, n):
            return FakePull()

    class FakeGH:
        def __init__(self, *a, **k):
            pass

        def get_repo(self, slug, lazy=False):  # production passes lazy=True
            return FakeRepo()

    monkeypatch.setattr("github.Github", FakeGH, raising=False)
    assert get_pr_changed_files("tok", "redis", "redis", 1, max_files=100) is None
    # load-bearing: prove we returned via the cap, not via a swallowed error
    assert calls["get_files"] == 0


def test_compute_full_suite_when_files_unavailable(monkeypatch):
    monkeypatch.setattr(scope_mod, "get_pr_changed_files", lambda *a, **k: None)
    fields, msg = compute_pr_scope_fields("tok", "redis", "redis", 123)
    assert fields == {}
    assert "full suite" in msg


def test_compute_scopes_from_diff(monkeypatch):
    monkeypatch.setattr(
        scope_mod, "get_pr_changed_files", lambda *a, **k: ["src/t_zset.c"]
    )
    fields, _ = compute_pr_scope_fields("tok", "redis", "redis", 123)
    assert fields == {"tests_groups_regexp": "^(?:sorted\\-set)$"}


def test_compute_never_raises_on_internal_error(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("unexpected")

    monkeypatch.setattr(scope_mod, "get_pr_changed_files", boom)
    fields, msg = compute_pr_scope_fields("tok", "redis", "redis", 123)
    assert fields == {} and "full suite" in msg


def test_load_normalizes_scalar_group_value(tmp_path):
    p = tmp_path / "f2g.json"
    p.write_text('{"exact": {"t_foo.c": "string"}}')
    assert load_files_to_groups(str(p)) == {"t_foo.c": ["string"]}
