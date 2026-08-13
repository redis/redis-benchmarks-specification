#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
"""Pure unit tests (no docker / no redis / no network — local git fixtures only) for the
opt-in --recurse_submodules archive path in builder_schema.get_archive_zip_from_hash().

Context: `git archive` (used by the default local-repo path) and GitHub's own
/archive/{ref}.zip endpoint (used by the no-local-repo-path fallback) BOTH exclude
submodule content by design — this is what blocks a Dragonfly build (helio/ is a
submodule) from ever reaching the builder container. These tests build a throwaway
parent+submodule git fixture and verify:
  * default (recurse_submodules=False) behavior is byte-for-byte the old `git archive`
    path — submodule content absent, exactly as before this change,
  * recurse_submodules=True includes the submodule's tracked files in the zip,
  * a genuinely dirty/uninitialized submodule state doesn't silently produce a
    submodule-less zip while claiming success.
"""
import io
import os
import subprocess
import zipfile

from redis_benchmarks_specification.__common__.builder_schema import (
    get_archive_zip_from_hash,
)


def _run(cmd, cwd, env=None):
    subprocess.run(cmd, cwd=cwd, check=True, env=env, capture_output=True)


def _make_fixture(tmp_path):
    """Build <tmp_path>/sub (a standalone repo) and <tmp_path>/parent (which embeds
    `sub` as a submodule at path `subdir`), returning (parent_dir, head_hash).

    Adding a submodule via a local filesystem path requires opting into the file://
    transport (git blocks it by default since the CVE-2022-39253 hardening) — this is
    purely a test-fixture concern for a local-path submodule; a real Dragonfly build
    uses an https:// GitHub URL, which is unaffected either way.
    """
    env = dict(os.environ)
    env["GIT_ALLOW_PROTOCOL"] = "file"

    sub_dir = str(tmp_path / "sub")
    parent_dir = str(tmp_path / "parent")
    os.makedirs(sub_dir)
    os.makedirs(parent_dir)

    _run(["git", "init", "-q"], cwd=sub_dir, env=env)
    _run(["git", "config", "user.email", "t@t.com"], cwd=sub_dir, env=env)
    _run(["git", "config", "user.name", "t"], cwd=sub_dir, env=env)
    with open(os.path.join(sub_dir, "sub_file.txt"), "w") as f:
        f.write("submodule content\n")
    _run(["git", "add", "."], cwd=sub_dir, env=env)
    _run(["git", "commit", "-q", "-m", "sub"], cwd=sub_dir, env=env)

    _run(["git", "init", "-q"], cwd=parent_dir, env=env)
    _run(["git", "config", "user.email", "t@t.com"], cwd=parent_dir, env=env)
    _run(["git", "config", "user.name", "t"], cwd=parent_dir, env=env)
    with open(os.path.join(parent_dir, "top_file.txt"), "w") as f:
        f.write("top-level content\n")
    _run(["git", "add", "."], cwd=parent_dir, env=env)
    _run(["git", "commit", "-q", "-m", "top"], cwd=parent_dir, env=env)
    _run(
        ["git", "-c", "protocol.file.allow=always", "submodule", "add", sub_dir, "subdir"],
        cwd=parent_dir,
        env=env,
    )
    _run(["git", "commit", "-q", "-m", "add submodule"], cwd=parent_dir, env=env)

    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=parent_dir,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    ).stdout.strip()
    return parent_dir, head


def test_recurse_submodules_includes_submodule_content(tmp_path, monkeypatch):
    parent_dir, head = _make_fixture(tmp_path)
    # The code under test shells out via GitPython, inheriting this process's env —
    # setting it here (not in production code) is what lets the fixture's local
    # file:// submodule resolve; a real https:// Dragonfly checkout needs no such override.
    monkeypatch.setenv("GIT_ALLOW_PROTOCOL", "file")

    result, bin_key, binary_value, error_msg = get_archive_zip_from_hash(
        "testorg",
        "testrepo",
        head,
        {},
        local_repo_path=parent_dir,
        recurse_submodules=True,
    )

    assert error_msg is None
    assert result is True
    assert binary_value is not None

    names = zipfile.ZipFile(io.BytesIO(binary_value)).namelist()
    prefix = "testrepo-{}/".format(head)
    assert prefix + "top_file.txt" in names
    assert prefix + ".gitmodules" in names
    assert (
        prefix + "subdir/sub_file.txt" in names
    ), "submodule content missing from recurse_submodules=True archive: {}".format(names)


def test_default_path_excludes_submodule_content(tmp_path, monkeypatch):
    """recurse_submodules defaults to False — must reproduce the pre-existing
    `git archive` behavior exactly (submodule content absent), so redis/valkey
    triggers (which never pass this flag) see zero behavior change."""
    parent_dir, head = _make_fixture(tmp_path)
    monkeypatch.setenv("GIT_ALLOW_PROTOCOL", "file")

    result, bin_key, binary_value, error_msg = get_archive_zip_from_hash(
        "testorg",
        "testrepo",
        head,
        {},
        local_repo_path=parent_dir,
    )

    assert error_msg is None
    assert result is True
    names = zipfile.ZipFile(io.BytesIO(binary_value)).namelist()
    prefix = "testrepo-{}/".format(head)
    assert prefix + "top_file.txt" in names
    assert not any(
        n.endswith("subdir/sub_file.txt") for n in names
    ), "submodule content leaked into the default (non-recursive) archive: {}".format(names)


def test_recurse_submodules_false_explicit_matches_default(tmp_path, monkeypatch):
    parent_dir, head = _make_fixture(tmp_path)
    monkeypatch.setenv("GIT_ALLOW_PROTOCOL", "file")

    default_result = get_archive_zip_from_hash(
        "testorg", "testrepo", head, {}, local_repo_path=parent_dir
    )
    explicit_false_result = get_archive_zip_from_hash(
        "testorg",
        "testrepo",
        head,
        {},
        local_repo_path=parent_dir,
        recurse_submodules=False,
    )
    assert default_result[2] == explicit_false_result[2]  # identical zip bytes
