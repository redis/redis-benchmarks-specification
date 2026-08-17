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
        [
            "git",
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            sub_dir,
            "subdir",
        ],
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
    ), "submodule content missing from recurse_submodules=True archive: {}".format(
        names
    )


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
    ), "submodule content leaked into the default (non-recursive) archive: {}".format(
        names
    )


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


def test_recurse_submodules_reused_clone_reflects_each_commit(tmp_path, monkeypatch):
    """A single local_repo_path is reused across every commit in a multi-hash trigger
    batch (one clone, checked out in a loop) -- the second call on the same clone must
    reflect the SECOND commit's tree, not stale content left behind by the first call's
    checkout+submodule-update (the gap the missing `git clean` used to leave open)."""
    parent_dir, head1 = _make_fixture(tmp_path)
    monkeypatch.setenv("GIT_ALLOW_PROTOCOL", "file")
    env = dict(os.environ)
    env["GIT_ALLOW_PROTOCOL"] = "file"

    # Second commit: add a new top-level file, independent of the submodule.
    with open(os.path.join(parent_dir, "second_file.txt"), "w") as f:
        f.write("second commit content\n")
    _run(["git", "add", "second_file.txt"], cwd=parent_dir, env=env)
    _run(["git", "commit", "-q", "-m", "second"], cwd=parent_dir, env=env)
    head2 = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=parent_dir,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    ).stdout.strip()
    assert head2 != head1

    result1 = get_archive_zip_from_hash(
        "testorg",
        "testrepo",
        head1,
        {},
        local_repo_path=parent_dir,
        recurse_submodules=True,
    )
    result2 = get_archive_zip_from_hash(
        "testorg",
        "testrepo",
        head2,
        {},
        local_repo_path=parent_dir,
        recurse_submodules=True,
    )

    assert result1[0] is True and result2[0] is True

    names1 = zipfile.ZipFile(io.BytesIO(result1[2])).namelist()
    names2 = zipfile.ZipFile(io.BytesIO(result2[2])).namelist()
    prefix1 = "testrepo-{}/".format(head1)
    prefix2 = "testrepo-{}/".format(head2)

    assert not any(n.endswith("second_file.txt") for n in names1), (
        "reused clone leaked the SECOND commit's file into the FIRST commit's "
        "archive: {}".format(names1)
    )
    assert prefix2 + "second_file.txt" in names2
    assert prefix2 + "subdir/sub_file.txt" in names2, (
        "reused clone lost submodule content when moving to the second "
        "commit: {}".format(names2)
    )


def test_recurse_submodules_broken_submodule_fails_loudly(tmp_path, monkeypatch):
    """If `git submodule update --init --recursive` cannot succeed (e.g. the submodule's
    remote is gone), the call must return result=False with a populated error_msg --
    never silently produce a zip missing the submodule while still claiming success."""
    parent_dir, head = _make_fixture(tmp_path)
    monkeypatch.setenv("GIT_ALLOW_PROTOCOL", "file")

    # Delete the submodule's source repo out from under the parent so
    # `submodule update --init` has nothing to clone/fetch from.
    import shutil

    shutil.rmtree(str(tmp_path / "sub"))

    # Force a fresh clone of `parent_dir` so the submodule isn't already
    # materialized on disk from `_make_fixture`'s own `submodule add` step.
    fresh_parent = str(tmp_path / "parent_fresh")
    env = dict(os.environ)
    env["GIT_ALLOW_PROTOCOL"] = "file"
    _run(
        [
            "git",
            "-c",
            "protocol.file.allow=always",
            "clone",
            "-q",
            parent_dir,
            fresh_parent,
        ],
        cwd=str(tmp_path),
        env=env,
    )

    result, bin_key, binary_value, error_msg = get_archive_zip_from_hash(
        "testorg",
        "testrepo",
        head,
        {},
        local_repo_path=fresh_parent,
        recurse_submodules=True,
    )

    assert result is False
    assert error_msg is not None
    assert binary_value is None


def test_recurse_submodules_tracked_symlink_preserved(tmp_path, monkeypatch):
    """A tracked symlink must round-trip through the zip as a symlink (git-style
    encoding: content = link target, external_attr carries S_IFLNK) -- not be silently
    dropped (dangling target) or silently replaced by its target's file content under
    the link's name (both of which os.path.isfile()/zf.write() would do unguarded)."""
    import stat

    parent_dir, head = _make_fixture(tmp_path)
    monkeypatch.setenv("GIT_ALLOW_PROTOCOL", "file")
    env = dict(os.environ)
    env["GIT_ALLOW_PROTOCOL"] = "file"

    os.symlink("top_file.txt", os.path.join(parent_dir, "top_file_link.txt"))
    _run(["git", "add", "top_file_link.txt"], cwd=parent_dir, env=env)
    _run(["git", "commit", "-q", "-m", "add symlink"], cwd=parent_dir, env=env)
    head_with_link = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=parent_dir,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    ).stdout.strip()

    result, bin_key, binary_value, error_msg = get_archive_zip_from_hash(
        "testorg",
        "testrepo",
        head_with_link,
        {},
        local_repo_path=parent_dir,
        recurse_submodules=True,
    )

    assert error_msg is None
    assert result is True

    zf = zipfile.ZipFile(io.BytesIO(binary_value))
    prefix = "testrepo-{}/".format(head_with_link)
    info = zf.getinfo(prefix + "top_file_link.txt")
    assert stat.S_ISLNK(info.external_attr >> 16), (
        "symlink lost its S_IFLNK bit in the archive -- would extract as a "
        "regular file containing the literal target path"
    )
    assert zf.read(info).decode("utf-8") == "top_file.txt"
