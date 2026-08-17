#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import argparse
import os
import git

from redis_benchmarks_specification.__cli__.args import spec_cli_args
from redis_benchmarks_specification.__cli__.cli import (
    trigger_tests_cli_command_logic,
    get_commits_by_branch,
    get_commits_by_tags,
    get_repo,
    resolve_local_repo_path,
)


def test_run_local_command_logic_oss_cluster():
    # should error due to missing --use-tags or --use-branch
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = spec_cli_args(parser)
    args = parser.parse_args(args=[])
    try:
        trigger_tests_cli_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 1
    db_port = os.getenv("DATASINK_PORT", "6379")

    # should error due to missing --use-tags or --use-branch
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = spec_cli_args(parser)
    TST_REDIS_DIR = os.getenv("TST_REDIS_DIR", None)
    run_args = [
        "--use-tags",
        "--redis_port",
        "{}".format(db_port),
    ]
    if TST_REDIS_DIR is not None:
        run_args.extend(["--redis_repo", TST_REDIS_DIR])
    args = parser.parse_args(
        args=run_args,
    )
    try:
        trigger_tests_cli_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0


def test_get_commits():
    parser = argparse.ArgumentParser(
        description="Get commits test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = spec_cli_args(parser)

    args = parser.parse_args(args=[])
    redisDirPath, cleanUp = get_repo(args)
    repo = git.Repo(redisDirPath)

    args = parser.parse_args(args=["--use-branch", "--from-date", "2023-02-11"])
    try:
        get_commits_by_branch(args, repo)
    except SystemExit as e:
        assert e.code == 0

    args = parser.parse_args(args=["--use-branch", "--from-date", "2023-02-11"])
    try:
        get_commits_by_tags(args, repo)
    except SystemExit as e:
        assert e.code == 0


def test_resolve_local_repo_path_dry_run_never_returns_a_path():
    # --dry-run must force the non-mutating GitHub-archive-endpoint fallback
    # (local_repo_path=None) regardless of which other flags are set --
    # get_commit_dict_from_sha()'s checkout/`git clean -ffdx`/submodule-update
    # path only runs when it receives a non-None local_repo_path.
    dry_run = True
    assert (
        resolve_local_repo_path(
            "/some/redis/repo", True, True, dry_run, "/some/redis/repo"
        )
        is None
    )
    assert (
        resolve_local_repo_path(
            "/some/redis/repo", False, False, dry_run, "/some/redis/repo"
        )
        is None
    )
    assert (
        resolve_local_repo_path(None, True, True, dry_run, "/tmp/clone") is None
    )
    assert resolve_local_repo_path(None, False, False, dry_run, "/tmp/clone") is None


def test_resolve_local_repo_path_non_dry_run_matches_prior_behavior():
    dry_run = False
    # --redis_repo alone -> local path used
    assert (
        resolve_local_repo_path("/some/redis/repo", False, False, dry_run, "/some/redis/repo")
        == "/some/redis/repo"
    )
    # --recurse_submodules with our own disposable clone (clean_up=True) -> local path used
    assert (
        resolve_local_repo_path(None, True, True, dry_run, "/tmp/clone") == "/tmp/clone"
    )
    # --recurse_submodules but NOT our own clone (e.g. a shared/reused checkout,
    # clean_up=False) -> no local path, avoid mutating a checkout we don't own
    assert resolve_local_repo_path(None, True, False, dry_run, "/tmp/clone") is None
    # neither flag set -> GitHub-archive-endpoint fallback (no local path)
    assert resolve_local_repo_path(None, False, False, dry_run, "/tmp/clone") is None
