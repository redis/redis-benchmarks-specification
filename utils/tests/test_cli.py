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
