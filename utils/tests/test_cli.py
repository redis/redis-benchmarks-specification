#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import argparse
import os

from redis_benchmarks_specification.__cli__.args import spec_cli_args
from redis_benchmarks_specification.__cli__.cli import cli_command_logic


def test_run_local_command_logic_oss_cluster():
    # should error due to missing --use-tags or --use-branch
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = spec_cli_args(parser)
    args = parser.parse_args(args=[])
    try:
        cli_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 1

    # should error due to missing --use-tags or --use-branch
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = spec_cli_args(parser)
    TST_REDIS_DIR = os.getenv("TST_REDIS_DIR", None)
    run_args = ["--use-tags"]
    if TST_REDIS_DIR is not None:
        run_args.extend(["--redis_repo", TST_REDIS_DIR])
    args = parser.parse_args(
        args=run_args,
    )
    try:
        cli_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0
