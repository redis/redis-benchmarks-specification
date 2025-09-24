#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import datetime
import os
from distutils.util import strtobool
from redis_benchmarks_specification.__common__.env import (
    GH_REDIS_SERVER_HOST,
    GH_TOKEN,
    GH_REDIS_SERVER_PORT,
    GH_REDIS_SERVER_AUTH,
    GH_REDIS_SERVER_USER,
    SPECS_PATH_TEST_SUITES,
)

from redisbench_admin.run.common import get_start_time_vars

START_TIME_NOW_UTC, _, _ = get_start_time_vars()
START_TIME_LAST_YEAR_UTC = START_TIME_NOW_UTC - datetime.timedelta(days=90)
CLI_TOOL_STATS = "stats"
CLI_TOOL_TRIGGER = "trigger"
CLI_TOOL_DOCKERHUB = "dockerhub"
PERFORMANCE_GH_TOKEN = os.getenv("PERFORMANCE_GH_TOKEN", None)


def spec_cli_args(parser):
    parser.add_argument(
        "--test-suites-folder",
        type=str,
        default=SPECS_PATH_TEST_SUITES,
        help="Test suites folder, containing the different test variations",
    )
    parser.add_argument(
        "--tests-regexp",
        type=str,
        default=".*",
        help="Interpret PATTERN as a regular expression to filter test names",
    )
    parser.add_argument(
        "--tests-groups-regexp",
        type=str,
        default=".*",
        help="Interpret PATTERN as a regular expression to filter test group names",
    )
    parser.add_argument(
        "--tests-priority-lower-limit",
        type=int,
        default=0,
        help="Run a subset of the tests based uppon a preset priority. By default runs all tests.",
    )
    parser.add_argument(
        "--tests-priority-upper-limit",
        type=int,
        default=100000,
        help="Run a subset of the tests based uppon a preset priority. By default runs all tests.",
    )
    parser.add_argument(
        "--defaults_filename",
        type=str,
        default="defaults.yml",
        help="specify the defaults file containing spec topologies, common metric extractions,etc...",
    )
    parser.add_argument("--redis_host", type=str, default=GH_REDIS_SERVER_HOST)
    parser.add_argument("--branch", type=str, default="unstable")
    parser.add_argument("--commandstats-csv", type=str, default="")
    parser.add_argument(
        "--commandstats-csv-include-modules",
        default=False,
        action="store_true",
        help="Include modules statistics on commandstats.",
    )
    parser.add_argument(
        "--use-git-timestamp",
        type=lambda x: bool(strtobool(x)),
        default=True,
        help="Use git timestamp",
    )
    parser.add_argument("--github_token", type=str, default=PERFORMANCE_GH_TOKEN)
    parser.add_argument("--pull-request", type=str, default=None, nargs="?", const="")
    parser.add_argument(
        "--auto-approve",
        required=False,
        default=False,
        action="store_true",
        help="Skip interactive approval of changes to github before applying.",
    )
    parser.add_argument("--summary-csv", type=str, default="")
    parser.add_argument("--group-csv", type=str, default="")
    parser.add_argument("--commands-json-file", type=str, default="./commands.json")
    parser.add_argument(
        "--commands-priority-file", type=str, default="./commands-priority.json"
    )
    parser.add_argument("--groups-json-file", type=str, default="./groups.json")
    parser.add_argument(
        "--override-tests",
        default=False,
        action="store_true",
        help="Override test specs.",
    )
    parser.add_argument(
        "--fail-on-required-diff",
        default=False,
        action="store_true",
        help="Fail tool when there is difference between required parameters.",
    )
    parser.add_argument(
        "--push-stats-redis",
        default=False,
        action="store_true",
        help="Push test stats to redis.",
    )
    parser.add_argument(
        "--tool",
        type=str,
        default=CLI_TOOL_TRIGGER,
        help="subtool to use. One of '{}' ".format(
            ",".join([CLI_TOOL_STATS, CLI_TOOL_TRIGGER, CLI_TOOL_DOCKERHUB])
        ),
    )
    parser.add_argument("--gh_token", type=str, default=GH_TOKEN)
    parser.add_argument("--redis_port", type=int, default=GH_REDIS_SERVER_PORT)
    parser.add_argument("--redis_pass", type=str, default=GH_REDIS_SERVER_AUTH)
    parser.add_argument("--redis_user", type=str, default=GH_REDIS_SERVER_USER)
    parser.add_argument(
        "--from-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        default=START_TIME_LAST_YEAR_UTC,
    )
    parser.add_argument(
        "--to-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        default=START_TIME_NOW_UTC,
    )
    parser.add_argument("--redis_repo", type=str, default=None)
    parser.add_argument("--gh_org", type=str, default="redis")
    parser.add_argument("--gh_repo", type=str, default="redis")
    parser.add_argument("--server_name", type=str, default=None)
    parser.add_argument("--run_image", type=str, default="redis")
    parser.add_argument("--arch", type=str, default="amd64")
    parser.add_argument("--id", type=str, default="dockerhub")
    parser.add_argument("--mnt_point", type=str, default="")
    parser.add_argument("--trigger-unstable-commits", type=bool, default=True)
    parser.add_argument(
        "--docker-dont-air-gap",
        default=False,
        action="store_true",
        help="Dont store the docker images in redis keys.",
    )
    parser.add_argument(
        "--use-tags",
        default=False,
        action="store_true",
        help="Iterate over the git tags.",
    )
    parser.add_argument(
        "--tags-regexp",
        type=str,
        default=".*",
        help="Interpret PATTERN as a regular expression to filter tag names",
    )
    parser.add_argument(
        "--hash-regexp",
        type=str,
        default=".*",
        help="Interpret PATTERN as a regular expression to filter commit hashes",
    )
    parser.add_argument(
        "--use-branch",
        default=False,
        action="store_true",
        help="Iterate over the git commits.",
    )
    parser.add_argument(
        "--build_artifacts",
        type=str,
        default="",
    )
    parser.add_argument(
        "--build_command",
        type=str,
        default="",
    )
    parser.add_argument(
        "--git_hash",
        type=str,
        default="",
    )
    parser.add_argument(
        "--dry-run",
        default=False,
        action="store_true",
        help="Only check how many benchmarks we would trigger. Don't request benchmark runs at the end.",
    )
    parser.add_argument(
        "--last_n",
        type=int,
        default=1,
        help="Use the last N samples. by default will use last commit",
    )
    parser.add_argument(
        "--platform",
        type=str,
        default="",
        help="Only trigger tests on the specified platform.",
    )
    parser.add_argument(
        "--wait-build",
        default=False,
        action="store_true",
        help="Wait for build to be finished.",
    )
    parser.add_argument(
        "--wait-build-timeout",
        type=int,
        default=-1,
        help="Wait x sections for build. If -1, waits forever.",
    )
    parser.add_argument(
        "--command-regex",
        type=str,
        default=".*",
        help="Filter tests by command using regex. Only tests that include commands matching this regex will be processed.",
    )
    return parser
