#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import datetime


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
START_TIME_LAST_YEAR_UTC = START_TIME_NOW_UTC - datetime.timedelta(days=7)
CLI_TOOL_STATS = "stats"
CLI_TOOL_TRIGGER = "trigger"


def spec_cli_args(parser):
    parser.add_argument(
        "--test-suites-folder",
        type=str,
        default=SPECS_PATH_TEST_SUITES,
        help="Test suites folder, containing the different test variations",
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
            ",".join([CLI_TOOL_STATS, CLI_TOOL_TRIGGER])
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
    parser.add_argument("--trigger-unstable-commits", type=bool, default=True)
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
        "--dry-run",
        default=False,
        action="store_true",
        help="Only check how many benchmarks we would trigger. Don't request benchmark runs at the end.",
    )
    parser.add_argument(
        "--last_n",
        type=int,
        default=-1,
        help="Use the last N samples. by default will use all available values",
    )
    return parser
