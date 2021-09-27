#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import datetime


from redis_benchmarks_specification.__common__.env import (
    GH_REDIS_SERVER_HOST,
    GH_REDIS_SERVER_PORT,
    GH_REDIS_SERVER_AUTH,
    GH_REDIS_SERVER_USER,
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
    STREAM_KEYNAME_NEW_BUILD_EVENTS,
)

from redisbench_admin.run.common import get_start_time_vars

START_TIME_NOW_UTC, _, _ = get_start_time_vars()
START_TIME_LAST_YEAR_UTC = START_TIME_NOW_UTC - datetime.timedelta(days=7)


def spec_watchdog_args(parser):
    # events related Redis
    parser.add_argument("--redis_host", type=str, default=GH_REDIS_SERVER_HOST)
    parser.add_argument("--redis_port", type=int, default=GH_REDIS_SERVER_PORT)
    parser.add_argument("--redis_pass", type=str, default=GH_REDIS_SERVER_AUTH)
    parser.add_argument("--redis_user", type=str, default=GH_REDIS_SERVER_USER)
    # event stream from github
    parser.add_argument(
        "--events_stream_keyname_commits",
        type=str,
        default=STREAM_KEYNAME_GH_EVENTS_COMMIT,
    )
    # build events stream. This is the stream read by the coordinators to kickoff benchmark variations
    parser.add_argument(
        "--events_stream_keyname_builds",
        type=str,
        default=STREAM_KEYNAME_NEW_BUILD_EVENTS,
    )
    parser.add_argument(
        "--update-interval",
        type=int,
        default=60,
        help="watchdog update interval in seconds",
    )
    parser.add_argument(
        "--dry-run",
        default=False,
        action="store_true",
        help="Only check how many benchmarks we would trigger. Don't request benchmark runs at the end.",
    )
    return parser
