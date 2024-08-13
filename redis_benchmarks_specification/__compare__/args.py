#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

# environment variables
import datetime
import os


def get_start_time_vars(start_time=None):
    if start_time is None:
        start_time = datetime.datetime.utcnow()
    start_time_ms = int(
        (start_time - datetime.datetime(1970, 1, 1)).total_seconds() * 1000
    )
    start_time_str = start_time.strftime("%Y-%m-%d-%H-%M-%S")
    return start_time, start_time_ms, start_time_str


PERFORMANCE_GH_TOKEN = os.getenv("PERFORMANCE_GH_TOKEN", None)
PERFORMANCE_RTS_PUSH = bool(int(os.getenv("PUSH_RTS", "0")))


_, NOW_UTC, _ = get_start_time_vars()
LAST_MONTH_UTC = NOW_UTC - (31 * 24 * 60 * 60 * 1000)
START_TIME_NOW_UTC, _, _ = get_start_time_vars()
START_TIME_LAST_SIX_MONTHS_UTC = START_TIME_NOW_UTC - datetime.timedelta(days=180)


def create_compare_arguments(parser):
    parser.add_argument(
        "--test",
        type=str,
        default="",
        help="specify a test (or a comma separated list of tests) to use for comparison. If none is specified by default will use all of them.",
    )
    parser.add_argument(
        "--defaults_filename",
        type=str,
        default="defaults.yml",
        help="specify the defaults file containing spec topologies, common metric extractions,etc...",
    )
    parser.add_argument("--github_repo", type=str, default="redis")
    parser.add_argument("--github_org", type=str, default="redis")
    parser.add_argument("--triggering_env", type=str, default="ci")
    parser.add_argument("--github_token", type=str, default=PERFORMANCE_GH_TOKEN)
    parser.add_argument("--pull-request", type=str, default=None, nargs="?", const="")
    parser.add_argument("--deployment_name", type=str, default="oss-standalone")
    parser.add_argument("--deployment_type", type=str, default="oss-standalone")
    parser.add_argument("--baseline_deployment_name", type=str, default="")
    parser.add_argument("--comparison_deployment_name", type=str, default="")
    parser.add_argument("--metric_name", type=str, default="ALL_STATS.Totals.Ops/sec")
    parser.add_argument(
        "--running_platform", type=str, default="intel64-ubuntu22.04-redis-icx1"
    )
    parser.add_argument("--extra-filter", type=str, default=None)
    parser.add_argument(
        "--last_n",
        type=int,
        default=-1,
        help="Use the last N samples for each time-serie. by default will use all available values",
    )
    parser.add_argument(
        "--last_n_baseline",
        type=int,
        default=7,
        help="Use the last N samples for each time-serie. by default will use last 7 available values",
    )
    parser.add_argument(
        "--last_n_comparison",
        type=int,
        default=1,
        help="Use the last N samples for each time-serie. by default will use last value only",
    )
    parser.add_argument(
        "--from-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        default=START_TIME_LAST_SIX_MONTHS_UTC,
    )
    parser.add_argument(
        "--to-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        default=START_TIME_NOW_UTC,
    )
    parser.add_argument(
        "--metric_mode",
        type=str,
        default="higher-better",
        help="either 'lower-better' or 'higher-better'",
    )
    parser.add_argument("--baseline-branch", type=str, default=None, required=False)
    parser.add_argument("--baseline-tag", type=str, default=None, required=False)
    parser.add_argument(
        "--baseline-target-version", type=str, default=None, required=False
    )
    parser.add_argument("--comparison-branch", type=str, default=None, required=False)
    parser.add_argument("--comparison-tag", type=str, default=None, required=False)
    parser.add_argument(
        "--comparison-target-version", type=str, default=None, required=False
    )
    parser.add_argument("--print-regressions-only", type=bool, default=False)
    parser.add_argument("--print-improvements-only", type=bool, default=False)
    parser.add_argument("--skip-unstable", type=bool, default=False)
    parser.add_argument("--verbose", type=bool, default=False)
    parser.add_argument("--simple-table", type=bool, default=False)
    parser.add_argument("--use_metric_context_path", type=bool, default=False)
    parser.add_argument("--testname_regex", type=str, default=".*", required=False)
    parser.add_argument(
        "--regressions-percent-lower-limit",
        type=float,
        default=5.0,
        help="Only consider regressions with a percentage over the defined limit. (0-100)",
    )
    parser.add_argument(
        "--redistimeseries_host", type=str, default="benchmarks.redislabs.com"
    )
    parser.add_argument("--redistimeseries_port", type=int, default=12011)
    parser.add_argument("--redistimeseries_pass", type=str, default=None)
    parser.add_argument("--redistimeseries_user", type=str, default=None)
    parser.add_argument(
        "--from_timestamp",
        default=None,
        help="The minimum period to use for the the value fetching",
    )
    parser.add_argument("--to_timestamp", default=None)

    parser.add_argument(
        "--grafana_base_dashboard",
        type=str,
        default="https://benchmarksrediscom.grafana.net/d/",
    )
    parser.add_argument(
        "--auto-approve",
        required=False,
        default=False,
        action="store_true",
        help="Skip interactive approval of changes to github before applying.",
    )
    return parser
