import argparse
import os
from redis_benchmarks_specification.__common__.env import (
    MACHINE_CPU_COUNT,
    SPECS_PATH_SETUPS,
    SPECS_PATH_TEST_SUITES,
    DATASINK_RTS_HOST,
    DATASINK_RTS_PORT,
    DATASINK_RTS_AUTH,
    DATASINK_RTS_USER,
    DATASINK_RTS_PUSH,
    MACHINE_NAME,
    GH_REDIS_SERVER_HOST,
    GH_REDIS_SERVER_PORT,
    GH_REDIS_SERVER_AUTH,
    GH_REDIS_SERVER_USER,
    PROFILERS_ENABLED,
    PROFILERS,
    PROFILERS_DEFAULT,
    ALLOWED_PROFILERS,
)

PERFORMANCE_GH_TOKEN = os.getenv("PERFORMANCE_GH_TOKEN", None)


def create_self_contained_coordinator_args(project_name):
    parser = argparse.ArgumentParser(
        description=project_name,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--version",
        action="version",
        version=project_name,
        help="Show version information and exit",
    )
    parser.add_argument("--event_stream_host", type=str, default=GH_REDIS_SERVER_HOST)
    parser.add_argument("--event_stream_port", type=int, default=GH_REDIS_SERVER_PORT)
    parser.add_argument("--event_stream_pass", type=str, default=GH_REDIS_SERVER_AUTH)
    parser.add_argument("--event_stream_user", type=str, default=GH_REDIS_SERVER_USER)
    parser.add_argument("--github_token", type=str, default=PERFORMANCE_GH_TOKEN)
    parser.add_argument(
        "--cpu-count",
        type=int,
        default=MACHINE_CPU_COUNT,
        help="Specify how much of the available CPU resources the coordinator can use.",
    )
    parser.add_argument("--redis_proc_start_port", type=int, default=6379)
    parser.add_argument("--cpuset_start_pos", type=int, default=0)
    parser.add_argument(
        "--platform-name",
        type=str,
        default=MACHINE_NAME,
        help="Specify the running platform name. By default it will use the machine name.",
    )
    parser.add_argument(
        "--logname", type=str, default=None, help="logname to write the logs to"
    )
    parser.add_argument(
        "--consumer-start-id",
        type=str,
        default=">",
    )
    parser.add_argument(
        "--consumer-id",
        type=int,
        default=1,
        help="Consumer id for consumer group for platform",
    )
    parser.add_argument(
        "--consumer-pos",
        type=int,
        default=1,
        help="Consumer pos for consumer group for platform",
    )

    parser.add_argument(
        "--setups-folder",
        type=str,
        default=SPECS_PATH_SETUPS,
        help="Setups folder, containing the build environment variations sub-folder that we use to trigger different build artifacts",
    )
    parser.add_argument(
        "--test-suites-folder",
        type=str,
        default=SPECS_PATH_TEST_SUITES,
        help="Test suites folder, containing the different test variations",
    )
    parser.add_argument(
        "--test",
        type=str,
        default="",
        help="specify a test to run. By default will run all the tests"
        + " present in the folder specified in --test-suites-folder.",
    )
    parser.add_argument(
        "--tests-regexp",
        type=str,
        default=".*",
        help="Interpret PATTERN as a regular expression to filter test names",
    )
    parser.add_argument(
        "--datasink_redistimeseries_host", type=str, default=DATASINK_RTS_HOST
    )
    parser.add_argument(
        "--datasink_redistimeseries_port", type=int, default=DATASINK_RTS_PORT
    )
    parser.add_argument(
        "--datasink_redistimeseries_pass", type=str, default=DATASINK_RTS_AUTH
    )
    parser.add_argument(
        "--datasink_redistimeseries_user", type=str, default=DATASINK_RTS_USER
    )
    parser.add_argument(
        "--datasink_push_results_redistimeseries",
        default=DATASINK_RTS_PUSH,
        action="store_true",
        help="uploads the results to RedisTimeSeries. Proper credentials are required",
    )
    parser.add_argument("--profilers", type=str, default=PROFILERS)
    parser.add_argument(
        "--enable-profilers",
        default=PROFILERS_ENABLED,
        action="store_true",
        help="Enable Identifying On-CPU and Off-CPU Time using perf/ebpf/vtune tooling. "
        + "By default the chosen profilers are {}".format(PROFILERS_DEFAULT)
        + "Full list of profilers: {}".format(ALLOWED_PROFILERS)
        + "Only available on x86 Linux platform and kernel version >= 4.9",
    )
    parser.add_argument(
        "--grafana-profile-dashboard",
        type=str,
        default="https://benchmarksredisio.grafana.net/d/uRPZar57k/ci-profiler-viewer",
    )
    parser.add_argument(
        "--docker-air-gap",
        default=False,
        action="store_true",
        help="Read the docker images from redis keys.",
    )
    parser.add_argument(
        "--verbose",
        default=False,
        action="store_true",
        help="Run in verbose mode.",
    )
    parser.add_argument(
        "--override-memtier-test-time",
        default=0,
        type=int,
        help="override memtier test-time for each benchmark. By default will preserve test time specified in test spec",
    )
    parser.add_argument(
        "--defaults_filename",
        type=str,
        default="{}/defaults.yml".format(SPECS_PATH_TEST_SUITES),
        help="specify the defaults file containing spec topologies, common metric extractions,etc...",
    )
    parser.add_argument(
        "--arch", type=str, default="amd64", help="arch to build artifacts"
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
        "--topology",
        type=str,
        default="",
        help="Filter tests to run only with the specified topology (e.g. oss-standalone)",
    )
    parser.add_argument(
        "--exclusive-hardware",
        default=False,
        action="store_true",
        help="Enable exclusive hardware mode. Kills all memtier processes and stops all docker containers before and after each test.",
    )
    parser.add_argument(
        "--http-port",
        type=int,
        default=8080,
        help="Port for HTTP server endpoints (/ping health check and /reset-queue POST endpoint).",
    )
    parser.add_argument(
        "--http-auth-username",
        type=str,
        default=None,
        help="Username for HTTP endpoint authentication. HTTP server is disabled if not provided.",
    )
    parser.add_argument(
        "--http-auth-password",
        type=str,
        default=None,
        help="Password for HTTP endpoint authentication. HTTP server is disabled if not provided.",
    )
    parser.add_argument(
        "--skip-clear-pending-on-startup",
        default=False,
        action="store_true",
        help="Skip automatically clearing pending messages and resetting consumer group position on startup. By default, pending messages are cleared and consumer group is reset to latest position to skip old work and recover from crashes.",
    )
    return parser
