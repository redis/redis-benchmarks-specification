import argparse

from redis_benchmarks_specification.__common__.env import (
    ALLOWED_PROFILERS,
    DATASINK_RTS_AUTH,
    DATASINK_RTS_HOST,
    DATASINK_RTS_PORT,
    DATASINK_RTS_PUSH,
    DATASINK_RTS_USER,
    MACHINE_NAME,
    PROFILERS,
    PROFILERS_DEFAULT,
    PROFILERS_ENABLED,
    SPECS_PATH_TEST_SUITES,
)


def create_client_runner_args(project_name):
    parser = argparse.ArgumentParser(
        description=project_name,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--platform-name",
        type=str,
        default=MACHINE_NAME,
        help="Specify the running platform name. By default it will use the machine name.",
    )
    parser.add_argument(
        "--defaults_filename",
        type=str,
        default="{}/defaults.yml".format(SPECS_PATH_TEST_SUITES),
        help="specify the defaults file containing spec topologies, common metric extractions,etc...",
    )
    parser.add_argument("--triggering_env", type=str, default="ci")
    parser.add_argument("--setup_type", type=str, default="oss-standalone")
    parser.add_argument("--github_repo", type=str, default="redis")
    parser.add_argument("--github_org", type=str, default="redis")
    parser.add_argument("--github_version", type=str, default="NA")
    parser.add_argument(
        "--logname", type=str, default=None, help="logname to write the logs to"
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
    parser.add_argument("--db_server_host", type=str, default="localhost")
    parser.add_argument("--db_server_password", type=str, default=None)
    parser.add_argument("--db_server_port", type=int, default=6379)
    parser.add_argument("--cpuset_start_pos", type=int, default=0)
    parser.add_argument(
        "--maxmemory",
        type=int,
        default=0,
        help="If specified will not retrieved the maxmemory from the DB Server and will use this limit. If 0 will read the this value from the DB servers.",
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
        "--dry-run",
        default=False,
        action="store_true",
        help="Only check how many benchmarks we would run. Don't run benchmark but can change state of DB.",
    )
    parser.add_argument(
        "--dry-run-include-preload",
        default=False,
        action="store_true",
        help="Run all steps before benchmark. This can change the state of the DB.",
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
        + f"By default the chosen profilers are {PROFILERS_DEFAULT}"
        + f"Full list of profilers: {ALLOWED_PROFILERS}"
        + "Only available on x86 Linux platform and kernel version >= 4.9",
    )

    parser.add_argument(
        "--flushall_on_every_test_start",
        default=False,
        action="store_true",
        help="At the start of every test send a FLUSHALL",
    )
    parser.add_argument(
        "--flushall_on_every_test_end",
        default=False,
        action="store_true",
        help="At the end of every test send a FLUSHALL",
    )
    parser.add_argument(
        "--preserve_temporary_client_dirs",
        default=False,
        action="store_true",
        help="Preserve the temporary client dirs",
    )
    parser.add_argument(
        "--run-tests-with-dataset",
        default=False,
        action="store_true",
        help="Run tests that contain a dbconfig with dataset",
    )
    parser.add_argument(
        "--skip-tests-with-preload-via-tool",
        default=False,
        action="store_true",
        help="Run tests that contain a dbconfig with dataset",
    )
    parser.add_argument(
        "--client_aggregated_results_folder",
        type=str,
        default="",
        help="Client tool aggregated results folder ( contains all results from all runs ). If specified then all results will be copied there at the end of each run.",
    )
    parser.add_argument(
        "--tls",
        default=False,
        action="store_true",
        help="Enable SSL/TLS transport security",
    )
    parser.add_argument(
        "--tls-skip-verify",
        default=False,
        action="store_true",
        help="Skip verification of server certificate",
    )
    parser.add_argument(
        "--cert",
        default="",
        help="Use specified client certificate for TLS",
    )
    parser.add_argument(
        "--key",
        default="",
        help="Use specified private key for TLS",
    )
    parser.add_argument(
        "--cacert",
        default="",
        help="Use specified CA certs bundle for TLS",
    )
    parser.add_argument(
        "--resp",
        default="2",
        help="Set up RESP protocol version",
    )
    parser.add_argument(
        "--override-memtier-test-time",
        default=0,
        type=int,
        help="override memtier test-time for each benchmark. By default will preserve test time specified in test spec",
    )
    parser.add_argument(
        "--benchmark_local_install",
        default=False,
        action="store_true",
        help="Assume benchmarking tool (e.g. memtier benchmark) is installed locally and execute it without using a docker container.",
    )
    parser.add_argument(
        "--override-test-runs",
        default=1,
        type=int,
        help="override memtier number of runs for each benchmark. By default will run once each test",
    )
    parser.add_argument(
        "--container-timeout-buffer",
        default=60,
        type=int,
        help="Buffer time in seconds to add to test-time for container timeout. Default is 60 seconds.",
    )
    parser.add_argument(
        "--cluster-mode",
        default=False,
        action="store_true",
        help="Run client in cluster mode.",
    )
    parser.add_argument(
        "--unix-socket",
        default="",
        help="UNIX Domain socket name",
    )
    return parser
