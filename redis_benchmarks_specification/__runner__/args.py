import argparse

from redis_benchmarks_specification.__common__.env import (
    SPECS_PATH_TEST_SUITES,
    DATASINK_RTS_HOST,
    DATASINK_RTS_PORT,
    DATASINK_RTS_AUTH,
    DATASINK_RTS_USER,
    DATASINK_RTS_PUSH,
    MACHINE_NAME,
    PROFILERS_ENABLED,
    PROFILERS,
    PROFILERS_DEFAULT,
    ALLOWED_PROFILERS,
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
    parser.add_argument("--db_server_host", type=str, default="localhost")
    parser.add_argument("--db_server_port", type=int, default=6379)
    parser.add_argument("--cpuset_start_pos", type=int, default=0)
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
    return parser
