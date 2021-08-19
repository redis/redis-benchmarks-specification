import argparse

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
)


def create_self_contained_coordinator_args(project_name):
    parser = argparse.ArgumentParser(
        description=project_name,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--cpu-count",
        type=int,
        default=MACHINE_CPU_COUNT,
        help="Specify how much of the available CPU resources the coordinator can use.",
    )
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
    return parser
