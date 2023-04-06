import csv
import logging
import os
import pathlib
import re

import redis
from redisbench_admin.run.metrics import collect_redis_metrics
from redisbench_admin.run.redistimeseries import timeseries_test_sucess_flow
from redisbench_admin.run_remote.run_remote import export_redis_metrics


def execute_init_commands(benchmark_config, r, dbconfig_keyname="dbconfig"):
    cmds = None
    res = 0
    if dbconfig_keyname in benchmark_config:
        for k, v in benchmark_config[dbconfig_keyname].items():
            if "init_commands" in k:
                cmds = v

    if type(cmds) == str:
        cmds = [cmds]
    if cmds is not None:
        for cmd in cmds:
            is_array = False
            if type(cmd) == list:
                is_array = True
            if '"' in cmd:
                cols = []
                for lines in csv.reader(
                    cmd,
                    quotechar='"',
                    delimiter=" ",
                    quoting=csv.QUOTE_ALL,
                    skipinitialspace=True,
                ):
                    if lines[0] != " " and len(lines[0]) > 0:
                        cols.append(lines[0])
                cmd = cols
                is_array = True
            try:
                logging.info("Sending init command: {}".format(cmd))
                stdout = ""
                if is_array:
                    stdout = r.execute_command(*cmd)
                else:
                    stdout = r.execute_command(cmd)
                res = res + 1
                logging.info("Command reply: {}".format(stdout))
            except redis.connection.ConnectionError as e:
                logging.error(
                    "Error establishing connection to Redis. Message: {}".format(
                        e.__str__()
                    )
                )

    return res


def get_benchmark_specs(testsuites_folder, test="", test_regex=".*"):
    final_files = []
    if test == "":
        files = pathlib.Path(testsuites_folder).glob("*.yml")
        original_files = [str(x) for x in files]
        if test_regex == ".*":
            logging.info(
                "Acception all test files. If you need further filter specify a regular expression via --tests-regexp"
            )
            "Running all specified benchmarks: {}".format(" ".join(original_files))
            final_files = original_files
        else:
            logging.info(
                "Filtering all test names via a regular expression: {}".format(
                    test_regex
                )
            )
            test_regexp_string = re.compile(test_regex)
            for test_name in original_files:
                match_obj = re.search(test_regexp_string, test_name)
                if match_obj is None:
                    logging.info(
                        "Skipping test file: {} given it does not match regex {}".format(
                            test_name, test_regexp_string
                        )
                    )
                else:
                    final_files.append(test_name)

    else:
        files = test.split(",")
        final_files = ["{}/{}".format(testsuites_folder, x) for x in files]
        logging.info(
            "Running specific benchmark in {} files: {}".format(
                len(final_files), final_files
            )
        )
    return final_files


def extract_testsuites(args):
    testsuites_folder = os.path.abspath(args.test_suites_folder)
    logging.info("Using test-suites folder dir {}".format(testsuites_folder))
    testsuite_spec_files = get_benchmark_specs(
        testsuites_folder, args.test, args.tests_regexp
    )
    logging.info(
        "There are a total of {} test-suites in folder {}".format(
            len(testsuite_spec_files), testsuites_folder
        )
    )
    return testsuite_spec_files


def reset_commandstats(redis_conns):
    for pos, redis_conn in enumerate(redis_conns):
        logging.info("Resetting commmandstats for shard {}".format(pos))
        try:
            redis_conn.config_resetstat()
        except redis.exceptions.ResponseError as e:
            logging.warning(
                "Catched an error while resetting status: {}".format(e.__str__())
            )


def exporter_datasink_common(
    benchmark_config,
    benchmark_duration_seconds,
    build_variant_name,
    datapoint_time_ms,
    dataset_load_duration_seconds,
    datasink_conn,
    datasink_push_results_redistimeseries,
    git_branch,
    git_version,
    metadata,
    redis_conns,
    results_dict,
    running_platform,
    setup_name,
    setup_type,
    test_name,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    topology_spec_name,
    default_metrics=None,
):
    logging.info("Using datapoint_time_ms: {}".format(datapoint_time_ms))
    timeseries_test_sucess_flow(
        datasink_push_results_redistimeseries,
        git_version,
        benchmark_config,
        benchmark_duration_seconds,
        dataset_load_duration_seconds,
        default_metrics,
        topology_spec_name,
        setup_name,
        None,
        results_dict,
        datasink_conn,
        datapoint_time_ms,
        test_name,
        git_branch,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        metadata,
        build_variant_name,
        running_platform,
    )
    logging.info("Collecting memory metrics")
    (_, _, overall_end_time_metrics,) = collect_redis_metrics(
        redis_conns,
        ["memory"],
        {
            "memory": [
                "used_memory",
                "used_memory_dataset",
            ]
        },
    )
    # 7 days from now
    expire_redis_metrics_ms = 7 * 24 * 60 * 60 * 1000
    export_redis_metrics(
        git_version,
        datapoint_time_ms,
        overall_end_time_metrics,
        datasink_conn,
        setup_name,
        setup_type,
        test_name,
        git_branch,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        {"metric-type": "redis-metrics"},
        expire_redis_metrics_ms,
    )
    logging.info("Collecting commandstat metrics")
    (
        _,
        _,
        overall_commandstats_metrics,
    ) = collect_redis_metrics(redis_conns, ["commandstats"])
    export_redis_metrics(
        git_version,
        datapoint_time_ms,
        overall_commandstats_metrics,
        datasink_conn,
        setup_name,
        setup_type,
        test_name,
        git_branch,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        {"metric-type": "commandstats"},
        expire_redis_metrics_ms,
    )
