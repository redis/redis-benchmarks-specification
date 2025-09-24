import csv
import logging
import os
import pathlib
import re
import datetime as dt
import redis

from redis_benchmarks_specification.__common__.timeseries import (
    timeseries_test_sucess_flow,
    push_data_to_redistimeseries,
    get_project_ts_tags,
)


def execute_init_commands(benchmark_config, r, dbconfig_keyname="dbconfig"):
    cmds = None
    lua_scripts = None
    res = 0
    if dbconfig_keyname in benchmark_config:
        for k, v in benchmark_config[dbconfig_keyname].items():
            if "init_commands" in k:
                cmds = v
            elif "init_lua" in k:
                lua_scripts = v

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
                    if len(lines) > 0 and lines[0] != " " and len(lines[0]) > 0:
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

    # Process init_lua scripts
    if lua_scripts is not None:
        if type(lua_scripts) == str:
            lua_scripts = [lua_scripts]

        for lua_script in lua_scripts:
            try:
                logging.info(
                    "Executing Lua script (length: {} chars)".format(len(lua_script))
                )
                # Execute the Lua script using EVAL command with 0 keys
                stdout = r.execute_command("EVAL", lua_script, 0)
                logging.info("Lua script result: {}".format(stdout))
                res = res + 1
            except Exception as e:
                logging.error("Lua script execution failed: {}".format(e))

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
                    logging.debug(
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
    testsuite_spec_files.sort()
    logging.info(
        "There are a total of {} test-suites in folder {}".format(
            len(testsuite_spec_files), testsuites_folder
        )
    )
    if len(testsuite_spec_files) < 11:
        for test in testsuite_spec_files:
            logging.info(f"Test {test}")

    return testsuite_spec_files


def commandstats_latencystats_process_name(
    metric_name, prefix, setup_name, variant_labels_dict
):
    if prefix in metric_name:
        command_and_metric_and_shard = metric_name[len(prefix) :]
        command = (
            command_and_metric_and_shard[0]
            + command_and_metric_and_shard[1:].split("_", 1)[0]
        )
        metric_and_shard = command_and_metric_and_shard[1:].split("_", 1)[1]
        metric = metric_and_shard
        shard = "1"
        if "_shard_" in metric_and_shard:
            metric = metric_and_shard.split("_shard_")[0]
            shard = metric_and_shard.split("_shard_")[1]
        variant_labels_dict["metric"] = metric
        variant_labels_dict["command"] = command
        variant_labels_dict["command_and_metric"] = "{} - {}".format(command, metric)
        variant_labels_dict["command_and_metric_and_setup"] = "{} - {} - {}".format(
            command, metric, setup_name
        )
        variant_labels_dict["command_and_setup"] = "{} - {}".format(command, setup_name)
        variant_labels_dict["shard"] = shard
        variant_labels_dict["metric_and_shard"] = metric_and_shard

        version = None
        branch = None
        if "version" in variant_labels_dict:
            version = variant_labels_dict["version"]
        if "branch" in variant_labels_dict:
            branch = variant_labels_dict["branch"]

        if version is not None:
            variant_labels_dict["command_and_metric_and_version"] = (
                "{} - {} - {}".format(command, metric, version)
            )
            variant_labels_dict["command_and_metric_and_setup_and_version"] = (
                "{} - {} - {} - {}".format(command, metric, setup_name, version)
            )

        if branch is not None:
            variant_labels_dict["command_and_metric_and_branch"] = (
                "{} - {} - {}".format(command, metric, branch)
            )
            variant_labels_dict["command_and_metric_and_setup_and_branch"] = (
                "{} - {} - {} - {}".format(command, metric, setup_name, branch)
            )


def collect_redis_metrics(
    redis_conns,
    sections=["memory", "cpu", "commandstats", "latencystats"],
    section_filter=None,
):
    start_time = dt.datetime.utcnow()
    start_time_ms = int((start_time - dt.datetime(1970, 1, 1)).total_seconds() * 1000)
    res = []
    overall = {}
    multi_shard = False
    if len(redis_conns) > 1:
        multi_shard = True
    for conn_n, conn in enumerate(redis_conns):
        conn_res = {}
        for section in sections:
            info = conn.info(section)
            conn_res[section] = info
            if section not in overall:
                overall[section] = {}
            for k, v in info.items():
                collect = True
                if section_filter is not None:
                    if section in section_filter:
                        if k not in section_filter[section]:
                            collect = False
                if collect and type(v) is float or type(v) is int:
                    if k not in overall[section]:
                        overall[section][k] = 0
                    overall[section][k] += v
                if collect and type(v) is dict:
                    for inner_k, inner_v in v.items():
                        if type(inner_v) is float or type(inner_v) is int:
                            final_str_k = "{}_{}".format(k, inner_k)
                            if multi_shard:
                                final_str_k += "_shard_{}".format(conn_n + 1)
                            if final_str_k not in overall[section]:
                                overall[section][final_str_k] = inner_v

        res.append(conn_res)

    kv_overall = {}
    for sec, kv_detail in overall.items():
        for k, metric_value in kv_detail.items():
            metric_name = "{}_{}".format(sec, k)
            kv_overall[metric_name] = metric_value

    return start_time_ms, res, kv_overall


def export_redis_metrics(
    artifact_version,
    end_time_ms,
    overall_end_time_metrics,
    rts,
    setup_name,
    setup_type,
    test_name,
    tf_github_branch,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    metadata_dict=None,
    expire_ms=0,
    git_hash=None,
    running_platform=None,
):
    datapoint_errors = 0
    datapoint_inserts = 0
    sprefix = (
        "ci.benchmarks.redis/"
        + "{triggering_env}/{github_org}/{github_repo}".format(
            triggering_env=tf_triggering_env,
            github_org=tf_github_org,
            github_repo=tf_github_repo,
        )
    )
    logging.info(
        "Adding a total of {} server side metrics collected at the end of benchmark".format(
            len(list(overall_end_time_metrics.items()))
        )
    )
    timeseries_dict = {}
    by_variants = {}
    if tf_github_branch is not None and tf_github_branch != "":
        by_variants["by.branch/{}".format(tf_github_branch)] = {
            "branch": tf_github_branch
        }
    if git_hash is not None and git_hash != "":
        by_variants["by.hash/{}".format(git_hash)] = {"hash": git_hash}
        if artifact_version is not None and artifact_version != "":
            by_variants["by.hash/{}".format(git_hash)]["version"] = artifact_version
    if artifact_version is not None and artifact_version != "":
        by_variants["by.version/{}".format(artifact_version)] = {
            "version": artifact_version
        }
    for (
        by_variant,
        variant_labels_dict,
    ) in by_variants.items():
        for (
            metric_name,
            metric_value,
        ) in overall_end_time_metrics.items():
            tsname_metric = f"{sprefix}/{test_name}/{by_variant}/benchmark_end/{running_platform}/{setup_name}/{metric_name}"

            logging.debug(
                "Adding a redis server side metric collected at the end of benchmark."
                + " metric_name={} metric_value={} time-series name: {}".format(
                    metric_name,
                    metric_value,
                    tsname_metric,
                )
            )
            variant_labels_dict["metric"] = metric_name
            commandstats_latencystats_process_name(
                metric_name, "commandstats_cmdstat_", setup_name, variant_labels_dict
            )
            commandstats_latencystats_process_name(
                metric_name,
                "latencystats_latency_percentiles_usec_",
                setup_name,
                variant_labels_dict,
            )

            variant_labels_dict["test_name"] = test_name
            if metadata_dict is not None:
                variant_labels_dict.update(metadata_dict)

            timeseries_dict[tsname_metric] = {
                "labels": get_project_ts_tags(
                    tf_github_org,
                    tf_github_repo,
                    setup_name,
                    setup_type,
                    tf_triggering_env,
                    variant_labels_dict,
                    None,
                    running_platform,
                ),
                "data": {end_time_ms: metric_value},
            }
    i_errors, i_inserts = push_data_to_redistimeseries(rts, timeseries_dict, expire_ms)
    datapoint_errors = datapoint_errors + i_errors
    datapoint_inserts = datapoint_inserts + i_inserts
    return datapoint_errors, datapoint_inserts


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
    git_hash=None,
    collect_commandstats=True,
    collect_memory_metrics=True,
):
    logging.info(
        f"Using datapoint_time_ms: {datapoint_time_ms}. git_hash={git_hash}, git_branch={git_branch}, git_version={git_version}. gh_org={tf_github_org}, gh_repo={tf_github_repo}"
    )
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
        None,
        git_hash,
        disable_target_tables=True,
    )
    if collect_memory_metrics:
        logging.info("Collecting memory metrics")
        (
            _,
            _,
            overall_end_time_metrics,
        ) = collect_redis_metrics(
            redis_conns,
            ["memory"],
            {
                "memory": [
                    "used_memory",
                    "used_memory_dataset",
                ]
            },
        )
        print(overall_end_time_metrics)
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
            {"metric-type": "redis-memory-metrics"},
            expire_redis_metrics_ms,
            git_hash,
            running_platform,
        )
    if collect_commandstats:
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
            git_hash,
            running_platform,
        )

    # Update deployment tracking sets
    deployment_type_and_name = f"{setup_type}_AND_{setup_name}"
    deployment_type_and_name_and_version = (
        f"{setup_type}_AND_{setup_name}_AND_{git_version}"
    )

    # Add to deployment-specific set (only if datasink connection is available)
    if datasink_conn is not None:
        deployment_set_key = f"ci.benchmarks.redis/{tf_triggering_env}/{deployment_type_and_name_and_version}:set"
        datasink_conn.sadd(deployment_set_key, test_name)

        # Add to testcases set
        testcases_set_key = f"ci.benchmarks.redis/{tf_triggering_env}/testcases:set"
        datasink_conn.sadd(testcases_set_key, test_name)
    else:
        logging.debug("Datasink connection not available, skipping set operations")

    # Add metadata fields to timeseries metadata
    metadata["deployment_type_AND_deployment_name"] = deployment_type_and_name
    metadata["deployment_type_AND_deployment_name_AND_version"] = (
        deployment_type_and_name_and_version
    )
