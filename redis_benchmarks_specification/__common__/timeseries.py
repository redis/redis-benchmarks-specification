#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import datetime
import logging
from tqdm import tqdm
import redis
from jsonpath_ng.parser import JsonPathParser


def parse(string):
    return JsonPathParser().parse(string)


def parse_exporter_timemetric(metric_path: str, results_dict: dict):
    datapoints_timestamp = None
    try:
        jsonpath_expr = parse(metric_path)
        find_res = jsonpath_expr.find(results_dict)
        if len(find_res) > 0:
            datapoints_timestamp = int(find_res[0].value)
    except Exception as e:
        logging.error(
            "Unable to parse time-metric {}. Error: {}".format(metric_path, e.__str__())
        )
    return datapoints_timestamp


def parse_exporter_timemetric_definition(
    benchmark_config: dict, configkey: str = "redistimeseries"
):
    metric_path = None
    if "timemetric" in benchmark_config[configkey]:
        metric_path = benchmark_config[configkey]["timemetric"]
    return metric_path


def parse_exporter_metrics_definition(
    benchmark_config: dict, configkey: str = "redistimeseries"
):
    metrics = []
    if configkey in benchmark_config:
        if "metrics" in benchmark_config[configkey]:
            for metric_name in benchmark_config[configkey]["metrics"]:
                metrics.append(metric_name)
    return metrics


def get_ts_metric_name(
    by,
    by_value,
    tf_github_org,
    tf_github_repo,
    deployment_name,
    deployment_type,
    test_name,
    tf_triggering_env,
    metric_name,
    metric_context_path=None,
    use_metric_context_path=False,
    build_variant_name=None,
    running_platform=None,
):
    if use_metric_context_path:
        metric_name = "{}/{}".format(metric_name, metric_context_path)
    build_variant_str = ""
    if build_variant_name is not None:
        build_variant_str = "{}/".format(str(build_variant_name))
    running_platform_str = ""
    if running_platform is not None:
        running_platform_str = "{}/".format(str(running_platform))
    if deployment_name != deployment_type:
        deployment_name = "/{}".format(deployment_name)
    else:
        deployment_name = ""
    ts_name = (
        "ci.benchmarks.redis/{by}/"
        "{triggering_env}/{github_org}/{github_repo}/"
        "{test_name}/{build_variant_str}{running_platform_str}{deployment_type}{deployment_name}/{by_value}/{metric}".format(
            by=by,
            triggering_env=tf_triggering_env,
            github_org=tf_github_org,
            github_repo=tf_github_repo,
            test_name=test_name,
            deployment_type=deployment_type,
            deployment_name=deployment_name,
            build_variant_str=build_variant_str,
            running_platform_str=running_platform_str,
            by_value=str(by_value),
            metric=metric_name,
        )
    )
    return ts_name


def extract_results_table(
    metrics,
    results_dict,
):
    results_matrix = []
    cleaned_metrics = []
    already_present_metrics = []
    # insert first the dict metrics
    for jsonpath in metrics:
        if type(jsonpath) == dict:
            cleaned_metrics.append(jsonpath)
            metric_jsonpath = list(jsonpath.keys())[0]
            already_present_metrics.append(metric_jsonpath)
    for jsonpath in metrics:
        if type(jsonpath) == str:
            if jsonpath not in already_present_metrics:
                already_present_metrics.append(jsonpath)
                cleaned_metrics.append(jsonpath)

    for jsonpath in cleaned_metrics:
        test_case_targets_dict = {}
        metric_jsonpath = jsonpath
        find_res = None
        try:
            if type(jsonpath) == str:
                jsonpath_expr = parse(jsonpath)
            if type(jsonpath) == dict:
                metric_jsonpath = list(jsonpath.keys())[0]
                test_case_targets_dict = jsonpath[metric_jsonpath]
                jsonpath_expr = parse(metric_jsonpath)
            find_res = jsonpath_expr.find(results_dict)
        except Exception:
            pass
        finally:
            if find_res is not None:
                use_metric_context_path = False
                if len(find_res) > 1:
                    use_metric_context_path = True
                # Always use context path for precision_summary metrics to show actual precision levels
                if "precision_summary" in metric_jsonpath and "*" in metric_jsonpath:
                    use_metric_context_path = True
                for metric in find_res:
                    metric_name = str(metric.path)
                    metric_value = float(metric.value)
                    metric_context_path = str(metric.context.path)
                    if metric_jsonpath[0] == "$":
                        metric_jsonpath = metric_jsonpath[1:]
                    if metric_jsonpath[0] == ".":
                        metric_jsonpath = metric_jsonpath[1:]

                    # For precision_summary metrics, construct the full resolved path for display
                    display_path = metric_jsonpath
                    if (
                        "precision_summary" in metric_jsonpath
                        and "*" in metric_jsonpath
                        and use_metric_context_path
                    ):
                        # Replace the wildcard with the actual precision level
                        display_path = metric_jsonpath.replace("*", metric_context_path)

                    # retro-compatible naming
                    if use_metric_context_path is False:
                        metric_name = metric_jsonpath
                    else:
                        # For display purposes, use the resolved path for precision_summary
                        if (
                            "precision_summary" in metric_jsonpath
                            and "*" in metric_jsonpath
                        ):
                            metric_name = display_path
                        else:
                            # Clean up the metric name for other cases
                            metric_name = metric_name.replace("'", "")
                            metric_name = metric_name.replace('"', "")
                            metric_name = metric_name.replace("(", "")
                            metric_name = metric_name.replace(")", "")
                            metric_name = metric_name.replace(" ", "_")

                    # Apply standard cleaning to all metric names
                    if not (
                        "precision_summary" in metric_jsonpath
                        and "*" in metric_jsonpath
                        and use_metric_context_path
                    ):
                        metric_name = metric_name.replace("'", "")
                        metric_name = metric_name.replace('"', "")
                        metric_name = metric_name.replace("(", "")
                        metric_name = metric_name.replace(")", "")
                        metric_name = metric_name.replace(" ", "_")

                    results_matrix.append(
                        [
                            metric_jsonpath,
                            metric_context_path,
                            metric_name,
                            metric_value,
                            test_case_targets_dict,
                            use_metric_context_path,
                        ]
                    )

            else:
                logging.warning(
                    "Unable to find metric path {} in result dict".format(jsonpath)
                )
    return results_matrix


def get_ts_tags_and_name(
    break_by_key,
    break_by_str,
    break_by_value,
    build_variant_name,
    deployment_name,
    deployment_type,
    metadata_tags,
    metric_context_path,
    metric_jsonpath,
    metric_name,
    running_platform,
    test_name,
    testcase_metric_context_paths,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    use_metric_context_path,
):
    # prepare tags
    timeserie_tags = get_project_ts_tags(
        tf_github_org,
        tf_github_repo,
        deployment_name,
        deployment_type,
        tf_triggering_env,
        metadata_tags,
        build_variant_name,
        running_platform,
    )
    timeserie_tags[break_by_key] = break_by_value
    timeserie_tags["{}+{}".format("deployment_name", break_by_key)] = "{} {}".format(
        deployment_name, break_by_value
    )
    timeserie_tags[break_by_key] = break_by_value
    timeserie_tags["{}+{}".format("target", break_by_key)] = "{} {}".format(
        break_by_value, tf_github_repo
    )
    timeserie_tags["test_name"] = str(test_name)
    if build_variant_name is not None:
        timeserie_tags["test_name:build_variant"] = "{}:{}".format(
            test_name, build_variant_name
        )
    timeserie_tags["metric"] = str(metric_name)
    timeserie_tags["metric_name"] = metric_name
    timeserie_tags["metric_context_path"] = metric_context_path
    if metric_context_path is not None:
        timeserie_tags["test_name:metric_context_path"] = "{}:{}".format(
            test_name, metric_context_path
        )
    timeserie_tags["metric_jsonpath"] = metric_jsonpath
    if metric_context_path not in testcase_metric_context_paths:
        testcase_metric_context_paths.append(metric_context_path)
    ts_name = get_ts_metric_name(
        break_by_str,
        break_by_value,
        tf_github_org,
        tf_github_repo,
        deployment_name,
        deployment_type,
        test_name,
        tf_triggering_env,
        metric_name,
        metric_context_path,
        use_metric_context_path,
        build_variant_name,
        running_platform,
    )
    return timeserie_tags, ts_name


def from_metric_kv_to_timeserie(
    break_by_key,
    break_by_str,
    break_by_value,
    build_variant_name,
    datapoints_timestamp,
    deployment_name,
    deployment_type,
    metadata_tags,
    metric_context_path,
    metric_jsonpath,
    metric_name,
    metric_value,
    running_platform,
    test_case_targets_dict,
    test_name,
    testcase_metric_context_paths,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    time_series_dict,
    use_metric_context_path,
):
    timeserie_tags, ts_name = get_ts_tags_and_name(
        break_by_key,
        break_by_str,
        break_by_value,
        build_variant_name,
        deployment_name,
        deployment_type,
        metadata_tags,
        metric_context_path,
        metric_jsonpath,
        metric_name,
        running_platform,
        test_name,
        testcase_metric_context_paths,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        use_metric_context_path,
    )
    logging.info(f"Adding timeserie named {ts_name} to time_series_dict.")
    time_series_dict[ts_name] = {
        "labels": timeserie_tags.copy(),
        "data": {datapoints_timestamp: metric_value},
    }

    original_ts_name = ts_name
    target_table_keyname = "target_tables:{triggering_env}:ci.benchmarks.redis/{break_by_key}/{break_by_str}/{tf_github_org}/{tf_github_repo}/{deployment_type}/{deployment_name}/{test_name}/{metric_name}".format(
        triggering_env=tf_triggering_env,
        break_by_key=break_by_key,
        break_by_str=break_by_str,
        tf_github_org=tf_github_org,
        tf_github_repo=tf_github_repo,
        deployment_name=deployment_name,
        deployment_type=deployment_type,
        test_name=test_name,
        metric_name=metric_name,
    )
    target_table_dict = {
        "test-case": test_name,
        "metric-name": metric_name,
        tf_github_repo: metric_value,
        "contains-target": False,
    }
    for target_name, target_value in test_case_targets_dict.items():
        target_table_dict["contains-target"] = True
        ts_name = original_ts_name + "/target/{}".format(target_name)
        timeserie_tags_target = timeserie_tags.copy()
        timeserie_tags_target["is_target"] = "true"
        timeserie_tags_target["{}+{}".format("target", break_by_key)] = "{} {}".format(
            break_by_value, target_name
        )
        time_series_dict[ts_name] = {
            "labels": timeserie_tags_target,
            "data": {datapoints_timestamp: target_value},
        }
        if "overallQuantiles" in metric_name:
            comparison_type = "(lower-better)"
        else:
            comparison_type = "(higher-better)"
        if comparison_type == "(higher-better)":
            target_value_pct = (
                (float(metric_value) / float(target_value)) - 1.0
            ) * 100.0
        else:
            target_value_pct = (
                (float(target_value) / float(metric_value)) - 1.0
            ) * 100.0

        target_value_pct_str = "{:.2f}".format(target_value_pct)

        target_table_dict[target_name] = target_value

        target_table_dict["{}:percent {}".format(target_name, comparison_type)] = (
            target_value_pct_str
        )
    return target_table_keyname, target_table_dict


def common_timeseries_extraction(
    break_by_key,
    break_by_str,
    datapoints_timestamp,
    deployment_name,
    deployment_type,
    metrics,
    break_by_value,
    results_dict,
    test_name,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
    testcase_metric_context_paths=[],
):
    time_series_dict = {}
    target_tables = {}
    cleaned_metrics_arr = extract_results_table(metrics, results_dict)
    total_metrics = len(cleaned_metrics_arr)
    logging.info(f"Total of {total_metrics} cleaned metrics: {cleaned_metrics_arr}")
    for cleaned_metric in cleaned_metrics_arr:

        metric_jsonpath = cleaned_metric[0]
        metric_context_path = cleaned_metric[1]
        metric_name = cleaned_metric[2]
        metric_value = cleaned_metric[3]
        test_case_targets_dict = cleaned_metric[4]
        use_metric_context_path = cleaned_metric[5]

        target_table_keyname, target_table_dict = from_metric_kv_to_timeserie(
            break_by_key,
            break_by_str,
            break_by_value,
            build_variant_name,
            datapoints_timestamp,
            deployment_name,
            deployment_type,
            metadata_tags,
            metric_context_path,
            metric_jsonpath,
            metric_name,
            metric_value,
            running_platform,
            test_case_targets_dict,
            test_name,
            testcase_metric_context_paths,
            tf_github_org,
            tf_github_repo,
            tf_triggering_env,
            time_series_dict,
            use_metric_context_path,
        )
        target_tables[target_table_keyname] = target_table_dict

    return time_series_dict, target_tables


def extract_perhash_timeseries_from_results(
    datapoints_timestamp: int,
    metrics: list,
    results_dict: dict,
    git_hash: str,
    tf_github_org: str,
    tf_github_repo: str,
    deployment_name: str,
    deployment_type: str,
    test_name: str,
    tf_triggering_env: str,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
    testcase_metric_context_paths=[],
):
    break_by_key = "hash"
    break_by_str = "by.{}".format(break_by_key)
    (
        time_series_dict,
        target_tables,
    ) = common_timeseries_extraction(
        break_by_key,
        break_by_str,
        datapoints_timestamp,
        deployment_name,
        deployment_type,
        metrics,
        git_hash,
        results_dict,
        test_name,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        metadata_tags,
        build_variant_name,
        running_platform,
        testcase_metric_context_paths,
    )
    return True, time_series_dict, target_tables


def extract_perversion_timeseries_from_results(
    datapoints_timestamp: int,
    metrics: list,
    results_dict: dict,
    project_version: str,
    tf_github_org: str,
    tf_github_repo: str,
    deployment_name: str,
    deployment_type: str,
    test_name: str,
    tf_triggering_env: str,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
    testcase_metric_context_paths=[],
):
    break_by_key = "version"
    break_by_str = "by.{}".format(break_by_key)
    (
        branch_time_series_dict,
        target_tables,
    ) = common_timeseries_extraction(
        break_by_key,
        break_by_str,
        datapoints_timestamp,
        deployment_name,
        deployment_type,
        metrics,
        project_version,
        results_dict,
        test_name,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        metadata_tags,
        build_variant_name,
        running_platform,
        testcase_metric_context_paths,
    )
    return True, branch_time_series_dict, target_tables


def push_data_to_redistimeseries(rts, time_series_dict: dict, expire_msecs=0):
    datapoint_errors = 0
    datapoint_inserts = 0
    if rts is not None and time_series_dict is not None:
        progress = tqdm(
            unit="benchmark time-series", total=len(time_series_dict.values())
        )
        for timeseries_name, time_series in time_series_dict.items():
            exporter_create_ts(rts, time_series, timeseries_name)
            for orig_timestamp, value in time_series["data"].items():
                if orig_timestamp is None:
                    logging.warning("The provided timestamp is null. Using auto-ts")
                    timestamp = "*"
                else:
                    timestamp = orig_timestamp

                try_to_insert = True
                retry_count = 0
                while try_to_insert and retry_count < 100:
                    # (try to) insert the datapoint in given timestamp
                    try_to_insert = False

                    try:
                        rts.ts().add(
                            timeseries_name,
                            timestamp,
                            value,
                            duplicate_policy="block",
                        )
                        datapoint_inserts += 1
                    except redis.exceptions.DataError:
                        logging.warning(
                            "Error while inserting datapoint ({} : {}) in timeseries named {}. ".format(
                                timestamp, value, timeseries_name
                            )
                        )
                        datapoint_errors += 1
                    except redis.exceptions.ResponseError as e:
                        if "DUPLICATE_POLICY" in e.__str__():
                            # duplicate timestamp: try to insert again, but in the next milisecond
                            timestamp += 1
                            try_to_insert = True
                            retry_count += 1
                        else:
                            logging.warning(
                                "Error while inserting datapoint ({} : {}) in timeseries named {}. ".format(
                                    timestamp, value, timeseries_name
                                )
                            )
                            datapoint_errors += 1
            if expire_msecs > 0:
                rts.pexpire(timeseries_name, expire_msecs)
            progress.update()
    return datapoint_errors, datapoint_inserts


def extract_perbranch_timeseries_from_results(
    datapoints_timestamp: int,
    metrics: list,
    results_dict: dict,
    tf_github_branch: str,
    tf_github_org: str,
    tf_github_repo: str,
    deployment_name: str,
    deployment_type: str,
    test_name: str,
    tf_triggering_env: str,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
    testcase_metric_context_paths=[],
):
    break_by_key = "branch"
    break_by_str = "by.{}".format(break_by_key)
    (branch_time_series_dict, target_tables) = common_timeseries_extraction(
        break_by_key,
        break_by_str,
        datapoints_timestamp,
        deployment_name,
        deployment_type,
        metrics,
        tf_github_branch,
        results_dict,
        test_name,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        metadata_tags,
        build_variant_name,
        running_platform,
        testcase_metric_context_paths,
    )
    return True, branch_time_series_dict, target_tables


def check_rts_labels(rts, time_series, timeseries_name):
    updated_create = False
    logging.debug(
        "Timeseries named {} already exists. Checking that the labels match.".format(
            timeseries_name
        )
    )
    set1 = set(time_series["labels"].items())
    set2 = set(rts.ts().info(timeseries_name).labels.items())
    if len(set1 - set2) > 0 or len(set2 - set1) > 0:
        logging.info(
            "Given the labels don't match using TS.ALTER on {} to update labels to {}".format(
                timeseries_name, time_series["labels"]
            )
        )
        updated_create = True
        rts.ts().alter(timeseries_name, labels=time_series["labels"])
    return updated_create


def exporter_create_ts(rts, time_series, timeseries_name):
    updated_create = False
    try:
        if rts.exists(timeseries_name):
            updated_create = check_rts_labels(rts, time_series, timeseries_name)
        else:
            logging.debug(
                "Creating timeseries named {} with labels {}".format(
                    timeseries_name, time_series["labels"]
                )
            )
            rts.ts().create(
                timeseries_name, labels=time_series["labels"], chunk_size=128
            )
            updated_create = True

    except redis.exceptions.ResponseError as e:
        if "already exists" in e.__str__():
            updated_create = check_rts_labels(rts, time_series, timeseries_name)
            pass
        else:
            logging.error(
                "While creating timeseries named {} with the following labels: {} this error ocurred: {}".format(
                    timeseries_name, time_series["labels"], e.__str__()
                )
            )
            raise
    return updated_create


def get_overall_dashboard_keynames(
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    build_variant_name=None,
    running_platform=None,
    test_name=None,
):
    build_variant_str = ""
    if build_variant_name is not None:
        build_variant_str = "/{}".format(build_variant_name)
    running_platform_str = ""
    if running_platform is not None:
        running_platform_str = "/{}".format(running_platform)
    sprefix = (
        "ci.benchmarks.redis/"
        + "{triggering_env}/{github_org}/{github_repo}".format(
            triggering_env=tf_triggering_env,
            github_org=tf_github_org,
            github_repo=tf_github_repo,
        )
    )
    testcases_setname = "{}:testcases".format(sprefix)
    deployment_name_setname = "{}:deployment_names".format(sprefix)
    project_archs_setname = "{}:archs".format(sprefix)
    project_oss_setname = "{}:oss".format(sprefix)
    project_branches_setname = "{}:branches".format(sprefix)
    project_versions_setname = "{}:versions".format(sprefix)
    project_compilers_setname = "{}:compilers".format(sprefix)
    running_platforms_setname = "{}:platforms".format(sprefix)
    build_variant_setname = "{}:build_variants".format(sprefix)
    build_variant_prefix = "{sprefix}{build_variant_str}".format(
        sprefix=sprefix,
        build_variant_str=build_variant_str,
    )
    prefix = "{build_variant_prefix}{running_platform_str}".format(
        build_variant_prefix=build_variant_prefix,
        running_platform_str=running_platform_str,
    )
    tsname_project_total_success = "{}:total_success".format(
        prefix,
    )
    tsname_project_total_failures = "{}:total_failures".format(
        prefix,
    )
    testcases_metric_context_path_setname = ""
    if test_name is not None:
        testcases_metric_context_path_setname = (
            "{testcases_setname}:metric_context_path:{test_name}".format(
                testcases_setname=testcases_setname, test_name=test_name
            )
        )
    testcases_and_metric_context_path_setname = (
        "{testcases_setname}_AND_metric_context_path".format(
            testcases_setname=testcases_setname
        )
    )
    return (
        prefix,
        testcases_setname,
        deployment_name_setname,
        tsname_project_total_failures,
        tsname_project_total_success,
        running_platforms_setname,
        build_variant_setname,
        testcases_metric_context_path_setname,
        testcases_and_metric_context_path_setname,
        project_archs_setname,
        project_oss_setname,
        project_branches_setname,
        project_versions_setname,
        project_compilers_setname,
    )


def get_project_ts_tags(
    tf_github_org: str,
    tf_github_repo: str,
    deployment_name: str,
    deployment_type: str,
    tf_triggering_env: str,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
):
    tags = {
        "github_org": tf_github_org,
        "github_repo": tf_github_repo,
        "github_org/github_repo": "{}/{}".format(tf_github_org, tf_github_repo),
        "deployment_type": deployment_type,
        "deployment_name": deployment_name,
        "triggering_env": tf_triggering_env,
    }
    if build_variant_name is not None:
        tags["build_variant"] = build_variant_name
    if running_platform is not None:
        tags["running_platform"] = running_platform
    for k, v in metadata_tags.items():
        tags[k] = str(v)
    return tags


def common_exporter_logic(
    deployment_name,
    deployment_type,
    exporter_timemetric_path,
    metrics,
    results_dict,
    test_name,
    tf_github_branch,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    artifact_version="N/A",
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
    datapoints_timestamp=None,
    git_hash=None,
):
    per_version_time_series_dict = {}
    per_branch_time_series_dict = {}
    per_hash_time_series_dict = {}
    testcase_metric_context_paths = []
    version_target_tables = None
    branch_target_tables = None
    hash_target_tables = None
    used_ts = datapoints_timestamp

    if exporter_timemetric_path is not None and used_ts is None:
        # extract timestamp
        used_ts = parse_exporter_timemetric(exporter_timemetric_path, results_dict)

    if used_ts is None:
        used_ts = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000.0)
        logging.warning(
            "Error while trying to parse datapoints timestamp. Using current system timestamp Error: {}".format(
                used_ts
            )
        )
    assert used_ts is not None
    total_break_by_added = 0
    if (git_hash is not None) and (git_hash != ""):
        # extract per-hash datapoints
        (
            _,
            per_hash_time_series_dict,
            version_target_tables,
        ) = extract_perhash_timeseries_from_results(
            used_ts,
            metrics,
            results_dict,
            git_hash,
            tf_github_org,
            tf_github_repo,
            deployment_name,
            deployment_type,
            test_name,
            tf_triggering_env,
            metadata_tags,
            build_variant_name,
            running_platform,
            testcase_metric_context_paths,
        )
        total_break_by_added += 1
    else:
        logging.warning(
            "there was no git hash information to push data brokedown by hash"
        )
    if (
        artifact_version is not None
        and artifact_version != ""
        and artifact_version != "N/A"
    ):
        # Check if version 255.255.255 should only be pushed for unstable branch
        should_push_version = True
        if artifact_version == "255.255.255":
            if tf_github_branch != "unstable":
                logging.info(
                    f"Skipping version 255.255.255 data push for branch '{tf_github_branch}' "
                    f"(only pushing for 'unstable' branch)"
                )
                should_push_version = False
            else:
                logging.info(f"Pushing version 255.255.255 data for unstable branch")

        if should_push_version:
            # extract per-version datapoints
            total_hs_ts = len(per_hash_time_series_dict.keys())
            logging.info(
                f"Extending the by.hash {git_hash} timeseries ({total_hs_ts}) with version info {artifact_version}"
            )
            for hash_timeserie in per_hash_time_series_dict.values():
                hash_timeserie["labels"]["version"] = artifact_version
            (
                _,
                per_version_time_series_dict,
                version_target_tables,
            ) = extract_perversion_timeseries_from_results(
                used_ts,
                metrics,
                results_dict,
                artifact_version,
                tf_github_org,
                tf_github_repo,
                deployment_name,
                deployment_type,
                test_name,
                tf_triggering_env,
                metadata_tags,
                build_variant_name,
                running_platform,
                testcase_metric_context_paths,
            )
            total_break_by_added += 1
    else:
        logging.warning(
            "there was no git VERSION information to push data brokedown by VERSION"
        )
    if tf_github_branch is not None and tf_github_branch != "":
        total_hs_ts = len(per_hash_time_series_dict.keys())
        logging.info(
            f"Extending the by.hash {git_hash} timeseries ({total_hs_ts}) with branch info {tf_github_branch}"
        )
        for hash_timeserie in per_hash_time_series_dict.values():
            hash_timeserie["labels"]["branch"] = tf_github_branch
        # extract per branch datapoints
        (
            _,
            per_branch_time_series_dict,
            branch_target_tables,
        ) = extract_perbranch_timeseries_from_results(
            used_ts,
            metrics,
            results_dict,
            str(tf_github_branch),
            tf_github_org,
            tf_github_repo,
            deployment_name,
            deployment_type,
            test_name,
            tf_triggering_env,
            metadata_tags,
            build_variant_name,
            running_platform,
            testcase_metric_context_paths,
        )
        total_break_by_added += 1
    else:
        logging.warning(
            "there was no git BRANCH information to push data brokedown by BRANCH"
        )
    if total_break_by_added == 0:
        logging.error(
            "There was no BRANCH, HASH, or VERSION info to break this info by in timeseries"
        )
    return (
        per_version_time_series_dict,
        per_branch_time_series_dict,
        per_hash_time_series_dict,
        testcase_metric_context_paths,
        version_target_tables,
        branch_target_tables,
        hash_target_tables,
    )


def merge_default_and_config_metrics(
    benchmark_config, default_metrics, exporter_timemetric_path
):
    if default_metrics is None:
        default_metrics = []
    metrics = default_metrics
    if benchmark_config is not None:
        if "exporter" in benchmark_config:
            extra_metrics = parse_exporter_metrics_definition(
                benchmark_config["exporter"]
            )
            metrics.extend(extra_metrics)
            extra_timemetric_path = parse_exporter_timemetric_definition(
                benchmark_config["exporter"]
            )
            if extra_timemetric_path is not None:
                exporter_timemetric_path = extra_timemetric_path
    return exporter_timemetric_path, metrics


def get_profilers_rts_key_prefix(triggering_env, tf_github_org, tf_github_repo):
    zset_name = "ci.benchmarks.redis.com/{triggering_env}/{github_org}/{github_repo}:profiles".format(
        triggering_env=triggering_env,
        github_org=tf_github_org,
        github_repo=tf_github_repo,
    )
    return zset_name


def prepare_timeseries_dict(
    artifact_version,
    benchmark_config,
    default_metrics,
    deployment_name,
    deployment_type,
    exporter_timemetric_path,
    results_dict,
    test_name,
    tf_github_branch,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
    datapoints_timestamp=None,
    git_hash=None,
):
    time_series_dict = {}
    # check which metrics to extract
    exporter_timemetric_path, metrics = merge_default_and_config_metrics(
        benchmark_config, default_metrics, exporter_timemetric_path
    )
    (
        per_version_time_series_dict,
        per_branch_time_series_dict,
        per_hash_timeseries_dict,
        testcase_metric_context_paths,
        version_target_tables,
        branch_target_tables,
        _,
    ) = common_exporter_logic(
        deployment_name,
        deployment_type,
        exporter_timemetric_path,
        metrics,
        results_dict,
        test_name,
        tf_github_branch,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        artifact_version,
        metadata_tags,
        build_variant_name,
        running_platform,
        datapoints_timestamp,
        git_hash,
    )
    time_series_dict.update(per_version_time_series_dict)
    time_series_dict.update(per_branch_time_series_dict)
    time_series_dict.update(per_hash_timeseries_dict)
    return (
        time_series_dict,
        testcase_metric_context_paths,
        version_target_tables,
        branch_target_tables,
    )


def add_standardized_metric_bybranch(
    metric_name,
    metric_value,
    tf_github_branch,
    deployment_name,
    deployment_type,
    rts,
    start_time_ms,
    test_name,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
):
    if metric_value is not None:
        tsname_use_case_duration = get_ts_metric_name(
            "by.branch",
            tf_github_branch,
            tf_github_org,
            tf_github_repo,
            deployment_name,
            deployment_type,
            test_name,
            tf_triggering_env,
            metric_name,
            None,
            False,
            build_variant_name,
            running_platform,
        )
        labels = get_project_ts_tags(
            tf_github_org,
            tf_github_repo,
            deployment_name,
            deployment_type,
            tf_triggering_env,
            metadata_tags,
            build_variant_name,
            running_platform,
        )
        labels["branch"] = tf_github_branch
        labels["deployment_name+branch"] = "{} {}".format(
            deployment_name, tf_github_branch
        )
        labels["running_platform+branch"] = "{} {}".format(
            running_platform, tf_github_branch
        )
        labels["test_name"] = str(test_name)
        labels["metric"] = str(metric_name)
        logging.info(
            "Adding metric {}={} to time-serie named {}".format(
                metric_name, metric_value, tsname_use_case_duration
            )
        )
        ts = {"labels": labels}
        exporter_create_ts(rts, ts, tsname_use_case_duration)
        logging.error(labels)
        rts.ts().add(
            tsname_use_case_duration,
            start_time_ms,
            metric_value,
            labels=labels,
        )
    else:
        logging.warning(
            "Given that metric {}={} ( is None ) we will skip adding it to timeseries".format(
                metric_name, metric_value
            )
        )


def add_standardized_metric_byversion(
    metric_name,
    metric_value,
    artifact_version,
    deployment_name,
    deployment_type,
    rts,
    start_time_ms,
    test_name,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
):
    if metric_value is not None:
        tsname_use_case_duration = get_ts_metric_name(
            "by.version",
            artifact_version,
            tf_github_org,
            tf_github_repo,
            deployment_name,
            deployment_type,
            test_name,
            tf_triggering_env,
            metric_name,
            None,
            False,
            build_variant_name,
            running_platform,
        )
        labels = get_project_ts_tags(
            tf_github_org,
            tf_github_repo,
            deployment_name,
            deployment_type,
            tf_triggering_env,
            metadata_tags,
            build_variant_name,
            running_platform,
        )
        labels["version"] = artifact_version
        labels["deployment_name+version"] = "{} {}".format(
            deployment_name, artifact_version
        )
        labels["running_platform+version"] = "{} {}".format(
            running_platform, artifact_version
        )
        labels["test_name"] = str(test_name)
        labels["metric"] = str(metric_name)
        logging.info(
            "Adding metric {}={} to time-serie named {}".format(
                metric_name, metric_value, tsname_use_case_duration
            )
        )
        ts = {"labels": labels}
        exporter_create_ts(rts, ts, tsname_use_case_duration)
        rts.ts().add(
            tsname_use_case_duration,
            start_time_ms,
            metric_value,
            labels=labels,
        )
    else:
        logging.warning(
            "Given that metric {}={} ( is None ) we will skip adding it to timeseries".format(
                metric_name, metric_value
            )
        )


def timeseries_test_sucess_flow(
    push_results_redistimeseries,
    artifact_version,
    benchmark_config,
    benchmark_duration_seconds,
    dataset_load_duration_seconds,
    default_metrics,
    deployment_name,
    deployment_type,
    exporter_timemetric_path,
    results_dict,
    rts,
    start_time_ms,
    test_name,
    tf_github_branch,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
    timeseries_dict=None,
    git_hash=None,
    disable_target_tables=False,
):
    testcase_metric_context_paths = []
    version_target_tables = None
    branch_target_tables = None
    if timeseries_dict is None:
        (
            timeseries_dict,
            testcase_metric_context_paths,
            version_target_tables,
            branch_target_tables,
        ) = prepare_timeseries_dict(
            artifact_version,
            benchmark_config,
            default_metrics,
            deployment_name,
            deployment_type,
            exporter_timemetric_path,
            results_dict,
            test_name,
            tf_github_branch,
            tf_github_org,
            tf_github_repo,
            tf_triggering_env,
            metadata_tags,
            build_variant_name,
            running_platform,
            start_time_ms,
            git_hash,
        )
    if push_results_redistimeseries:
        logging.info(
            "Pushing results to RedisTimeSeries. Have {} distinct data-points to insert.".format(
                len(timeseries_dict.keys())
            )
        )
        push_data_to_redistimeseries(rts, timeseries_dict)
        if not disable_target_tables and version_target_tables is not None:
            logging.info(
                "There are a total of {} distinct target tables by version".format(
                    len(version_target_tables.keys())
                )
            )
            for (
                version_target_table_keyname,
                version_target_table_dict,
            ) in version_target_tables.items():
                logging.info(
                    "Setting target table by version on key {}".format(
                        version_target_table_keyname
                    )
                )
                if "contains-target" in version_target_table_dict:
                    del version_target_table_dict["contains-target"]
                rts.hset(
                    version_target_table_keyname, None, None, version_target_table_dict
                )
        elif disable_target_tables:
            logging.info(
                "Target tables disabled - skipping version target table creation"
            )

        if not disable_target_tables and branch_target_tables is not None:
            logging.info(
                "There are a total of {} distinct target tables by branch".format(
                    len(branch_target_tables.keys())
                )
            )
            for (
                branch_target_table_keyname,
                branch_target_table_dict,
            ) in branch_target_tables.items():

                logging.info(
                    "Setting target table by branch on key {}".format(
                        branch_target_table_keyname
                    )
                )
                if "contains-target" in branch_target_table_dict:
                    del branch_target_table_dict["contains-target"]
                rts.hset(
                    branch_target_table_keyname, None, None, branch_target_table_dict
                )
        elif disable_target_tables:
            logging.info(
                "Target tables disabled - skipping branch target table creation"
            )
        if test_name is not None:
            if type(test_name) is str:
                update_secondary_result_keys(
                    artifact_version,
                    benchmark_duration_seconds,
                    build_variant_name,
                    dataset_load_duration_seconds,
                    deployment_name,
                    deployment_type,
                    metadata_tags,
                    rts,
                    running_platform,
                    start_time_ms,
                    test_name,
                    testcase_metric_context_paths,
                    tf_github_branch,
                    tf_github_org,
                    tf_github_repo,
                    tf_triggering_env,
                )
            if type(test_name) is list:
                for inner_test_name in test_name:
                    update_secondary_result_keys(
                        artifact_version,
                        benchmark_duration_seconds,
                        build_variant_name,
                        dataset_load_duration_seconds,
                        deployment_name,
                        deployment_type,
                        metadata_tags,
                        rts,
                        running_platform,
                        start_time_ms,
                        inner_test_name,
                        testcase_metric_context_paths,
                        tf_github_branch,
                        tf_github_org,
                        tf_github_repo,
                        tf_triggering_env,
                    )
        else:
            update_secondary_result_keys(
                artifact_version,
                benchmark_duration_seconds,
                build_variant_name,
                dataset_load_duration_seconds,
                deployment_name,
                deployment_type,
                metadata_tags,
                rts,
                running_platform,
                start_time_ms,
                test_name,
                testcase_metric_context_paths,
                tf_github_branch,
                tf_github_org,
                tf_github_repo,
                tf_triggering_env,
            )
    return version_target_tables, branch_target_tables


def update_secondary_result_keys(
    artifact_version,
    benchmark_duration_seconds,
    build_variant_name,
    dataset_load_duration_seconds,
    deployment_name,
    deployment_type,
    metadata_tags,
    rts,
    running_platform,
    start_time_ms,
    test_name,
    testcase_metric_context_paths,
    tf_github_branch,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
):
    (
        _,
        testcases_setname,
        deployment_name_zsetname,
        _,
        tsname_project_total_success,
        running_platforms_setname,
        build_variant_setname,
        testcases_metric_context_path_setname,
        testcases_and_metric_context_path_setname,
        project_archs_setname,
        project_oss_setname,
        project_branches_setname,
        project_versions_setname,
        project_compilers_setname,
    ) = get_overall_dashboard_keynames(
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        build_variant_name,
        running_platform,
        test_name,
    )
    try:
        rts.zadd(deployment_name_zsetname, {deployment_name: start_time_ms})
        if test_name is not None:
            deployment_name_zsetname_testnames = (
                deployment_name_zsetname
                + "{}:deployment_name={}".format(
                    deployment_name_zsetname, deployment_name
                )
            )
            rts.zadd(deployment_name_zsetname_testnames, {test_name: start_time_ms})
            rts.sadd(testcases_setname, test_name)
            testcases_zsetname = testcases_setname + ":zset"
            rts.zadd(testcases_zsetname, {test_name: start_time_ms})
            if "component" in metadata_tags:
                testcases_zsetname_component = "{}:zset:component:{}".format(
                    testcases_setname, metadata_tags["component"]
                )
                rts.zadd(testcases_zsetname_component, {test_name: start_time_ms})
        if "arch" in metadata_tags:
            rts.sadd(project_archs_setname, metadata_tags["arch"])
        if "os" in metadata_tags:
            rts.sadd(project_oss_setname, metadata_tags["os"])
        if "compiler" in metadata_tags:
            rts.sadd(project_compilers_setname, metadata_tags["compiler"])
        if tf_github_branch is not None and tf_github_branch != "":
            rts.sadd(project_branches_setname, tf_github_branch)
            project_branches_zsetname = project_branches_setname + ":zset"
            rts.zadd(project_branches_zsetname, {tf_github_branch: start_time_ms})
        if artifact_version is not None and artifact_version != "":
            rts.sadd(project_versions_setname, artifact_version)
            project_versions_zsetname = project_versions_setname + ":zset"
            rts.zadd(project_versions_zsetname, {artifact_version: start_time_ms})
        if running_platform is not None:
            rts.sadd(running_platforms_setname, running_platform)
            running_platforms_szetname = running_platforms_setname + ":zset"
            rts.zadd(running_platforms_szetname, {running_platform: start_time_ms})
        if build_variant_name is not None:
            rts.sadd(build_variant_setname, build_variant_name)
            build_variant_zsetname = build_variant_setname + ":zset"
            rts.zadd(build_variant_zsetname, {build_variant_name: start_time_ms})
        if testcase_metric_context_paths is not None:
            for metric_context_path in testcase_metric_context_paths:
                if testcases_metric_context_path_setname != "":
                    rts.sadd(testcases_metric_context_path_setname, metric_context_path)
                    rts.sadd(
                        testcases_and_metric_context_path_setname,
                        "{}:{}".format(test_name, metric_context_path),
                    )
        rts.ts().incrby(
            tsname_project_total_success,
            1,
            timestamp=start_time_ms,
            labels=get_project_ts_tags(
                tf_github_org,
                tf_github_repo,
                deployment_name,
                deployment_type,
                tf_triggering_env,
                metadata_tags,
                build_variant_name,
                running_platform,
            ),
        )
        if tf_github_branch is not None and tf_github_branch != "":
            add_standardized_metric_bybranch(
                "benchmark_duration",
                benchmark_duration_seconds,
                str(tf_github_branch),
                deployment_name,
                deployment_type,
                rts,
                start_time_ms,
                test_name,
                tf_github_org,
                tf_github_repo,
                tf_triggering_env,
                metadata_tags,
                build_variant_name,
                running_platform,
            )
            add_standardized_metric_bybranch(
                "dataset_load_duration",
                dataset_load_duration_seconds,
                str(tf_github_branch),
                deployment_name,
                deployment_type,
                rts,
                start_time_ms,
                test_name,
                tf_github_org,
                tf_github_repo,
                tf_triggering_env,
                metadata_tags,
                build_variant_name,
                running_platform,
            )
        if artifact_version is not None and artifact_version != "":
            add_standardized_metric_byversion(
                "benchmark_duration",
                benchmark_duration_seconds,
                artifact_version,
                deployment_name,
                deployment_type,
                rts,
                start_time_ms,
                test_name,
                tf_github_org,
                tf_github_repo,
                tf_triggering_env,
                metadata_tags,
                build_variant_name,
                running_platform,
            )
            add_standardized_metric_byversion(
                "dataset_load_duration",
                dataset_load_duration_seconds,
                artifact_version,
                deployment_name,
                deployment_type,
                rts,
                start_time_ms,
                test_name,
                tf_github_org,
                tf_github_repo,
                tf_triggering_env,
                metadata_tags,
                build_variant_name,
                running_platform,
            )
    except redis.exceptions.ResponseError as e:
        logging.warning(
            "Error while updating secondary data structures {}. ".format(e.__str__())
        )
        pass


def get_start_time_vars(start_time=None):
    if start_time is None:
        start_time = datetime.datetime.utcnow()
    start_time_ms = int(
        (start_time - datetime.datetime(1970, 1, 1)).total_seconds() * 1000
    )
    start_time_str = start_time.strftime("%Y-%m-%d-%H-%M-%S")
    return start_time, start_time_ms, start_time_str


def timeseries_test_failure_flow(
    args,
    deployment_name,
    deployment_type,
    rts,
    start_time_ms,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    tsname_project_total_failures,
):
    if args.push_results_redistimeseries:
        if start_time_ms is None:
            _, start_time_ms, _ = get_start_time_vars()
        try:
            rts.ts().incrby(
                tsname_project_total_failures,
                1,
                timestamp=start_time_ms,
                labels=get_project_ts_tags(
                    tf_github_org,
                    tf_github_repo,
                    deployment_name,
                    deployment_type,
                    tf_triggering_env,
                ),
            )
        except redis.exceptions.ResponseError as e:
            logging.warning(
                "Error while updating secondary data structures {}. ".format(
                    e.__str__()
                )
            )
            pass


def datasink_profile_tabular_data(
    github_branch,
    github_org_name,
    github_repo_name,
    github_sha,
    overall_tabular_data_map,
    rts,
    setup_type,
    start_time_ms,
    start_time_str,
    test_name,
    tf_triggering_env,
):
    zset_profiles_key_name = get_profilers_rts_key_prefix(
        tf_triggering_env,
        github_org_name,
        github_repo_name,
    )
    profile_test_suffix = "{start_time_str}:{test_name}/{setup_type}/{github_branch}/{github_hash}".format(
        start_time_str=start_time_str,
        test_name=test_name,
        setup_type=setup_type,
        github_branch=github_branch,
        github_hash=github_sha,
    )
    rts.zadd(
        zset_profiles_key_name,
        {profile_test_suffix: start_time_ms},
    )
    for (
        profile_tabular_type,
        tabular_data,
    ) in overall_tabular_data_map.items():
        tabular_suffix = "{}:{}".format(profile_tabular_type, profile_test_suffix)
        logging.info(
            "Pushing to data-sink tabular data from pprof ({}). Tabular suffix: {}".format(
                profile_tabular_type, tabular_suffix
            )
        )

        table_columns_text_key = "{}:columns:text".format(tabular_suffix)
        table_columns_type_key = "{}:columns:type".format(tabular_suffix)
        logging.info(
            "Pushing list key (named {}) the following column text: {}".format(
                table_columns_text_key, tabular_data["columns:text"]
            )
        )
        rts.rpush(table_columns_text_key, *tabular_data["columns:text"])
        logging.info(
            "Pushing list key (named {}) the following column types: {}".format(
                table_columns_type_key, tabular_data["columns:type"]
            )
        )
        rts.rpush(table_columns_type_key, *tabular_data["columns:type"])
        for row_name in tabular_data["columns:text"]:
            table_row_key = "{}:rows:{}".format(tabular_suffix, row_name)
            row_values = tabular_data["rows:{}".format(row_name)]
            rts.rpush(table_row_key, *row_values)
