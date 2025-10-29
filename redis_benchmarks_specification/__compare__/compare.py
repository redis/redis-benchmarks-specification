#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import datetime
import logging
import re
import pandas as pd
import redis
import yaml
from pytablewriter import MarkdownTableWriter
import humanize
import datetime as dt
import os
from tqdm import tqdm
import argparse
import numpy as np
from concurrent.futures import ThreadPoolExecutor

from io import StringIO
import sys

# Import command categorization function
try:
    from utils.summary import categorize_command
except ImportError:
    # Fallback if utils.summary is not available
    def categorize_command(command):
        return "unknown"


# Optional matplotlib import for box plot generation
try:
    import matplotlib.pyplot as plt

    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    logging.warning("matplotlib not available, box plot generation will be disabled")

from redis_benchmarks_specification.__common__.github import (
    update_comment_if_needed,
    create_new_pr_comment,
    check_github_available_and_actionable,
    check_regression_comment,
)
from redis_benchmarks_specification.__compare__.args import create_compare_arguments

from redis_benchmarks_specification.__common__.runner import get_benchmark_specs

from redis_benchmarks_specification.__common__.package import (
    get_version_string,
    populate_with_poetry_data,
)


WH_TOKEN = os.getenv("PERFORMANCE_WH_TOKEN", None)

LOG_LEVEL = logging.DEBUG
if os.getenv("VERBOSE", "0") == "0":
    LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s %(levelname)-4s %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"


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


def get_start_time_vars(start_time=None):
    if start_time is None:
        start_time = dt.datetime.utcnow()
    start_time_ms = int((start_time - dt.datetime(1970, 1, 1)).total_seconds() * 1000)
    start_time_str = start_time.strftime("%Y-%m-%d-%H-%M-%S")
    return start_time, start_time_ms, start_time_str


def get_project_compare_zsets(triggering_env, org, repo):
    return "ci.benchmarks.redis/{}/{}/{}:compare:pull_requests:zset".format(
        triggering_env, org, repo
    )


def compare_command_logic(args, project_name, project_version):

    logger = logging.getLogger()
    logger.setLevel(LOG_LEVEL)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(LOG_LEVEL)

    # create formatter
    formatter = logging.Formatter(LOG_FORMAT)

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)

    logging.info(
        "Using: {project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )
    logging.info(
        "Checking connection to RedisTimeSeries with user: {}, host: {}, port: {}".format(
            args.redistimeseries_user,
            args.redistimeseries_host,
            args.redistimeseries_port,
        )
    )
    rts = redis.Redis(
        host=args.redistimeseries_host,
        port=args.redistimeseries_port,
        password=args.redistimeseries_pass,
        username=args.redistimeseries_user,
    )
    rts.ping()
    default_baseline_branch, default_metrics_str = extract_default_branch_and_metric(
        args.defaults_filename
    )

    tf_github_org = args.github_org
    tf_github_repo = args.github_repo
    tf_triggering_env = args.triggering_env
    if args.baseline_deployment_name != "":
        baseline_deployment_name = args.baseline_deployment_name
    else:
        baseline_deployment_name = args.deployment_name
    if args.comparison_deployment_name != "":
        comparison_deployment_name = args.comparison_deployment_name
    else:
        comparison_deployment_name = args.deployment_name

    logging.info(
        "Using baseline deployment_name={} and comparison deployment_name={} for the analysis".format(
            baseline_deployment_name,
            comparison_deployment_name,
        )
    )
    from_ts_ms = args.from_timestamp
    to_ts_ms = args.to_timestamp
    from_date = args.from_date
    to_date = args.to_date
    baseline_branch = args.baseline_branch
    if baseline_branch is None and default_baseline_branch is not None:
        logging.info(
            "Given --baseline-branch was null using the default baseline branch {}".format(
                default_baseline_branch
            )
        )
        baseline_branch = default_baseline_branch
    if baseline_branch == "":
        baseline_branch = None
    comparison_branch = args.comparison_branch
    simplify_table = args.simple_table
    print_regressions_only = args.print_regressions_only
    print_improvements_only = args.print_improvements_only
    skip_unstable = args.skip_unstable
    baseline_tag = args.baseline_tag
    comparison_tag = args.comparison_tag
    last_n_baseline = args.last_n
    last_n_comparison = args.last_n
    if last_n_baseline < 0:
        last_n_baseline = args.last_n_baseline
    if last_n_comparison < 0:
        last_n_comparison = args.last_n_comparison
    logging.info("Using last {} samples for baseline analysis".format(last_n_baseline))
    logging.info(
        "Using last {} samples for comparison analysis".format(last_n_comparison)
    )
    verbose = args.verbose
    regressions_percent_lower_limit = args.regressions_percent_lower_limit
    metric_name = args.metric_name
    if (metric_name is None or metric_name == "") and default_metrics_str != "":
        logging.info(
            "Given --metric_name was null using the default metric names {}".format(
                default_metrics_str
            )
        )
        metric_name = default_metrics_str

    if metric_name is None:
        logging.error(
            "You need to provider either "
            + " --metric_name or provide a defaults file via --defaults_filename that contains exporter.redistimeseries.comparison.metrics array. Exiting..."
        )
        exit(1)
    else:
        logging.info("Using metric {}".format(metric_name))

    metric_mode = args.metric_mode
    test = args.test
    use_metric_context_path = args.use_metric_context_path
    github_token = args.github_token
    pull_request = args.pull_request
    testname_regex = args.testname_regex
    auto_approve = args.auto_approve
    running_platform = args.running_platform

    # Handle separate baseline and comparison platform/environment arguments
    # Fall back to general arguments if specific ones are not provided
    running_platform_baseline = args.running_platform_baseline or args.running_platform
    running_platform_comparison = (
        args.running_platform_comparison or args.running_platform
    )
    triggering_env_baseline = args.triggering_env_baseline or args.triggering_env
    triggering_env_comparison = args.triggering_env_comparison or args.triggering_env

    baseline_target_version = args.baseline_target_version
    comparison_target_version = args.comparison_target_version
    baseline_target_branch = args.baseline_target_branch
    comparison_target_branch = args.comparison_target_branch
    baseline_github_repo = args.baseline_github_repo
    comparison_github_repo = args.comparison_github_repo
    baseline_github_org = args.baseline_github_org
    comparison_github_org = args.comparison_github_org
    baseline_hash = args.baseline_hash
    comparison_hash = args.comparison_hash

    # Log platform and environment information
    if running_platform_baseline == running_platform_comparison:
        if running_platform_baseline is not None:
            logging.info(
                "Using platform named: {} for both baseline and comparison.\n\n".format(
                    running_platform_baseline
                )
            )
    else:
        logging.info(
            "Using platform named: {} for baseline and {} for comparison.\n\n".format(
                running_platform_baseline, running_platform_comparison
            )
        )

    if triggering_env_baseline == triggering_env_comparison:
        logging.info(
            "Using triggering environment: {} for both baseline and comparison.".format(
                triggering_env_baseline
            )
        )
    else:
        logging.info(
            "Using triggering environment: {} for baseline and {} for comparison.".format(
                triggering_env_baseline, triggering_env_comparison
            )
        )

    testsuites_folder = os.path.abspath(args.test_suites_folder)
    logging.info("Using test-suites folder dir {}".format(testsuites_folder))
    testsuite_spec_files = get_benchmark_specs(testsuites_folder)
    logging.info(
        "There are a total of {} test-suites being run in folder {}".format(
            len(testsuite_spec_files), testsuites_folder
        )
    )
    tests_with_config = {}
    for test_file in testsuite_spec_files:
        if args.defaults_filename in test_file:
            continue
        benchmark_config = {}
        with open(test_file, "r") as stream:
            try:
                benchmark_config = yaml.safe_load(stream)
                test_name = benchmark_config["name"]
                tests_with_config[test_name] = benchmark_config
                if "tested-groups" in benchmark_config:
                    origin_tested_groups = benchmark_config["tested-groups"]
                else:
                    logging.warn("dont have test groups in {}".format(test_name))
                if "tested-commands" in benchmark_config:
                    origin_tested_commands = benchmark_config["tested-commands"]
                else:
                    logging.warn("dont have test commands in {}".format(test_name))
            except Exception as e:
                logging.error(
                    "while loading file {} and error was returned: {}".format(
                        test_file, e.__str__()
                    )
                )
                pass

    fn = check_regression_comment
    (
        contains_regression_comment,
        github_pr,
        is_actionable_pr,
        old_regression_comment_body,
        pr_link,
        regression_comment,
    ) = check_github_available_and_actionable(
        fn, github_token, pull_request, tf_github_org, tf_github_repo, verbose
    )
    grafana_link_base = "https://benchmarksredisio.grafana.net/d/1fWbtb7nz/experimental-oss-spec-benchmarks"

    (
        detected_regressions,
        table_output,
        improvements_list,
        regressions_list,
        total_stable,
        total_unstable,
        total_comparison_points,
        boxplot_data,
        command_change,
    ) = compute_regression_table(
        rts,
        tf_github_org,
        tf_github_repo,
        triggering_env_baseline,
        triggering_env_comparison,
        metric_name,
        comparison_branch,
        baseline_branch,
        baseline_tag,
        comparison_tag,
        baseline_deployment_name,
        comparison_deployment_name,
        print_improvements_only,
        print_regressions_only,
        skip_unstable,
        regressions_percent_lower_limit,
        simplify_table,
        test,
        testname_regex,
        verbose,
        last_n_baseline,
        last_n_comparison,
        metric_mode,
        from_date,
        from_ts_ms,
        to_date,
        to_ts_ms,
        use_metric_context_path,
        running_platform_baseline,
        running_platform_comparison,
        baseline_target_version,
        comparison_target_version,
        baseline_hash,
        comparison_hash,
        baseline_github_repo,
        comparison_github_repo,
        baseline_target_branch,
        comparison_target_branch,
        baseline_github_org,
        comparison_github_org,
        args.regression_str,
        args.improvement_str,
        tests_with_config,
        args.use_test_suites_folder,
        testsuites_folder,
        args.extra_filters,
        getattr(args, "command_group_regex", ".*"),
        getattr(args, "command_regex", ".*"),
    )
    total_regressions = len(regressions_list)
    total_improvements = len(improvements_list)
    prepare_regression_comment(
        auto_approve,
        baseline_branch,
        baseline_tag,
        comparison_branch,
        comparison_tag,
        contains_regression_comment,
        github_pr,
        grafana_link_base,
        is_actionable_pr,
        old_regression_comment_body,
        pr_link,
        regression_comment,
        rts,
        running_platform_baseline,
        running_platform_comparison,
        table_output,
        tf_github_org,
        tf_github_repo,
        triggering_env_baseline,
        triggering_env_comparison,
        total_comparison_points,
        total_improvements,
        total_regressions,
        total_stable,
        total_unstable,
        verbose,
        args.regressions_percent_lower_limit,
        regressions_list,
        improvements_list,
        args.improvement_str,
        args.regression_str,
    )

    # Generate box plot if requested
    if args.generate_boxplot and command_change:
        if MATPLOTLIB_AVAILABLE:
            logging.info(f"Generating box plot with {len(command_change)} commands...")
            generate_command_performance_boxplot_from_command_data(
                command_change,
                args.boxplot_output,
                args.regression_str,
                args.improvement_str,
                getattr(args, "command_group_regex", ".*"),
            )
        else:
            logging.error(
                "Box plot generation requested but matplotlib is not available"
            )

    return (
        detected_regressions,
        "",
        total_improvements,
        total_regressions,
        total_stable,
        total_unstable,
        total_comparison_points,
    )


def prepare_regression_comment(
    auto_approve,
    baseline_branch,
    baseline_tag,
    comparison_branch,
    comparison_tag,
    contains_regression_comment,
    github_pr,
    grafana_link_base,
    is_actionable_pr,
    old_regression_comment_body,
    pr_link,
    regression_comment,
    rts,
    running_platform_baseline,
    running_platform_comparison,
    table_output,
    tf_github_org,
    tf_github_repo,
    triggering_env_baseline,
    triggering_env_comparison,
    total_comparison_points,
    total_improvements,
    total_regressions,
    total_stable,
    total_unstable,
    verbose,
    regressions_percent_lower_limit,
    regressions_list=[],
    improvements_list=[],
    improvement_str="Improvement",
    regression_str="Regression",
):
    if total_comparison_points > 0:
        comment_body = "### Automated performance analysis summary\n\n"
        comment_body += "This comment was automatically generated given there is performance data available.\n\n"
        # Add platform information to comment
        if running_platform_baseline == running_platform_comparison:
            if running_platform_baseline is not None:
                comment_body += "Using platform named: {} for both baseline and comparison.\n\n".format(
                    running_platform_baseline
                )
        else:
            comment_body += "Using platform named: {} for baseline and {} for comparison.\n\n".format(
                running_platform_baseline, running_platform_comparison
            )

        # Add triggering environment information to comment
        if triggering_env_baseline == triggering_env_comparison:
            comment_body += "Using triggering environment: {} for both baseline and comparison.\n\n".format(
                triggering_env_baseline
            )
        else:
            comment_body += "Using triggering environment: {} for baseline and {} for comparison.\n\n".format(
                triggering_env_baseline, triggering_env_comparison
            )
        comparison_summary = "In summary:\n"
        if total_stable > 0:
            comparison_summary += (
                "- Detected a total of {} stable tests between versions.\n".format(
                    total_stable,
                )
            )

        if total_unstable > 0:
            comparison_summary += (
                "- Detected a total of {} highly unstable benchmarks.\n".format(
                    total_unstable
                )
            )
        if total_improvements > 0:
            comparison_summary += "- Detected a total of {} improvements above the improvement water line ({}).\n".format(
                total_improvements, improvement_str
            )
            if len(improvements_list) > 0:
                improvement_values = [l[1] for l in improvements_list]
                improvement_df = pd.DataFrame(improvement_values)
                median_improvement = round(float(improvement_df.median().iloc[0]), 1)
                max_improvement = round(float(improvement_df.max().iloc[0]), 1)
                min_improvement = round(float(improvement_df.min().iloc[0]), 1)
                p25_improvement = round(float(improvement_df.quantile(0.25).iloc[0]), 1)
                p75_improvement = round(float(improvement_df.quantile(0.75).iloc[0]), 1)

                comparison_summary += f"   - The median improvement ({improvement_str}) was {median_improvement}%, with values ranging from {min_improvement}% to {max_improvement}%.\n"
                comparison_summary += f"   - Quartile distribution: P25={p25_improvement}%, P50={median_improvement}%, P75={p75_improvement}%.\n"

        if total_regressions > 0:
            comparison_summary += "- Detected a total of {} regressions below the regression water line of {} ({}).\n".format(
                total_regressions, regressions_percent_lower_limit, regression_str
            )
            if len(regressions_list) > 0:
                regression_values = [l[1] for l in regressions_list]
                regression_df = pd.DataFrame(regression_values)
                median_regression = round(float(regression_df.median().iloc[0]), 1)
                max_regression = round(float(regression_df.max().iloc[0]), 1)
                min_regression = round(float(regression_df.min().iloc[0]), 1)
                p25_regression = round(float(regression_df.quantile(0.25).iloc[0]), 1)
                p75_regression = round(float(regression_df.quantile(0.75).iloc[0]), 1)

                comparison_summary += f"   - The median regression ({regression_str}) was {median_regression}%, with values ranging from {min_regression}% to {max_regression}%.\n"
                comparison_summary += f"   - Quartile distribution: P25={p25_regression}%, P50={median_regression}%, P75={p75_regression}%.\n"

        comment_body += comparison_summary
        comment_body += "\n"

        if grafana_link_base is not None:
            grafana_link = "{}/".format(grafana_link_base)
            if baseline_tag is not None and comparison_tag is not None:
                grafana_link += "?var-version={}&var-version={}".format(
                    baseline_tag, comparison_tag
                )
            if baseline_branch is not None and comparison_branch is not None:
                grafana_link += "?var-branch={}&var-branch={}".format(
                    baseline_branch, comparison_branch
                )
            comment_body += "You can check a comparison in detail via the [grafana link]({})".format(
                grafana_link
            )

        comment_body += "\n\n##" + table_output
        print(comment_body)

        if is_actionable_pr:
            zset_project_pull_request = get_project_compare_zsets(
                triggering_env_baseline,
                tf_github_org,
                tf_github_repo,
            )
            logging.info(
                "Populating the pull request performance ZSETs: {} with branch {}".format(
                    zset_project_pull_request, comparison_branch
                )
            )
            # Only add to Redis sorted set if comparison_branch is not None
            if comparison_branch is not None:
                _, start_time_ms, _ = get_start_time_vars()
                res = rts.zadd(
                    zset_project_pull_request,
                    {comparison_branch: start_time_ms},
                )
                logging.info(
                    "Result of Populating the pull request performance ZSETs: {} with branch {}: {}".format(
                        zset_project_pull_request, comparison_branch, res
                    )
                )
            else:
                logging.warning(
                    "Skipping Redis ZADD operation because comparison_branch is None"
                )

            if contains_regression_comment:
                update_comment_if_needed(
                    auto_approve,
                    comment_body,
                    old_regression_comment_body,
                    regression_comment,
                    verbose,
                )
            else:
                create_new_pr_comment(auto_approve, comment_body, github_pr, pr_link)

    else:
        logging.error("There was no comparison points to produce a table...")


def extract_default_branch_and_metric(defaults_filename):
    default_baseline_branch = "unstable"
    default_metrics_str = ""
    if defaults_filename != "" and os.path.exists(defaults_filename):
        logging.info(
            "Loading configuration from defaults file: {}".format(defaults_filename)
        )
        with open(defaults_filename) as yaml_fd:
            defaults_dict = yaml.safe_load(yaml_fd)
            if "exporter" in defaults_dict:
                exporter_dict = defaults_dict["exporter"]
                if "comparison" in exporter_dict:
                    comparison_dict = exporter_dict["comparison"]
                    if "metrics" in comparison_dict:
                        metrics = comparison_dict["metrics"]
                        logging.info("Detected defaults metrics info. reading metrics")
                        default_metrics = []

                        for metric in metrics:
                            if metric.startswith("$."):
                                metric = metric[2:]
                            logging.info("Will use metric: {}".format(metric))
                            default_metrics.append(metric)
                        if len(default_metrics) == 1:
                            default_metrics_str = default_metrics[0]
                        if len(default_metrics) > 1:
                            default_metrics_str = "({})".format(
                                ",".join(default_metrics)
                            )
                        logging.info("Default metrics: {}".format(default_metrics_str))

                    if "baseline-branch" in comparison_dict:
                        default_baseline_branch = comparison_dict["baseline-branch"]
                        logging.info(
                            "Detected baseline branch in defaults file. {}".format(
                                default_baseline_branch
                            )
                        )
    return default_baseline_branch, default_metrics_str


def compute_regression_table(
    rts,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env_baseline,
    tf_triggering_env_comparison,
    metric_name,
    comparison_branch,
    baseline_branch="unstable",
    baseline_tag=None,
    comparison_tag=None,
    baseline_deployment_name="oss-standalone",
    comparison_deployment_name="oss-standalone",
    print_improvements_only=False,
    print_regressions_only=False,
    skip_unstable=False,
    regressions_percent_lower_limit=5.0,
    simplify_table=False,
    test="",
    testname_regex=".*",
    verbose=False,
    last_n_baseline=-1,
    last_n_comparison=-1,
    metric_mode="higher-better",
    from_date=None,
    from_ts_ms=None,
    to_date=None,
    to_ts_ms=None,
    use_metric_context_path=None,
    running_platform_baseline=None,
    running_platform_comparison=None,
    baseline_target_version=None,
    comparison_target_version=None,
    comparison_hash=None,
    baseline_hash=None,
    baseline_github_repo="redis",
    comparison_github_repo="redis",
    baseline_target_branch=None,
    comparison_target_branch=None,
    baseline_github_org="redis",
    comparison_github_org="redis",
    regression_str="REGRESSION",
    improvement_str="IMPROVEMENT",
    tests_with_config={},
    use_test_suites_folder=False,
    test_suites_folder=None,
    extra_filters="",
    command_group_regex=".*",
    command_regex=".*",
):
    START_TIME_NOW_UTC, _, _ = get_start_time_vars()
    START_TIME_LAST_MONTH_UTC = START_TIME_NOW_UTC - datetime.timedelta(days=31)
    if from_date is None:
        from_date = START_TIME_LAST_MONTH_UTC
    if to_date is None:
        to_date = START_TIME_NOW_UTC
    if from_ts_ms is None:
        from_ts_ms = int(from_date.timestamp() * 1000)
    if to_ts_ms is None:
        to_ts_ms = int(to_date.timestamp() * 1000)
    from_human_str = humanize.naturaltime(
        dt.datetime.utcfromtimestamp(from_ts_ms / 1000)
    )
    to_human_str = humanize.naturaltime(dt.datetime.utcfromtimestamp(to_ts_ms / 1000))
    logging.info(
        "Using a time-delta from {} to {}".format(from_human_str, to_human_str)
    )
    baseline_str, by_str_baseline, comparison_str, by_str_comparison = get_by_strings(
        baseline_branch,
        comparison_branch,
        baseline_tag,
        comparison_tag,
        baseline_target_version,
        comparison_target_version,
        comparison_hash,
        baseline_hash,
        baseline_target_branch,
        comparison_target_branch,
    )
    logging.info(f"Using baseline filter {by_str_baseline}={baseline_str}")
    logging.info(f"Using comparison filter {by_str_comparison}={comparison_str}")
    (
        prefix,
        testcases_setname,
        _,
        tsname_project_total_failures,
        tsname_project_total_success,
        _,
        _,
        _,
        testcases_metric_context_path_setname,
        _,
        _,
        _,
        _,
        _,
    ) = get_overall_dashboard_keynames(
        tf_github_org, tf_github_repo, tf_triggering_env_baseline
    )
    test_names = []
    used_key = testcases_setname
    test_filter = "test_name"
    if use_metric_context_path:
        test_filter = "test_name:metric_context_path"
        used_key = testcases_metric_context_path_setname
    tags_regex_string = re.compile(testname_regex)
    if test != "":
        test_names = test.split(",")
        logging.info("Using test name {}".format(test_names))
    elif use_test_suites_folder:
        test_names = get_test_names_from_yaml_files(
            test_suites_folder, tags_regex_string
        )
    else:
        test_names = get_test_names_from_db(
            rts, tags_regex_string, test_names, used_key
        )

    # Apply command regex filtering to tests_with_config
    tests_with_config = filter_tests_by_command_regex(tests_with_config, command_regex)

    (
        detected_regressions,
        table_full,
        table_stable,
        table_unstable,
        table_improvements,
        table_regressions,
        total_improvements,
        total_regressions,
        total_stable,
        total_unstable,
        total_comparison_points,
        regressions_list,
        improvements_list,
        unstable_list,
        baseline_only_list,
        comparison_only_list,
        no_datapoints_list,
        group_change,
        command_change,
        boxplot_data,
    ) = from_rts_to_regression_table(
        baseline_deployment_name,
        comparison_deployment_name,
        baseline_str,
        comparison_str,
        by_str_baseline,
        by_str_comparison,
        from_ts_ms,
        to_ts_ms,
        last_n_baseline,
        last_n_comparison,
        metric_mode,
        metric_name,
        print_improvements_only,
        print_regressions_only,
        skip_unstable,
        regressions_percent_lower_limit,
        rts,
        simplify_table,
        test_filter,
        test_names,
        tf_triggering_env_baseline,
        tf_triggering_env_comparison,
        verbose,
        running_platform_baseline,
        running_platform_comparison,
        baseline_github_repo,
        comparison_github_repo,
        baseline_github_org,
        comparison_github_org,
        regression_str,
        improvement_str,
        tests_with_config,
        extra_filters,
    )
    logging.info(
        "Printing differential analysis between {} and {}".format(
            baseline_str, comparison_str
        )
    )

    table_output = "# Comparison between {} and {}.\n\nTime Period from {}. (environment used: {})\n\n".format(
        baseline_str,
        comparison_str,
        from_human_str,
        baseline_deployment_name,
    )

    table_output += "<details>\n  <summary>By GROUP change csv:</summary>\n\n"
    table_output += (
        "\ncommand_group,min_change,q1_change,median_change,q3_change,max_change  \n"
    )
    for group_name, changes_list in group_change.items():
        min_change = min(changes_list)
        q1_change = np.percentile(changes_list, 25)
        median_change = np.median(changes_list)
        q3_change = np.percentile(changes_list, 75)
        max_change = max(changes_list)
        table_output += f"{group_name},{min_change:.3f},{q1_change:.3f},{median_change:.3f},{q3_change:.3f},{max_change:.3f}\n"
    table_output += "\n</details>\n"
    table_output += "\n\n"
    table_output += "<details>\n  <summary>By COMMAND change csv:</summary>\n\n"
    table_output += (
        "\ncommand,min_change,q1_change,median_change,q3_change,max_change  \n"
    )

    # Filter commands by command group regex if specified
    filtered_command_change = command_change
    if command_group_regex != ".*":
        group_regex = re.compile(command_group_regex)
        filtered_command_change = {}
        for command_name, changes_list in command_change.items():
            command_group = categorize_command(command_name.lower())
            if re.search(group_regex, command_group):
                filtered_command_change[command_name] = changes_list

    for command_name, changes_list in filtered_command_change.items():
        min_change = min(changes_list)
        q1_change = np.percentile(changes_list, 25)
        median_change = np.median(changes_list)
        q3_change = np.percentile(changes_list, 75)
        max_change = max(changes_list)
        table_output += f"{command_name},{min_change:.3f},{q1_change:.3f},{median_change:.3f},{q3_change:.3f},{max_change:.3f}\n"
    table_output += "\n</details>\n"

    if total_unstable > 0:
        old_stdout = sys.stdout
        sys.stdout = mystdout = StringIO()
        table_output += "#### Unstable Table\n\n"
        writer_regressions = MarkdownTableWriter(
            table_name="",
            headers=[
                "Test Case",
                f"Baseline {baseline_github_org}/{baseline_github_repo} {baseline_str} (median obs. +- std.dev)",
                f"Comparison {comparison_github_org}/{comparison_github_repo} {comparison_str} (median obs. +- std.dev)",
                "% change ({})".format(metric_mode),
                "Note",
            ],
            value_matrix=table_unstable,
        )
        writer_regressions.dump(mystdout, False)
        table_output += mystdout.getvalue()
        table_output += "\n\n"
        test_names_str = "|".join([l[0] for l in unstable_list])
        table_output += f"Unstable test regexp names: {test_names_str}\n\n"
        mystdout.close()
        sys.stdout = old_stdout

    if total_regressions > 0:
        old_stdout = sys.stdout
        sys.stdout = mystdout = StringIO()
        table_output += "#### Regressions Table\n\n"
        writer_regressions = MarkdownTableWriter(
            table_name="",
            headers=[
                "Test Case",
                f"Baseline {baseline_github_org}/{baseline_github_repo} {baseline_str} (median obs. +- std.dev)",
                f"Comparison {comparison_github_org}/{comparison_github_repo} {comparison_str} (median obs. +- std.dev)",
                "% change ({})".format(metric_mode),
                "Note",
            ],
            value_matrix=table_regressions,
        )
        writer_regressions.dump(mystdout, False)
        table_output += mystdout.getvalue()
        table_output += "\n\n"
        test_names_str = "|".join([l[0] for l in regressions_list])
        table_output += f"Regressions test regexp names: {test_names_str}\n\n"
        mystdout.close()
        sys.stdout = old_stdout

    if total_improvements > 0:
        old_stdout = sys.stdout
        sys.stdout = mystdout = StringIO()
        table_output += "#### Improvements Table\n\n"
        writer_regressions = MarkdownTableWriter(
            table_name="",
            headers=[
                "Test Case",
                f"Baseline {baseline_github_org}/{baseline_github_repo} {baseline_str} (median obs. +- std.dev)",
                f"Comparison {comparison_github_org}/{comparison_github_repo} {comparison_str} (median obs. +- std.dev)",
                "% change ({})".format(metric_mode),
                "Note",
            ],
            value_matrix=table_improvements,
        )
        writer_regressions.dump(mystdout, False)
        table_output += mystdout.getvalue()
        table_output += "\n\n"
        test_names_str = "|".join([l[0] for l in improvements_list])
        table_output += f"Improvements test regexp names: {test_names_str}\n\n"
        mystdout.close()
        sys.stdout = old_stdout

    old_stdout = sys.stdout
    sys.stdout = mystdout = StringIO()
    writer_full = MarkdownTableWriter(
        table_name="",
        headers=[
            "Test Case",
            f"Baseline {baseline_github_org}/{baseline_github_repo} {baseline_str} (median obs. +- std.dev)",
            f"Comparison {comparison_github_org}/{comparison_github_repo} {comparison_str} (median obs. +- std.dev)",
            "% change ({})".format(metric_mode),
            "Note",
        ],
        value_matrix=table_full,
    )
    table_output += "<details>\n  <summary>Full Results table:</summary>\n\n"

    writer_full.dump(mystdout, False)

    sys.stdout = old_stdout
    table_output += mystdout.getvalue()
    table_output += "\n</details>\n"
    len_baseline_only_list = len(baseline_only_list)
    if len_baseline_only_list > 0:
        table_output += f"\n  WARNING: There were {len_baseline_only_list} benchmarks with datapoints only on baseline.\n\n"
        baseline_only_test_names_str = "|".join([l for l in baseline_only_list])
        table_output += (
            f"  Baseline only test regexp names: {baseline_only_test_names_str}\n\n"
        )
    len_comparison_only_list = len(comparison_only_list)
    if len_comparison_only_list > 0:
        table_output += f"\n  WARNING: There were {len_comparison_only_list} benchmarks with datapoints only on comparison.\n\n"
        comparison_only_test_names_str = "|".join([l for l in comparison_only_list])
        table_output += (
            f"  Comparison only test regexp names: {comparison_only_test_names_str}\n\n"
        )
    len_no_datapoints = len(no_datapoints_list)
    if len_no_datapoints > 0:
        table_output += f"\n  WARNING: There were {len_no_datapoints} benchmarks with NO datapoints for both baseline and comparison.\n\n"
        table_output += "<details>\n  <summary>NO datapoints for both baseline and comparison:</summary>\n\n"
        no_datapoints_test_names_str = "|".join([l for l in no_datapoints_list])
        table_output += (
            f"  NO DATAPOINTS test regexp names: {no_datapoints_test_names_str}\n\n"
        )
        table_output += "\n</details>\n"

    return (
        detected_regressions,
        table_output,
        improvements_list,
        regressions_list,
        total_stable,
        total_unstable,
        total_comparison_points,
        boxplot_data,
        command_change,
    )


def get_by_error(name, by_str_arr):
    by_string = ",".join(by_str_arr)
    return f"--{name}-branch, --{name}-tag, --{name}-target-branch, --{name}-hash, and --{name}-target-version are mutually exclusive. You selected a total of {len(by_str_arr)}: {by_string}. Pick one..."


def get_by_strings(
    baseline_branch,
    comparison_branch,
    baseline_tag,
    comparison_tag,
    baseline_target_version=None,
    comparison_target_version=None,
    baseline_hash=None,
    comparison_hash=None,
    baseline_target_branch=None,
    comparison_target_branch=None,
):
    baseline_covered = False
    comparison_covered = False
    by_str_baseline = ""
    by_str_comparison = ""
    baseline_str = ""
    comparison_str = ""
    baseline_by_arr = []
    comparison_by_arr = []

    ################# BASELINE BY ....

    if baseline_branch is not None:
        by_str_baseline = "branch"
        baseline_covered = True
        baseline_str = baseline_branch
        baseline_by_arr.append(by_str_baseline)

    if baseline_tag is not None:
        by_str_baseline = "version"
        if baseline_covered:
            baseline_by_arr.append(by_str_baseline)
            logging.error(get_by_error("baseline", baseline_by_arr))
            exit(1)
        baseline_covered = True
        baseline_str = baseline_tag

    if baseline_target_version is not None:
        by_str_baseline = "target+version"
        if baseline_covered:
            baseline_by_arr.append(by_str_baseline)
            logging.error(get_by_error("baseline", baseline_by_arr))
            exit(1)
        baseline_covered = True
        baseline_str = baseline_target_version

    if baseline_hash is not None:
        by_str_baseline = "hash"
        if baseline_covered:
            baseline_by_arr.append(by_str_baseline)
            logging.error(get_by_error("baseline", baseline_by_arr))
            exit(1)
        baseline_covered = True
        baseline_str = baseline_hash
    if baseline_target_branch is not None:
        by_str_baseline = "target+branch"
        if baseline_covered:
            baseline_by_arr.append(by_str_baseline)
            logging.error(get_by_error("baseline", baseline_by_arr))
            exit(1)
        baseline_covered = True
        baseline_str = baseline_target_branch

    ################# COMPARISON BY ....

    if comparison_branch is not None:
        by_str_comparison = "branch"
        comparison_covered = True
        comparison_str = comparison_branch

    if comparison_tag is not None:
        # check if we had already covered comparison
        if comparison_covered:
            logging.error(
                "--comparison-branch and --comparison-tag, --comparison-hash, --comparison-target-branch, and --comparison-target-table are mutually exclusive. Pick one..."
            )
            exit(1)
        comparison_covered = True
        by_str_comparison = "version"
        comparison_str = comparison_tag
    if comparison_target_version is not None:
        # check if we had already covered comparison
        if comparison_covered:
            logging.error(
                "--comparison-branch, --comparison-tag, --comparison-hash, --comparison-target-branch, and --comparison-target-table are mutually exclusive. Pick one..."
            )
            exit(1)
        comparison_covered = True
        by_str_comparison = "target+version"
        comparison_str = comparison_target_version

    if comparison_target_branch is not None:
        # check if we had already covered comparison
        if comparison_covered:
            logging.error(
                "--comparison-branch, --comparison-tag, --comparison-hash, --comparison-target-branch, and --comparison-target-table are mutually exclusive. Pick one..."
            )
            exit(1)
        comparison_covered = True
        by_str_comparison = "target+branch"
        comparison_str = comparison_target_branch

    if comparison_hash is not None:
        # check if we had already covered comparison
        # if comparison_covered:
        #     logging.error(
        #         "--comparison-branch, --comparison-tag, --comparison-hash, --comparison-target-branch, and --comparison-target-table are mutually exclusive. Pick one..."
        #     )
        #     exit(1)
        comparison_covered = True
        by_str_comparison = "hash"
        comparison_str = comparison_hash

    if baseline_covered is False:
        logging.error(
            "You need to provider either "
            + "( --baseline-branch, --baseline-tag, --baseline-hash, --baseline-target-branch or --baseline-target-version ) "
        )
        exit(1)
    if comparison_covered is False:
        logging.error(
            "You need to provider either "
            + "( --comparison-branch, --comparison-tag, --comparison-hash, --comparison-target-branch or --comparison-target-version ) "
        )
        exit(1)
    return baseline_str, by_str_baseline, comparison_str, by_str_comparison


def process_single_test_comparison(
    test_name,
    tests_with_config,
    original_metric_mode,
    baseline_str,
    comparison_str,
    by_str_baseline,
    by_str_comparison,
    metric_name,
    test_filter,
    baseline_github_repo,
    comparison_github_repo,
    tf_triggering_env_baseline,
    tf_triggering_env_comparison,
    extra_filters,
    baseline_deployment_name,
    comparison_deployment_name,
    baseline_github_org,
    comparison_github_org,
    running_platform_baseline,
    running_platform_comparison,
    rts,
    from_ts_ms,
    to_ts_ms,
    last_n_baseline,
    last_n_comparison,
    verbose,
    regressions_percent_lower_limit,
    simplify_table,
    regression_str,
    improvement_str,
    progress,
):
    """
    Process comparison analysis for a single test.

    Returns a dictionary containing all the results and side effects that need to be
    accumulated by the caller.
    """
    tested_groups = []
    tested_commands = []
    if test_name in tests_with_config:
        test_spec = tests_with_config[test_name]
        if "tested-groups" in test_spec:
            tested_groups = test_spec["tested-groups"]
        if "tested-commands" in test_spec:
            tested_commands = test_spec["tested-commands"]
    else:
        logging.error(f"Test does not contain spec info: {test_name}")

    metric_mode = original_metric_mode
    compare_version = "main"
    # GE
    github_link = "https://github.com/redis/redis-benchmarks-specification/blob"
    test_path = f"redis_benchmarks_specification/test-suites/{test_name}.yml"
    test_link = f"[{test_name}]({github_link}/{compare_version}/{test_path})"
    multi_value_baseline = check_multi_value_filter(baseline_str)
    multi_value_comparison = check_multi_value_filter(comparison_str)

    filters_baseline = [
        "metric={}".format(metric_name),
        "{}={}".format(test_filter, test_name),
        "github_repo={}".format(baseline_github_repo),
        "triggering_env={}".format(tf_triggering_env_baseline),
    ]
    if extra_filters != "":
        filters_baseline.append(extra_filters)
    if baseline_str != "":
        filters_baseline.append("{}={}".format(by_str_baseline, baseline_str))
    if baseline_deployment_name != "":
        filters_baseline.append("deployment_name={}".format(baseline_deployment_name))
    if baseline_github_org != "":
        filters_baseline.append(f"github_org={baseline_github_org}")
    if running_platform_baseline is not None and running_platform_baseline != "":
        filters_baseline.append("running_platform={}".format(running_platform_baseline))
    filters_comparison = [
        "metric={}".format(metric_name),
        "{}={}".format(test_filter, test_name),
        "github_repo={}".format(comparison_github_repo),
        "triggering_env={}".format(tf_triggering_env_comparison),
    ]
    if comparison_str != "":
        filters_comparison.append("{}={}".format(by_str_comparison, comparison_str))
    if comparison_deployment_name != "":
        filters_comparison.append(
            "deployment_name={}".format(comparison_deployment_name)
        )
    if extra_filters != "":
        filters_comparison.append(extra_filters)
    if comparison_github_org != "":
        filters_comparison.append(f"github_org={comparison_github_org}")
    if "hash" not in by_str_baseline:
        filters_baseline.append("hash==")
    if "hash" not in by_str_comparison:
        filters_comparison.append("hash==")
    if running_platform_comparison is not None and running_platform_comparison != "":
        filters_comparison.append(
            "running_platform={}".format(running_platform_comparison)
        )
    baseline_timeseries = rts.ts().queryindex(filters_baseline)
    comparison_timeseries = rts.ts().queryindex(filters_comparison)

    # avoiding target time-series
    comparison_timeseries = [x for x in comparison_timeseries if "target" not in x]
    baseline_timeseries = [x for x in baseline_timeseries if "target" not in x]
    progress.update()
    if verbose:
        logging.info(
            "Baseline timeseries for {}: {}. test={}".format(
                baseline_str, len(baseline_timeseries), test_name
            )
        )
        logging.info(
            "Comparison timeseries for {}: {}. test={}".format(
                comparison_str, len(comparison_timeseries), test_name
            )
        )
    if len(baseline_timeseries) > 1 and multi_value_baseline is False:
        baseline_timeseries = get_only_Totals(baseline_timeseries)

    # Initialize result dictionary
    result = {
        "skip_test": False,
        "no_datapoints_baseline": False,
        "no_datapoints_comparison": False,
        "no_datapoints_both": False,
        "baseline_only": False,
        "comparison_only": False,
        "detected_regression": False,
        "detected_improvement": False,
        "unstable": False,
        "should_add_line": False,
        "line": None,
        "percentage_change": 0.0,
        "tested_groups": tested_groups,
        "tested_commands": tested_commands,
        "boxplot_data": None,
    }

    if len(baseline_timeseries) == 0:
        logging.warning(
            f"No datapoints for test={test_name} for baseline timeseries {baseline_timeseries}"
        )
        result["no_datapoints_baseline"] = True
        result["no_datapoints_both"] = True

    if len(comparison_timeseries) == 0:
        logging.warning(
            f"No datapoints for test={test_name} for comparison timeseries {comparison_timeseries}"
        )
        result["no_datapoints_comparison"] = True
        result["no_datapoints_both"] = True

    if len(baseline_timeseries) != 1 and multi_value_baseline is False:
        if verbose:
            logging.warning(
                "Skipping this test given the value of timeseries !=1. Baseline timeseries {}".format(
                    len(baseline_timeseries)
                )
            )
            if len(baseline_timeseries) > 1:
                logging.warning(
                    "\t\tTime-series: {}".format(", ".join(baseline_timeseries))
                )
        result["skip_test"] = True
        return result

    if len(comparison_timeseries) > 1 and multi_value_comparison is False:
        comparison_timeseries = get_only_Totals(comparison_timeseries)
    if len(comparison_timeseries) != 1 and multi_value_comparison is False:
        if verbose:
            logging.warning(
                "Comparison timeseries {}".format(len(comparison_timeseries))
            )
        result["skip_test"] = True
        return result

    baseline_v = "N/A"
    comparison_v = "N/A"
    baseline_values = []
    baseline_datapoints = []
    comparison_values = []
    comparison_datapoints = []
    percentage_change = 0.0
    baseline_v_str = "N/A"
    comparison_v_str = "N/A"
    largest_variance = 0
    baseline_pct_change = "N/A"
    comparison_pct_change = "N/A"

    note = ""
    try:
        for ts_name_baseline in baseline_timeseries:
            datapoints_inner = rts.ts().revrange(ts_name_baseline, from_ts_ms, to_ts_ms)
            baseline_datapoints.extend(datapoints_inner)
        (
            baseline_pct_change,
            baseline_v,
            largest_variance,
        ) = get_v_pct_change_and_largest_var(
            baseline_datapoints,
            baseline_pct_change,
            baseline_v,
            baseline_values,
            largest_variance,
            last_n_baseline,
            verbose,
        )
        for ts_name_comparison in comparison_timeseries:
            datapoints_inner = rts.ts().revrange(
                ts_name_comparison, from_ts_ms, to_ts_ms
            )
            comparison_datapoints.extend(datapoints_inner)

        (
            comparison_pct_change,
            comparison_v,
            largest_variance,
        ) = get_v_pct_change_and_largest_var(
            comparison_datapoints,
            comparison_pct_change,
            comparison_v,
            comparison_values,
            largest_variance,
            last_n_comparison,
            verbose,
        )

        waterline = regressions_percent_lower_limit
        # if regressions_percent_lower_limit < largest_variance:
        #     note = "waterline={:.1f}%.".format(largest_variance)
        #     waterline = largest_variance

    except redis.exceptions.ResponseError as e:
        logging.error(
            "Detected a redis.exceptions.ResponseError. {}".format(e.__str__())
        )
        pass
    except ZeroDivisionError as e:
        logging.error("Detected a ZeroDivisionError. {}".format(e.__str__()))
        pass

    unstable = False

    if baseline_v != "N/A" and comparison_v == "N/A":
        logging.warning(
            f"Baseline contains datapoints but comparison not for test: {test_name}"
        )
        result["baseline_only"] = True
    if comparison_v != "N/A" and baseline_v == "N/A":
        logging.warning(
            f"Comparison contains datapoints but baseline not for test: {test_name}"
        )
        result["comparison_only"] = True
    if (
        baseline_v != "N/A"
        and comparison_pct_change != "N/A"
        and comparison_v != "N/A"
        and baseline_pct_change != "N/A"
    ):
        if comparison_pct_change > 10.0 or baseline_pct_change > 10.0:
            note = "UNSTABLE (very high variance)"
            unstable = True
            result["unstable"] = True

        baseline_v_str = prepare_value_str(
            baseline_pct_change,
            baseline_v,
            baseline_values,
            simplify_table,
            metric_name,
        )
        comparison_v_str = prepare_value_str(
            comparison_pct_change,
            comparison_v,
            comparison_values,
            simplify_table,
            metric_name,
        )

        if metric_mode == "higher-better":
            percentage_change = (float(comparison_v) / float(baseline_v) - 1) * 100.0
        else:
            # lower-better
            percentage_change = (
                -(float(baseline_v) - float(comparison_v)) / float(baseline_v)
            ) * 100.0

        # Collect data for box plot
        result["boxplot_data"] = (test_name, percentage_change)
    else:
        logging.warn(
            f"Missing data for test {test_name}. baseline_v={baseline_v} (pct_change={baseline_pct_change}), comparison_v={comparison_v} (pct_change={comparison_pct_change}) "
        )

    result["percentage_change"] = percentage_change

    if baseline_v != "N/A" or comparison_v != "N/A":
        detected_regression = False
        detected_improvement = False
        noise_waterline = 3

        # For higher-better metrics: negative change = regression, positive change = improvement
        # For lower-better metrics: positive change = regression, negative change = improvement
        if metric_mode == "higher-better":
            # Higher is better: negative change is bad (regression), positive change is good (improvement)
            if percentage_change < 0.0:
                if -waterline >= percentage_change:
                    detected_regression = True
                    note = note + f" {regression_str}"
                elif percentage_change < -noise_waterline:
                    if simplify_table is False:
                        note = note + f" potential {regression_str}"
                else:
                    if simplify_table is False:
                        note = note + " No Change"

            if percentage_change > 0.0:
                if percentage_change > waterline:
                    detected_improvement = True
                    note = note + f" {improvement_str}"
                elif percentage_change > noise_waterline:
                    if simplify_table is False:
                        note = note + f" potential {improvement_str}"
                else:
                    if simplify_table is False:
                        note = note + " No Change"
        else:
            # Lower is better: positive change is bad (regression), negative change is good (improvement)
            if percentage_change > 0.0:
                if percentage_change >= waterline:
                    detected_regression = True
                    note = note + f" {regression_str}"
                elif percentage_change > noise_waterline:
                    if simplify_table is False:
                        note = note + f" potential {regression_str}"
                else:
                    if simplify_table is False:
                        note = note + " No Change"

            if percentage_change < 0.0:
                if -percentage_change > waterline:
                    detected_improvement = True
                    note = note + f" {improvement_str}"
                elif -percentage_change > noise_waterline:
                    if simplify_table is False:
                        note = note + f" potential {improvement_str}"
                else:
                    if simplify_table is False:
                        note = note + " No Change"

        result["detected_regression"] = detected_regression
        result["detected_improvement"] = detected_improvement

        line = get_line(
            baseline_v_str,
            comparison_v_str,
            note,
            percentage_change,
            test_link,
        )
        result["line"] = line
    else:
        logging.warning(
            "There were no datapoints both for baseline and comparison for test: {test_name}"
        )
        result["no_datapoints_both"] = True

    return result


def from_rts_to_regression_table(
    baseline_deployment_name,
    comparison_deployment_name,
    baseline_str,
    comparison_str,
    by_str_baseline,
    by_str_comparison,
    from_ts_ms,
    to_ts_ms,
    last_n_baseline,
    last_n_comparison,
    metric_mode,
    metric_name,
    print_improvements_only,
    print_regressions_only,
    skip_unstable,
    regressions_percent_lower_limit,
    rts,
    simplify_table,
    test_filter,
    test_names,
    tf_triggering_env_baseline,
    tf_triggering_env_comparison,
    verbose,
    running_platform_baseline=None,
    running_platform_comparison=None,
    baseline_github_repo="redis",
    comparison_github_repo="redis",
    baseline_github_org="redis",
    comparison_github_org="redis",
    regression_str="REGRESSION",
    improvement_str="IMPROVEMENT",
    tests_with_config={},
    extra_filters="",
):
    print_all = print_regressions_only is False and print_improvements_only is False
    table_full = []
    table_unstable = []
    table_stable = []
    table_regressions = []
    table_improvements = []
    detected_regressions = []
    total_improvements = 0
    total_stable = 0
    total_unstable = 0
    total_regressions = 0
    total_comparison_points = 0
    noise_waterline = 3
    progress = tqdm(unit="benchmark time-series", total=len(test_names))
    regressions_list = []
    improvements_list = []
    unstable_list = []
    baseline_only_list = []
    comparison_only_list = []
    no_datapoints_list = []
    no_datapoints_baseline_list = []
    no_datapoints_comparison_list = []
    group_change = {}
    command_change = {}
    original_metric_mode = metric_mode

    # Data collection for box plot
    boxplot_data = []

    # First loop: Collect all test results using parallel processing
    test_results = []

    def process_test_wrapper(test_name):
        """Wrapper function to process a single test and return test_name with result"""
        result = process_single_test_comparison(
            test_name,
            tests_with_config,
            original_metric_mode,
            baseline_str,
            comparison_str,
            by_str_baseline,
            by_str_comparison,
            metric_name,
            test_filter,
            baseline_github_repo,
            comparison_github_repo,
            tf_triggering_env_baseline,
            tf_triggering_env_comparison,
            extra_filters,
            baseline_deployment_name,
            comparison_deployment_name,
            baseline_github_org,
            comparison_github_org,
            running_platform_baseline,
            running_platform_comparison,
            rts,
            from_ts_ms,
            to_ts_ms,
            last_n_baseline,
            last_n_comparison,
            verbose,
            regressions_percent_lower_limit,
            simplify_table,
            regression_str,
            improvement_str,
            progress,
        )
        return (test_name, result)

    # Use ThreadPoolExecutor to process tests in parallel
    with ThreadPoolExecutor() as executor:
        test_results = list(executor.map(process_test_wrapper, test_names))

    # Second loop: Process all collected results
    for test_name, result in test_results:
        # Handle the results from the extracted function
        if result["skip_test"]:
            continue

        if result["no_datapoints_baseline"]:
            no_datapoints_baseline_list.append(test_name)
            if test_name not in no_datapoints_list:
                no_datapoints_list.append(test_name)

        if result["no_datapoints_comparison"]:
            no_datapoints_comparison_list.append(test_name)
            if test_name not in no_datapoints_list:
                no_datapoints_list.append(test_name)

        if result["baseline_only"]:
            baseline_only_list.append(test_name)

        if result["comparison_only"]:
            comparison_only_list.append(test_name)

        if result["unstable"]:
            unstable_list.append([test_name, "n/a"])

        if result["boxplot_data"]:
            boxplot_data.append(result["boxplot_data"])

        # Handle group and command changes
        for test_group in result["tested_groups"]:
            if test_group not in group_change:
                group_change[test_group] = []
            group_change[test_group].append(result["percentage_change"])

        for test_command in result["tested_commands"]:
            if test_command not in command_change:
                command_change[test_command] = []
            command_change[test_command].append(result["percentage_change"])

        # Handle regression/improvement detection and table updates
        if result["line"] is not None:
            detected_regression = result["detected_regression"]
            detected_improvement = result["detected_improvement"]
            unstable = result["unstable"]
            line = result["line"]
            percentage_change = result["percentage_change"]

            if detected_regression:
                total_regressions = total_regressions + 1
                detected_regressions.append(test_name)
                regressions_list.append([test_name, percentage_change])
                table_regressions.append(line)

            if detected_improvement:
                total_improvements = total_improvements + 1
                improvements_list.append([test_name, percentage_change])
                table_improvements.append(line)

            if unstable:
                total_unstable += 1
                table_unstable.append(line)
            else:
                if not detected_regression and not detected_improvement:
                    total_stable = total_stable + 1
                    table_stable.append(line)

            should_add_line = False
            if print_regressions_only and detected_regression:
                should_add_line = True
            if print_improvements_only and detected_improvement:
                should_add_line = True
            if print_all:
                should_add_line = True
            if unstable and skip_unstable:
                should_add_line = False

            if should_add_line:
                total_comparison_points = total_comparison_points + 1
                table_full.append(line)
        elif result["no_datapoints_both"]:
            if test_name not in no_datapoints_list:
                no_datapoints_list.append(test_name)
    logging.warning(
        f"There is a total of {len(no_datapoints_list)} tests without datapoints for baseline AND comparison"
    )
    logging.info(
        f"There is a total of {len(comparison_only_list)} tests without datapoints for baseline"
    )
    print(
        "No datapoint baseline regex={test_names_str}".format(
            test_names_str="|".join(no_datapoints_baseline_list)
        )
    )
    logging.info(
        f"There is a total of {len(baseline_only_list)} tests without datapoints for comparison"
    )
    print(
        "No datapoint comparison regex={test_names_str}".format(
            test_names_str="|".join(no_datapoints_comparison_list)
        )
    )
    logging.info(f"There is a total of {len(unstable_list)} UNSTABLE tests")
    return (
        detected_regressions,
        table_full,
        table_stable,
        table_unstable,
        table_improvements,
        table_regressions,
        total_improvements,
        total_regressions,
        total_stable,
        total_unstable,
        total_comparison_points,
        regressions_list,
        improvements_list,
        unstable_list,
        baseline_only_list,
        comparison_only_list,
        no_datapoints_list,
        group_change,
        command_change,
        boxplot_data,
    )


def get_only_Totals(baseline_timeseries):
    logging.warning("\t\tTime-series: {}".format(", ".join(baseline_timeseries)))
    logging.info(
        f"Checking if Totals will reduce timeseries. initial len={len(baseline_timeseries)}"
    )
    new_base = []
    for ts_name in baseline_timeseries:
        if "io-threads" in ts_name:
            continue
        if "oss-cluster" in ts_name:
            continue
        if "Totals" in ts_name:
            new_base.append(ts_name)
    baseline_timeseries = new_base
    logging.info(
        f"                                          final len={len(baseline_timeseries)}"
    )

    return baseline_timeseries


def check_multi_value_filter(baseline_str):
    multi_value_baseline = False
    if "(" in baseline_str and "," in baseline_str and ")" in baseline_str:
        multi_value_baseline = True
    return multi_value_baseline


def is_latency_metric(metric_name):
    """Check if a metric represents latency and should use 3-digit precision"""
    latency_indicators = [
        "latency",
        "percentile",
        "usec",
        "msec",
        "overallQuantiles",
        "latencystats",
        "p50",
        "p95",
        "p99",
        "p999",
    ]
    metric_name_lower = metric_name.lower()
    return any(indicator in metric_name_lower for indicator in latency_indicators)


def prepare_value_str(
    baseline_pct_change, baseline_v, baseline_values, simplify_table, metric_name=""
):
    """Prepare value string with appropriate precision based on metric type"""
    # Use 3-digit precision for latency metrics
    if is_latency_metric(metric_name):
        if baseline_v < 1.0:
            baseline_v_str = " {:.3f}".format(baseline_v)
        elif baseline_v < 10.0:
            baseline_v_str = " {:.3f}".format(baseline_v)
        elif baseline_v < 100.0:
            baseline_v_str = " {:.3f}".format(baseline_v)
        else:
            baseline_v_str = " {:.3f}".format(baseline_v)
    else:
        # Original formatting for non-latency metrics
        if baseline_v < 1.0:
            baseline_v_str = " {:.2f}".format(baseline_v)
        elif baseline_v < 10.0:
            baseline_v_str = " {:.1f}".format(baseline_v)
        else:
            baseline_v_str = " {:.0f}".format(baseline_v)

    stamp_b = ""
    if baseline_pct_change > 10.0:
        stamp_b = "UNSTABLE "
    if len(baseline_values) > 1:
        baseline_v_str += " +- {:.1f}% {}".format(
            baseline_pct_change,
            stamp_b,
        )
    if simplify_table is False and len(baseline_values) > 1:
        baseline_v_str += "({} datapoints)".format(len(baseline_values))
    return baseline_v_str


def filter_test_names_by_regex(test_names, tags_regex_string):
    """
    Filter test names based on regex pattern.

    Args:
        test_names: List of test names to filter
        tags_regex_string: Regex pattern to match against test names

    Returns:
        List of filtered test names that match the regex pattern
    """
    final_test_names = []
    for test_name in test_names:
        if not isinstance(test_name, str):
            test_name = test_name.decode()
        match_obj = re.search(tags_regex_string, test_name)
        if match_obj is not None:
            final_test_names.append(test_name)
    return final_test_names


def get_test_names_from_db(rts, tags_regex_string, test_names, used_key):
    try:
        test_names = rts.smembers(used_key)
        test_names = list(test_names)
        test_names.sort()
        test_names = filter_test_names_by_regex(test_names, tags_regex_string)

    except redis.exceptions.ResponseError as e:
        logging.warning(
            "Error while trying to fetch test cases set (key={}) {}. ".format(
                used_key, e.__str__()
            )
        )
        pass
    logging.warning(
        "Based on test-cases set (key={}) we have {} comparison points. ".format(
            used_key, len(test_names)
        )
    )
    return test_names


def filter_tests_by_command_regex(tests_with_config, command_regex=".*"):
    """Filter tests based on command regex matching tested-commands"""
    if command_regex == ".*":
        return tests_with_config

    logging.info(f"Filtering tests by command regex: {command_regex}")
    command_regex_compiled = re.compile(command_regex, re.IGNORECASE)
    filtered_tests = {}

    for test_name, test_config in tests_with_config.items():
        tested_commands = test_config.get("tested-commands", [])

        # Check if any tested command matches the regex
        command_match = False
        for command in tested_commands:
            if re.search(command_regex_compiled, command):
                command_match = True
                logging.info(f"Including test {test_name} (matches command: {command})")
                break

        if command_match:
            filtered_tests[test_name] = test_config
        else:
            logging.info(f"Excluding test {test_name} (commands: {tested_commands})")

    logging.info(
        f"Command regex filtering: {len(filtered_tests)} tests remaining out of {len(tests_with_config)}"
    )
    return filtered_tests


def get_test_names_from_yaml_files(test_suites_folder, tags_regex_string):
    """Get test names from YAML files in test-suites folder"""
    from redis_benchmarks_specification.__common__.runner import get_benchmark_specs

    # Get all YAML files
    yaml_files = get_benchmark_specs(test_suites_folder, test="", test_regex=".*")

    # Extract test names (remove path and .yml extension)
    test_names = []
    for yaml_file in yaml_files:
        test_name = os.path.basename(yaml_file).replace(".yml", "")
        # Apply regex filtering like database version
        match_obj = re.search(tags_regex_string, test_name)
        if match_obj is not None:
            test_names.append(test_name)

    test_names.sort()
    logging.info(
        "Based on test-suites folder ({}) we have {} comparison points: {}".format(
            test_suites_folder, len(test_names), test_names
        )
    )
    return test_names


def extract_command_from_test_name(test_name):
    """Extract Redis command from test name"""
    # Common patterns in test names
    test_name_lower = test_name.lower()

    # Handle specific patterns
    if "memtier_benchmark" in test_name_lower:
        # Look for command patterns in memtier test names
        for cmd in [
            "get",
            "set",
            "hget",
            "hset",
            "hgetall",
            "hmset",
            "hmget",
            "hdel",
            "hexists",
            "hkeys",
            "hvals",
            "hincrby",
            "hincrbyfloat",
            "hsetnx",
            "hscan",
            "multi",
            "exec",
        ]:
            if cmd in test_name_lower:
                return cmd.upper()

    # Try to extract command from test name directly
    parts = test_name.split("-")
    for part in parts:
        part_upper = part.upper()
        # Check if it looks like a Redis command
        if len(part_upper) >= 3 and part_upper.isalpha():
            return part_upper

    return "UNKNOWN"


def generate_command_performance_boxplot_from_command_data(
    command_change,
    output_filename,
    regression_str="Regression",
    improvement_str="Improvement",
    command_group_regex=".*",
):
    """Generate vertical box plot showing performance change distribution per command using command_change data"""
    if not MATPLOTLIB_AVAILABLE:
        logging.error("matplotlib not available, cannot generate box plot")
        return

    try:
        if not command_change:
            logging.warning("No command data found for box plot generation")
            return

        # Filter commands by command group regex
        if command_group_regex != ".*":
            logging.info(
                f"Filtering commands by command group regex: {command_group_regex}"
            )
            group_regex = re.compile(command_group_regex)
            filtered_command_change = {}

            for cmd, changes in command_change.items():
                command_group = categorize_command(cmd.lower())
                if re.search(group_regex, command_group):
                    filtered_command_change[cmd] = changes
                    logging.info(f"Including command {cmd} (group: {command_group})")
                else:
                    logging.info(f"Excluding command {cmd} (group: {command_group})")

            command_change = filtered_command_change

            if not command_change:
                logging.warning(
                    f"No commands found matching command group regex: {command_group_regex}"
                )
                return

            logging.info(f"After filtering: {len(command_change)} commands remaining")

        # Sort commands by median performance change for better visualization
        commands_with_median = [
            (cmd, np.median(changes)) for cmd, changes in command_change.items()
        ]
        commands_with_median.sort(key=lambda x: x[1])
        commands = [cmd for cmd, _ in commands_with_median]

        # Prepare data for plotting (vertical orientation)
        data_for_plot = [command_change[cmd] for cmd in commands]

        # Create labels with test count
        labels_with_count = [
            f"{cmd}\n({len(command_change[cmd])} tests)" for cmd in commands
        ]

        # Create the plot (vertical orientation)
        plt.figure(figsize=(10, 16))

        # Create horizontal box plot (which makes it vertical when we rotate)
        positions = range(1, len(commands) + 1)
        box_plot = plt.boxplot(
            data_for_plot,
            positions=positions,
            patch_artist=True,
            showfliers=True,
            flierprops={"marker": "o", "markersize": 4},
            vert=False,
        )  # vert=False makes it horizontal (commands on Y-axis)

        # Color the boxes and add value annotations
        for i, (patch, cmd) in enumerate(zip(box_plot["boxes"], commands)):
            changes = command_change[cmd]
            median_change = np.median(changes)
            min_change = min(changes)
            max_change = max(changes)

            # Color based on median performance
            if median_change > 0:
                patch.set_facecolor("lightcoral")  # Red for improvements
                patch.set_alpha(0.7)
            else:
                patch.set_facecolor("lightblue")  # Blue for degradations
                patch.set_alpha(0.7)

            # Store values for later annotation (after xlim is set)
            y_pos = i + 1  # Position corresponds to the box position

            # Store annotation data for after xlim is set
            if not hasattr(plt, "_annotation_data"):
                plt._annotation_data = []
            plt._annotation_data.append(
                {
                    "y_pos": y_pos,
                    "min_change": min_change,
                    "median_change": median_change,
                    "max_change": max_change,
                }
            )

        # Calculate optimal x-axis limits for maximum visibility
        all_values = []
        for changes in command_change.values():
            all_values.extend(changes)

        if all_values:
            data_min = min(all_values)
            data_max = max(all_values)

            logging.info(f"Box plot data range: {data_min:.3f}% to {data_max:.3f}%")

            # Add minimal padding - tight to the data
            data_range = data_max - data_min
            if data_range == 0:
                # If all values are the same, add minimal symmetric padding
                padding = max(abs(data_min) * 0.05, 0.5)  # At least 5% or 0.5
                x_min = data_min - padding
                x_max = data_max + padding
            else:
                # Add minimal padding: 2% on each side
                padding = data_range * 0.02
                x_min = data_min - padding
                x_max = data_max + padding

            # Only include 0 if it's actually within or very close to the data range
            if data_min <= 0 <= data_max:
                # 0 is within the data range, keep current limits
                pass
            elif data_min > 0 and data_min < data_range * 0.1:
                # All positive values, but 0 is very close - include it
                x_min = 0
            elif data_max < 0 and abs(data_max) < data_range * 0.1:
                # All negative values, but 0 is very close - include it
                x_max = 0

            plt.xlim(x_min, x_max)
            logging.info(f"Box plot x-axis limits set to: {x_min:.3f}% to {x_max:.3f}%")

        # Add vertical line at 0% (only if 0 is visible)
        current_xlim = plt.xlim()
        if current_xlim[0] <= 0 <= current_xlim[1]:
            plt.axvline(x=0, color="black", linestyle="-", linewidth=1, alpha=0.8)

        # Add background shading with current limits
        x_min, x_max = plt.xlim()
        if x_max > 0:
            plt.axvspan(max(0, x_min), x_max, alpha=0.1, color="red")
        if x_min < 0:
            plt.axvspan(x_min, min(0, x_max), alpha=0.1, color="blue")

        # Add value annotations within the plot area
        if hasattr(plt, "_annotation_data"):
            x_range = x_max - x_min
            for data in plt._annotation_data:
                y_pos = data["y_pos"]
                min_change = data["min_change"]
                median_change = data["median_change"]
                max_change = data["max_change"]

                # Position annotations inside the plot area
                # Use the actual values' positions with small offsets
                offset = x_range * 0.01  # Small offset for readability

                # Position each annotation near its corresponding value
                plt.text(
                    max_change + offset,
                    y_pos + 0.15,
                    f"{max_change:.1f}%",
                    fontsize=7,
                    va="center",
                    ha="left",
                    color="darkred",
                    weight="bold",
                    bbox=dict(
                        boxstyle="round,pad=0.2",
                        facecolor="white",
                        alpha=0.8,
                        edgecolor="none",
                    ),
                )
                plt.text(
                    median_change + offset,
                    y_pos,
                    f"{median_change:.1f}%",
                    fontsize=7,
                    va="center",
                    ha="left",
                    color="black",
                    weight="bold",
                    bbox=dict(
                        boxstyle="round,pad=0.2",
                        facecolor="yellow",
                        alpha=0.8,
                        edgecolor="none",
                    ),
                )
                plt.text(
                    min_change + offset,
                    y_pos - 0.15,
                    f"{min_change:.1f}%",
                    fontsize=7,
                    va="center",
                    ha="left",
                    color="darkblue",
                    weight="bold",
                    bbox=dict(
                        boxstyle="round,pad=0.2",
                        facecolor="white",
                        alpha=0.8,
                        edgecolor="none",
                    ),
                )

            # Clean up the temporary data
            delattr(plt, "_annotation_data")

        # Set Y-axis labels (commands)
        plt.yticks(positions, labels_with_count, fontsize=10)

        # Customize the plot
        title = f"Performance Change Distribution by Redis Command\nRedis is better ← | → Valkey is better"
        plt.title(title, fontsize=14, fontweight="bold", pad=20)
        plt.xlabel("Performance Change (%)", fontsize=12)
        plt.ylabel("Redis Commands", fontsize=12)
        plt.grid(True, alpha=0.3, axis="x")

        # Add legend for box colors (at the bottom)
        from matplotlib.patches import Patch

        legend_elements = [
            Patch(
                facecolor="lightcoral", alpha=0.7, label="Positive % = Valkey is better"
            ),
            Patch(
                facecolor="lightblue", alpha=0.7, label="Negative % = Redis is better"
            ),
        ]
        plt.legend(
            handles=legend_elements,
            bbox_to_anchor=(0.5, -0.05),
            loc="upper center",
            fontsize=10,
            ncol=2,
        )

        # Add statistics text
        total_commands = len(command_change)
        total_measurements = sum(len(changes) for changes in command_change.values())
        plt.figtext(
            0.02,
            0.02,
            f"Commands: {total_commands} | Total measurements: {total_measurements}",
            fontsize=10,
            style="italic",
        )

        # Adjust layout and save
        plt.tight_layout()
        plt.savefig(output_filename, dpi=300, bbox_inches="tight")
        plt.close()

        logging.info(f"Box plot saved to {output_filename}")

        # Print summary statistics
        logging.info("Command performance summary:")
        for cmd in commands:
            changes = command_change[cmd]
            min_change = min(changes)
            max_change = max(changes)
            median_change = np.median(changes)
            q1_change = np.percentile(changes, 25)
            q3_change = np.percentile(changes, 75)
            logging.info(
                f"  {cmd}: min={min_change:.3f}%, max={max_change:.3f}%, median={median_change:.3f}% ({len(changes)} measurements)"
            )

        # Print quartile summary for boxplot readiness
        logging.info("Command performance quartile summary (boxplot ready):")
        for cmd in commands:
            changes = command_change[cmd]
            min_change = min(changes)
            q1_change = np.percentile(changes, 25)
            median_change = np.median(changes)
            q3_change = np.percentile(changes, 75)
            max_change = max(changes)
            logging.info(
                f"  {cmd}: min={min_change:.3f}%, Q1={q1_change:.3f}%, median={median_change:.3f}%, Q3={q3_change:.3f}%, max={max_change:.3f}%"
            )

    except Exception as e:
        logging.error(f"Error generating box plot: {e}")
        import traceback

        traceback.print_exc()


def generate_command_performance_boxplot(comparison_data, output_filename):
    """Generate box plot showing performance change distribution per command"""
    if not MATPLOTLIB_AVAILABLE:
        logging.error("matplotlib not available, cannot generate box plot")
        return

    try:
        # Group data by command
        command_data = {}

        for test_name, pct_change in comparison_data:
            command = extract_command_from_test_name(test_name)
            if command not in command_data:
                command_data[command] = []
            command_data[command].append(pct_change)

        if not command_data:
            logging.warning("No command data found for box plot generation")
            return

        # Filter out commands with insufficient data
        filtered_command_data = {
            cmd: changes
            for cmd, changes in command_data.items()
            if len(changes) >= 1 and cmd != "UNKNOWN"
        }

        if not filtered_command_data:
            logging.warning("No valid command data found for box plot generation")
            return

        # Use the new function with the filtered data
        generate_command_performance_boxplot_from_command_data(
            filtered_command_data, output_filename, command_group_regex=".*"
        )

    except Exception as e:
        logging.error(f"Error generating box plot: {e}")
        import traceback

        traceback.print_exc()


def get_line(
    baseline_v_str,
    comparison_v_str,
    note,
    percentage_change,
    test_name,
):
    percentage_change_str = "{:.1f}% ".format(percentage_change)
    return [
        test_name,
        baseline_v_str,
        comparison_v_str,
        percentage_change_str,
        note.strip(),
    ]


def add_line(
    baseline_v_str,
    comparison_v_str,
    note,
    percentage_change,
    table,
    test_name,
):
    table.append(
        get_line(
            baseline_v_str,
            comparison_v_str,
            note,
            percentage_change,
            test_name,
        )
    )


def get_v_pct_change_and_largest_var(
    comparison_datapoints,
    comparison_pct_change,
    comparison_v,
    comparison_values,
    largest_variance,
    last_n=-1,
    verbose=False,
):
    comparison_nsamples = len(comparison_datapoints)
    if comparison_nsamples > 0:
        _, comparison_v = comparison_datapoints[0]
        for tuple in comparison_datapoints:
            if last_n < 0 or (last_n > 0 and len(comparison_values) < last_n):
                if tuple[1] > 0.0:
                    comparison_values.append(tuple[1])
        if len(comparison_values) > 0:
            comparison_df = pd.DataFrame(comparison_values)
            comparison_median = float(comparison_df.median().iloc[0])
            comparison_v = comparison_median
            comparison_std = float(comparison_df.std().iloc[0])
            if verbose:
                logging.info(
                    "comparison_datapoints: {} value: {}; std-dev: {}; median: {}".format(
                        comparison_datapoints,
                        comparison_v,
                        comparison_std,
                        comparison_median,
                    )
                )
            comparison_pct_change = (comparison_std / comparison_median) * 100.0
            if comparison_pct_change > largest_variance:
                largest_variance = comparison_pct_change
    return comparison_pct_change, comparison_v, largest_variance


def main():
    _, _, project_version = populate_with_poetry_data()
    project_name = "redis-benchmarks-spec-cli"
    parser = argparse.ArgumentParser(
        description=get_version_string(project_name, project_version),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_compare_arguments(parser)
    args = parser.parse_args()
    compare_command_logic(args, project_name, project_version)
