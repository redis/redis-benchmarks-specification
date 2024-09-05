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

from io import StringIO
import sys

from redis_benchmarks_specification.__common__.github import (
    update_comment_if_needed,
    create_new_pr_comment,
    check_github_available_and_actionable,
    check_regression_comment,
)
from redis_benchmarks_specification.__compare__.args import create_compare_arguments


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
        "ci.benchmarks.redislabs/"
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
    return "ci.benchmarks.redislabs/{}/{}/{}:compare:pull_requests:zset".format(
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

    if running_platform is not None:
        logging.info(
            "Using platform named: {} to do the comparison.\n\n".format(
                running_platform
            )
        )

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
    ) = compute_regression_table(
        rts,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
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
        running_platform,
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
        running_platform,
        table_output,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        total_comparison_points,
        total_improvements,
        total_regressions,
        total_stable,
        total_unstable,
        verbose,
        args.regressions_percent_lower_limit,
        regressions_list,
        improvements_list,
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
    running_platform,
    table_output,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    total_comparison_points,
    total_improvements,
    total_regressions,
    total_stable,
    total_unstable,
    verbose,
    regressions_percent_lower_limit,
    regressions_list=[],
    improvements_list=[],
):
    if total_comparison_points > 0:
        comment_body = "### Automated performance analysis summary\n\n"
        comment_body += "This comment was automatically generated given there is performance data available.\n\n"
        if running_platform is not None:
            comment_body += "Using platform named: {} to do the comparison.\n\n".format(
                running_platform
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
            comparison_summary += "- Detected a total of {} improvements above the improvement water line.\n".format(
                total_improvements
            )
            if len(improvements_list) > 0:
                regression_values = [l[1] for l in improvements_list]
                regression_df = pd.DataFrame(regression_values)
                median_regression = round(float(regression_df.median().iloc[0]), 1)
                max_regression = round(float(regression_df.max().iloc[0]), 1)
                min_regression = round(float(regression_df.min().iloc[0]), 1)

                comparison_summary += f"   - Median/Common-Case improvement was {median_regression}% and ranged from [{min_regression}%,{max_regression}%].\n"

        if total_regressions > 0:
            comparison_summary += "- Detected a total of {} regressions bellow the regression water line {}.\n".format(
                total_regressions, regressions_percent_lower_limit
            )
            if len(regressions_list) > 0:
                regression_values = [l[1] for l in regressions_list]
                regression_df = pd.DataFrame(regression_values)
                median_regression = round(float(regression_df.median().iloc[0]), 1)
                max_regression = round(float(regression_df.max().iloc[0]), 1)
                min_regression = round(float(regression_df.min().iloc[0]), 1)

                comparison_summary += f"   - Median/Common-Case regression was {median_regression}% and ranged from [{min_regression}%,{max_regression}%].\n"

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
                tf_triggering_env,
                tf_github_org,
                tf_github_repo,
            )
            logging.info(
                "Populating the pull request performance ZSETs: {} with branch {}".format(
                    zset_project_pull_request, comparison_branch
                )
            )
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
    tf_triggering_env,
    metric_name,
    comparison_branch,
    baseline_branch="master",
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
    running_platform=None,
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
    ) = get_overall_dashboard_keynames(tf_github_org, tf_github_repo, tf_triggering_env)
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
    else:
        test_names = get_test_names_from_db(
            rts, tags_regex_string, test_names, used_key
        )
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
        tf_triggering_env,
        verbose,
        running_platform,
        baseline_github_repo,
        comparison_github_repo,
        baseline_github_org,
        comparison_github_org,
        regression_str,
        improvement_str,
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

    return (
        detected_regressions,
        table_output,
        improvements_list,
        regressions_list,
        total_stable,
        total_unstable,
        total_comparison_points,
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
        if comparison_covered:
            logging.error(
                "--comparison-branch, --comparison-tag, --comparison-hash, --comparison-target-branch, and --comparison-target-table are mutually exclusive. Pick one..."
            )
            exit(1)
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
    tf_triggering_env,
    verbose,
    running_platform=None,
    baseline_github_repo="redis",
    comparison_github_repo="redis",
    baseline_github_org="redis",
    comparison_github_org="redis",
    regression_str="REGRESSION",
    improvement_str="IMPROVEMENT",
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
    for test_name in test_names:
        compare_version = "main"
        github_link = "https://github.com/redis/redis-benchmarks-specification/blob"
        test_path = f"redis_benchmarks_specification/test-suites/{test_name}.yml"
        test_link = f"[{test_name}]({github_link}/{compare_version}/{test_path})"
        multi_value_baseline = check_multi_value_filter(baseline_str)
        multi_value_comparison = check_multi_value_filter(comparison_str)

        filters_baseline = [
            "{}={}".format(by_str_baseline, baseline_str),
            "metric={}".format(metric_name),
            "{}={}".format(test_filter, test_name),
            "deployment_name={}".format(baseline_deployment_name),
            "github_repo={}".format(baseline_github_repo),
            "triggering_env={}".format(tf_triggering_env),
        ]
        if baseline_github_org != "":
            filters_baseline.append(f"github_org={baseline_github_org}")
        if running_platform is not None:
            filters_baseline.append("running_platform={}".format(running_platform))
        filters_comparison = [
            "{}={}".format(by_str_comparison, comparison_str),
            "metric={}".format(metric_name),
            "{}={}".format(test_filter, test_name),
            "deployment_name={}".format(comparison_deployment_name),
            "github_repo={}".format(comparison_github_repo),
            "triggering_env={}".format(tf_triggering_env),
        ]
        if comparison_github_org != "":
            filters_comparison.append(f"github_org={comparison_github_org}")
        if "hash" not in by_str_baseline:
            filters_baseline.append("hash==")
        if "hash" not in by_str_comparison:
            filters_comparison.append("hash==")
        if running_platform is not None:
            filters_comparison.append("running_platform={}".format(running_platform))
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
            continue

        if len(comparison_timeseries) > 1 and multi_value_comparison is False:
            comparison_timeseries = get_only_Totals(comparison_timeseries)
        if len(comparison_timeseries) != 1 and multi_value_comparison is False:
            if verbose:
                logging.warning(
                    "Comparison timeseries {}".format(len(comparison_timeseries))
                )
            continue

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
                datapoints_inner = rts.ts().revrange(
                    ts_name_baseline, from_ts_ms, to_ts_ms
                )
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
            if regressions_percent_lower_limit < largest_variance:
                note = "waterline={:.1f}%.".format(largest_variance)
                waterline = largest_variance

        except redis.exceptions.ResponseError:
            pass
        except ZeroDivisionError as e:
            logging.error("Detected a ZeroDivisionError. {}".format(e.__str__()))
            pass
        unstable = False
        if (
            baseline_v != "N/A"
            and comparison_pct_change != "N/A"
            and comparison_v != "N/A"
            and baseline_pct_change != "N/A"
        ):
            if comparison_pct_change > 10.0 or baseline_pct_change > 10.0:
                note = "UNSTABLE (very high variance)"
                unstable = True

            baseline_v_str = prepare_value_str(
                baseline_pct_change, baseline_v, baseline_values, simplify_table
            )
            comparison_v_str = prepare_value_str(
                comparison_pct_change, comparison_v, comparison_values, simplify_table
            )

            if metric_mode == "higher-better":
                percentage_change = (
                    float(comparison_v) / float(baseline_v) - 1
                ) * 100.0
            else:
                # lower-better
                percentage_change = (
                    float(baseline_v) / float(comparison_v) - 1
                ) * 100.0
        else:
            logging.warn(
                f"Missing data for test {test_name}. baseline_v={baseline_v} (pct_change={baseline_pct_change}), comparison_v={comparison_v} (pct_change={comparison_pct_change}) "
            )
        if baseline_v != "N/A" or comparison_v != "N/A":
            detected_regression = False
            detected_improvement = False
            if percentage_change < 0.0:
                if -waterline >= percentage_change:
                    detected_regression = True
                    total_regressions = total_regressions + 1
                    note = note + f" {regression_str}"
                    detected_regressions.append(test_name)
                elif percentage_change < -noise_waterline:
                    if simplify_table is False:
                        note = note + f" potential {regression_str}"
                else:
                    if simplify_table is False:
                        note = note + " No Change"

            if percentage_change > 0.0:
                if percentage_change > waterline:
                    detected_improvement = True
                    total_improvements = total_improvements + 1
                    note = note + f" {improvement_str}"
                elif percentage_change > noise_waterline:
                    if simplify_table is False:
                        note = note + f" potential {improvement_str}"
                else:
                    if simplify_table is False:
                        note = note + " No Change"

            if (
                detected_improvement is False
                and detected_regression is False
                and not unstable
            ):
                total_stable = total_stable + 1

            if unstable:
                total_unstable += 1

            should_add_line = False
            line = get_line(
                baseline_v_str,
                comparison_v_str,
                note,
                percentage_change,
                test_link,
            )
            if detected_regression:
                regressions_list.append([test_name, percentage_change])
                table_regressions.append(line)

            if detected_improvement:
                improvements_list.append([test_name, percentage_change])
                table_improvements.append(line)

            if unstable:
                table_unstable.append(line)
            else:
                if not detected_regression and not detected_improvement:
                    table_stable.append(line)

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
    )


def get_only_Totals(baseline_timeseries):
    logging.warning("\t\tTime-series: {}".format(", ".join(baseline_timeseries)))
    logging.info("Checking if Totals will reduce timeseries.")
    new_base = []
    for ts_name in baseline_timeseries:
        if "Totals" in ts_name:
            new_base.append(ts_name)
    baseline_timeseries = new_base
    return baseline_timeseries


def check_multi_value_filter(baseline_str):
    multi_value_baseline = False
    if "(" in baseline_str and "," in baseline_str and ")" in baseline_str:
        multi_value_baseline = True
    return multi_value_baseline


def prepare_value_str(baseline_pct_change, baseline_v, baseline_values, simplify_table):
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


def get_test_names_from_db(rts, tags_regex_string, test_names, used_key):
    try:
        test_names = rts.smembers(used_key)
        test_names = list(test_names)
        test_names.sort()
        final_test_names = []
        for test_name in test_names:
            if not isinstance(test_name, str):
                test_name = test_name.decode()
            match_obj = re.search(tags_regex_string, test_name)
            if match_obj is not None:
                final_test_names.append(test_name)
        test_names = final_test_names

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
