#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import argparse
import datetime
import logging
import re
import shutil
import subprocess
import sys
import tempfile

import docker
import git
import packaging
import redis
from packaging import version
import time

from redis_benchmarks_specification.__builder__.builder import (
    generate_benchmark_stream_request,
    store_airgap_image_redis,
)
from redis_benchmarks_specification.__common__.github import (
    update_comment_if_needed,
    create_new_pr_comment,
    check_github_available_and_actionable,
    generate_build_finished_pr_comment,
)

from redis_benchmarks_specification.__cli__.args import spec_cli_args
from redis_benchmarks_specification.__cli__.stats import (
    generate_stats_cli_command_logic,
)
from redis_benchmarks_specification.__common__.builder_schema import (
    get_commit_dict_from_sha,
    request_build_from_commit_info,
)
from redis_benchmarks_specification.__common__.env import (
    REDIS_BINS_EXPIRE_SECS,
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
    STREAM_GH_EVENTS_COMMIT_BUILDERS_CG,
    STREAM_KEYNAME_NEW_BUILD_EVENTS,
    get_arch_specific_stream_name,
)
from redis_benchmarks_specification.__common__.package import (
    get_version_string,
    populate_with_poetry_data,
)

# logging settings
logging.basicConfig(
    format="%(asctime)s %(levelname)-4s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


def trigger_tests_dockerhub_cli_command_logic(args, project_name, project_version):
    logging.info(
        "Using: {project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )
    logging.info(
        "Checking connection to redis with user: {}, host: {}, port: {}".format(
            args.redis_user,
            args.redis_host,
            args.redis_port,
        )
    )
    conn = redis.StrictRedis(
        host=args.redis_host,
        port=args.redis_port,
        password=args.redis_pass,
        username=args.redis_user,
        decode_responses=False,
    )
    conn.ping()

    # Extract version from Docker image tag if possible
    # e.g., "redis:7.4.0" -> "7.4.0"
    # e.g., "valkey/valkey:7.2.6-bookworm" -> "7.2.6"
    git_version = None
    if ":" in args.run_image:
        tag = args.run_image.split(":")[-1]
        # Try to extract version number from tag
        # Common patterns: "7.4.0", "7.2.6-bookworm", "latest"
        import re

        version_match = re.match(r"^(\d+\.\d+\.\d+)", tag)
        if version_match:
            git_version = version_match.group(1)
            logging.info(f"Extracted git_version '{git_version}' from image tag")

    testDetails = {}
    build_stream_fields, result = generate_benchmark_stream_request(
        args.id,
        conn,
        args.run_image,
        args.arch,
        testDetails,
        "n/a",
        [],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        git_version,  # Pass extracted version
        None,
        None,
        None,
        None,
        ".*",
        0,
        10000,
        args.tests_regexp,
        ".*",  # command_regexp
        False,  # use_git_timestamp
        "redis",  # server_name
        "redis",  # github_org
        "redis",  # github_repo
        None,  # existing_artifact_keys
    )
    build_stream_fields["github_repo"] = args.gh_repo
    build_stream_fields["github_org"] = args.gh_org
    build_stream_fields["restore_build_artifacts"] = "False"
    server_name = args.gh_repo
    if args.server_name is not None:
        server_name = args.server_name
    build_stream_fields["server_name"] = server_name
    build_stream_fields["mnt_point"] = args.mnt_point
    if args.docker_dont_air_gap is False:
        docker_client = docker.from_env()
        store_airgap_image_redis(conn, docker_client, args.run_image)

    if result is True:
        # Use architecture-specific stream
        arch_specific_stream = get_arch_specific_stream_name(args.arch)
        logging.info(
            f"CLI adding work to architecture-specific stream: {arch_specific_stream}"
        )
        benchmark_stream_id = conn.xadd(arch_specific_stream, build_stream_fields)
        logging.info(
            "sucessfully requested a new run {}. Stream id: {}".format(
                build_stream_fields, benchmark_stream_id
            )
        )


def main():
    _, _, project_version = populate_with_poetry_data()
    project_name = "redis-benchmarks-spec-cli"
    parser = argparse.ArgumentParser(
        description=get_version_string(project_name, project_version),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = spec_cli_args(parser)
    args = parser.parse_args()
    if args.tool == "trigger":
        trigger_tests_cli_command_logic(args, project_name, project_version)
    if args.tool == "stats":
        generate_stats_cli_command_logic(args, project_name, project_version)
    if args.tool == "dockerhub":
        trigger_tests_dockerhub_cli_command_logic(args, project_name, project_version)


def get_commits_by_branch(args, repo):
    total_commits = 0
    commits = []
    for commit in repo.iter_commits():
        commit_datetime = commit.committed_datetime
        git_timestamp_ms = int(
            datetime.datetime.utcfromtimestamp(commit_datetime.timestamp()).timestamp()
            * 1000
        )
        if (
            args.from_date
            <= datetime.datetime.utcfromtimestamp(commit_datetime.timestamp())
            <= args.to_date
        ):
            if (args.last_n > 0 and total_commits < args.last_n) or args.last_n == -1:
                total_commits = total_commits + 1
                print(commit.summary)
                commits.append(
                    {
                        "git_hash": commit.hexsha,
                        "git_branch": repo.active_branch.name,
                        "commit_summary": commit.summary,
                        "commit_datetime": str(commit_datetime),
                        "git_timestamp_ms": git_timestamp_ms,
                    }
                )
    return commits, total_commits


def get_commits_by_tags(args, repo):
    commits = []
    tags_regexp = args.tags_regexp
    if tags_regexp == ".*":
        logging.info(
            "Acception all tags that follow semver between the timeframe. If you need further filter specify a regular expression via --tags-regexp"
        )
    else:
        logging.info(
            "Filtering all tags via a regular expression: {}".format(tags_regexp)
        )
    tags_regex_string = re.compile(tags_regexp)

    tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime)
    for tag in tags:

        git_timestamp_ms = int(
            datetime.datetime.utcfromtimestamp(
                tag.commit.committed_datetime.timestamp()
            ).timestamp()
            * 1000
        )

        if (
            args.from_date
            <= datetime.datetime.utcfromtimestamp(
                tag.commit.committed_datetime.timestamp()
            )
            <= args.to_date
        ):
            try:
                version.Version(tag.name)
                match_obj = re.search(tags_regex_string, tag.name)
                if match_obj is None:
                    logging.info(
                        "Skipping {} given it does not match regex {}".format(
                            tag.name, tags_regexp
                        )
                    )
                else:
                    git_version = tag.name
                    commit_datetime = str(tag.commit.committed_datetime)
                    print(
                        "Commit summary: {}. Extract semver: {}".format(
                            tag.commit.summary, git_version
                        )
                    )
                    commits.append(
                        {
                            "git_hash": tag.commit.hexsha,
                            "git_version": git_version,
                            "commit_summary": tag.commit.summary,
                            "commit_datetime": commit_datetime,
                            "git_timestamp_ms": git_timestamp_ms,
                        }
                    )
            except packaging.version.InvalidVersion:
                logging.info(
                    "Ignoring tag {} given we were not able to extract commit or version info from it.".format(
                        tag.name
                    )
                )
                pass
    return commits


def get_repo(args):
    redisDirPath = args.redis_repo
    cleanUp = False
    last_n = args.last_n
    if redisDirPath is None:
        cleanUp = True
        redisDirPath = tempfile.mkdtemp()
        remote_url = f"https://github.com/{args.gh_org}/{args.gh_repo}"
        logging.info(
            f"Retrieving redis repo from remote {remote_url} into {redisDirPath}. Using branch {args.branch}."
        )
        depth_str = ""
        if last_n > 0:
            depth_str = f" --depth {last_n}"
        cmd = f"git clone {remote_url} {redisDirPath} --branch {args.branch} {depth_str}\n"
        process = subprocess.Popen(
            "/bin/bash", stdin=subprocess.PIPE, stdout=subprocess.PIPE
        )
        process.communicate(cmd.encode())
    else:
        logging.info(
            "Using the following redis repo to retrieve versions info {}. No need to fetch remote data.".format(
                redisDirPath
            )
        )
    return redisDirPath, cleanUp


def check_benchmark_run_comment(comments):
    res = False
    pos = -1
    for n, comment in enumerate(comments):
        body = comment.body
        if "CE Performance Automation" in body and "Triggered a benchmark" in body:
            res = True
            pos = n
    return res, pos


def check_benchmark_build_comment(comments):
    res = False
    pos = -1
    for n, comment in enumerate(comments):
        body = comment.body
        if "CE Performance Automation : step 1 of 2" in body:
            res = True
            pos = n
    return res, pos


def trigger_tests_cli_command_logic(args, project_name, project_version):
    logging.info(
        "Using: {project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )

    if args.use_branch is False and args.use_tags is False:
        logging.error("You must specify either --use-tags or --use-branch flag")
        sys.exit(1)

    github_token = args.github_token
    pull_request = args.pull_request
    verbose = True
    auto_approve = args.auto_approve

    fn = check_benchmark_build_comment
    (
        contains_regression_comment,
        github_pr,
        is_actionable_pr,
        old_regression_comment_body,
        pr_link,
        regression_comment,
    ) = check_github_available_and_actionable(
        fn, github_token, pull_request, "redis", "redis", verbose
    )

    redisDirPath, cleanUp = get_repo(args)
    repo = git.Repo(redisDirPath)

    logging.info(
        "Using the following timeframe: from {} to {}".format(
            args.from_date, args.to_date
        )
    )

    commits = []
    if args.use_branch:
        commits, total_commits = get_commits_by_branch(args, repo)
    if args.use_tags:
        commits = get_commits_by_tags(args, repo)

    by_description = "n/a"
    if args.use_branch:
        by_description = "from branch {}".format(repo.active_branch.name)
    if args.use_tags:
        by_description = "by tags"
    logging.info(
        "Will trigger {} distinct tests {}.".format(len(commits), by_description)
    )
    if args.platform:
        logging.info("Will trigger tests only for platform {}".format(args.platform))

    hash_regexp = args.hash_regexp
    if hash_regexp == ".*":
        logging.info(
            "Acception all commit hashes. If you need further filter specify a regular expression via --hash-regexp"
        )
    else:
        logging.info(
            "Filtering all commit hashes via a regular expression: {}".format(
                hash_regexp
            )
        )
    enable_hash_filtering = False
    hash_filters = []
    if args.git_hash != "":
        enable_hash_filtering = True
        hash_filters = args.git_hash.split(",")
        logging.info(
            f"There is a total of {len(hash_filters)} commit hash fitlers: {hash_filters}"
        )
    hash_regexp_string = re.compile(hash_regexp)
    filtered_hash_commits = []
    for cdict in commits:
        commit_hash = cdict["git_hash"]
        if enable_hash_filtering:
            if commit_hash not in hash_filters:
                logging.info(
                    f"Skipping {commit_hash} given it does not match any commit hash in {hash_filters}"
                )
                continue
        commit_summary = cdict["commit_summary"]
        commit_datetime = cdict["commit_datetime"]
        match_obj = re.search(hash_regexp_string, commit_hash)
        if match_obj is None:
            logging.info(
                "Skipping {} given it does not match regex {}".format(
                    commit_hash, hash_regexp_string
                )
            )
        else:
            print(
                f"Commit with hash: {commit_hash} from {commit_datetime} added. summary: {commit_summary}"
            )
            filtered_hash_commits.append(cdict)

    logging.info(
        "Checking connection to redis with user: {}, host: {}, port: {}".format(
            args.redis_user,
            args.redis_host,
            args.redis_port,
        )
    )
    conn = redis.StrictRedis(
        host=args.redis_host,
        port=args.redis_port,
        password=args.redis_pass,
        username=args.redis_user,
        decode_responses=False,
    )
    conn.ping()
    for rep in range(0, 1):
        for cdict in filtered_hash_commits:
            (
                result,
                error_msg,
                commit_dict,
                _,
                binary_key,
                binary_value,
            ) = get_commit_dict_from_sha(
                cdict["git_hash"],
                args.gh_org,
                args.gh_repo,
                cdict,
                True,
                args.gh_token,
            )
            if args.platform:
                commit_dict["platform"] = args.platform
            tests_priority_upper_limit = args.tests_priority_upper_limit
            tests_priority_lower_limit = args.tests_priority_lower_limit
            tests_regexp = args.tests_regexp
            tests_groups_regexp = args.tests_groups_regexp
            commit_dict["tests_priority_upper_limit"] = tests_priority_upper_limit
            commit_dict["tests_priority_lower_limit"] = tests_priority_lower_limit
            commit_dict["tests_regexp"] = tests_regexp
            commit_dict["tests_groups_regexp"] = tests_groups_regexp
            commit_dict["github_org"] = args.gh_org
            commit_dict["github_repo"] = args.gh_repo
            if args.arch is not None:
                commit_dict["build_arch"] = args.arch
                commit_dict["arch"] = args.arch
            if args.server_name is not None and args.server_name != "":
                commit_dict["server_name"] = args.server_name
            if args.build_artifacts != "":
                commit_dict["build_artifacts"] = args.build_artifacts
            if args.build_command != "":
                commit_dict["build_command"] = args.build_command
            if pull_request is not None:
                logging.info(
                    f"Have a pull request info to include in build request {pull_request}"
                )
                commit_dict["pull_request"] = pull_request
            git_hash = cdict["git_hash"]
            git_branch = "n/a"
            if "git_branch" in cdict:
                git_branch = cdict["git_branch"]
            commit_datetime = cdict["commit_datetime"]
            commit_summary = cdict["commit_summary"]
            reply_fields = {}
            use_git_timestamp = args.use_git_timestamp
            if use_git_timestamp is False:
                reply_fields["use_git_timestamp"] = str(use_git_timestamp)

            logging.info(
                f"Setting use use_git_timestamp={use_git_timestamp}. ({args.use_git_timestamp})"
            )

            if result is True:
                stream_id = "n/a"
                if args.dry_run is False:
                    (
                        result,
                        reply_fields,
                        error_msg,
                    ) = request_build_from_commit_info(
                        conn,
                        commit_dict,
                        reply_fields,
                        binary_key,
                        binary_value,
                        REDIS_BINS_EXPIRE_SECS,
                    )
                    stream_id = reply_fields["id"]
                    logging.info(
                        "Successfully requested a build for commit: {}. Date: {} Request stream id: {}. full commited info: {}. Reply fields: {}".format(
                            cdict["git_hash"],
                            cdict["commit_datetime"],
                            stream_id,
                            commit_dict,
                            reply_fields,
                        )
                    )

                    if args.wait_build is True:
                        build_start_datetime = datetime.datetime.utcnow()
                        decoded_stream_id = stream_id.decode()
                        builder_list_streams = (
                            f"builder:{decoded_stream_id}:builds_completed"
                        )
                        len_list = 0
                        stream_ack = False
                        sleep_secs = 10
                        benchmark_stream_ids = []
                        while len_list == 0 or stream_ack is False:

                            logging.info(
                                f"checking benchmark streams info in key: {builder_list_streams}"
                            )
                            benchmark_stream_ids = conn.lrange(
                                builder_list_streams, 0, -1
                            )
                            len_list = len(benchmark_stream_ids)
                            logging.info(
                                f"There is a total of {len_list} already build benchmark stream ids for this build: {benchmark_stream_ids}"
                            )

                            if len_list > 0:
                                pending_build_streams = conn.xpending_range(
                                    STREAM_KEYNAME_GH_EVENTS_COMMIT,
                                    STREAM_GH_EVENTS_COMMIT_BUILDERS_CG,
                                    "-",
                                    "+",
                                    1000,
                                )
                                len_pending = len(pending_build_streams)
                                logging.info(
                                    f"There is a total of {len_pending} pending builds for stream {STREAM_KEYNAME_GH_EVENTS_COMMIT} and cg {STREAM_GH_EVENTS_COMMIT_BUILDERS_CG}. Checking for stream id: {stream_id}"
                                )
                                found_id = False
                                for pending_try in pending_build_streams:
                                    logging.info(f"pending entry: {pending_try}")
                                    pending_id = pending_try["message_id"]
                                    if stream_id == pending_id:
                                        found_id = True
                                        logging.info(
                                            f"Found the stream id {stream_id} as part of pending entry list. Waiting for it to be ack."
                                        )

                                if found_id is True:
                                    logging.info(
                                        f"Sleeping for {sleep_secs} before checking pending list again."
                                    )
                                    time.sleep(sleep_secs)
                                else:
                                    stream_ack = True
                            else:
                                logging.info(
                                    f"Sleeping for {sleep_secs} before checking builds again."
                                )
                                time.sleep(sleep_secs)
                        logging.info(
                            f"FINAL total of {len_list} already build benchmark stream ids for this build: {benchmark_stream_ids}"
                        )
                        build_end_datetime = datetime.datetime.utcnow()
                        build_duration = build_end_datetime - build_start_datetime
                        build_duration_secs = build_duration.total_seconds()

                        comment_body = generate_build_finished_pr_comment(
                            benchmark_stream_ids,
                            commit_datetime,
                            commit_summary,
                            git_branch,
                            git_hash,
                            tests_groups_regexp,
                            tests_priority_lower_limit,
                            tests_priority_upper_limit,
                            tests_regexp,
                            build_start_datetime,
                            build_duration_secs,
                        )
                        if is_actionable_pr:
                            if contains_regression_comment:
                                update_comment_if_needed(
                                    auto_approve,
                                    comment_body,
                                    old_regression_comment_body,
                                    regression_comment,
                                    verbose,
                                )
                            else:
                                create_new_pr_comment(
                                    auto_approve, comment_body, github_pr, pr_link
                                )

                else:
                    logging.info(
                        "DRY-RUN: build for commit: {}. Date: {} Full commited info: {}".format(
                            cdict["git_hash"],
                            cdict["commit_datetime"],
                            commit_dict,
                        )
                    )
            else:
                logging.error(error_msg)
    if cleanUp is True:
        logging.info("Removing temporary redis dir {}.".format(redisDirPath))
        shutil.rmtree(redisDirPath)
