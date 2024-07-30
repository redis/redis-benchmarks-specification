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
import git
import packaging
import redis
from packaging import version


from redis_benchmarks_specification.__cli__.args import spec_cli_args
from redis_benchmarks_specification.__common__.builder_schema import (
    get_commit_dict_from_sha,
    request_build_from_commit_info,
)
from redis_benchmarks_specification.__common__.env import REDIS_BINS_EXPIRE_SECS
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


def main():
    _, _, project_version = populate_with_poetry_data()
    project_name = "redis-benchmarks-spec-cli"
    parser = argparse.ArgumentParser(
        description=get_version_string(project_name, project_version),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = spec_cli_args(parser)
    args = parser.parse_args()

    cli_command_logic(args, project_name, project_version)


def cli_command_logic(args, project_name, project_version):
    logging.info(
        "Using: {project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )
    if args.use_branch is False and args.use_tags is False:
        logging.error("You must specify either --use-tags or --use-branch flag")
        sys.exit(1)
    redisDirPath = args.redis_repo
    cleanUp = False
    if redisDirPath is None:
        cleanUp = True
        redisDirPath = tempfile.mkdtemp()
        logging.info(
            "Retrieving redis repo from remote into {}. Using branch {}.".format(
                redisDirPath, args.branch
            )
        )
        cmd = "git clone https://github.com/redis/redis {} --branch {}\n".format(
            redisDirPath, args.branch
        )
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
    logging.info(
        "Using the following timeframe: from {} to {}".format(
            args.from_date, args.to_date
        )
    )
    repo = git.Repo(redisDirPath)
    commits = []
    total_commits = 0
    if args.use_branch:
        for commit in repo.iter_commits():

            commit_datetime_utc = datetime.datetime.utcfromtimestamp(
                commit.committed_datetime.timestamp()
            )
            git_timestamp_ms = int(commit_datetime_utc.timestamp() * 1000)
            if args.from_date <= commit_datetime_utc <= args.to_date:
                if (
                    args.last_n > 0 and total_commits < args.last_n
                ) or args.last_n == -1:
                    total_commits = total_commits + 1
                    print(commit.summary)
                    commits.append(
                        {
                            "git_hash": commit.hexsha,
                            "git_branch": repo.active_branch.name,
                            "commit_summary": commit.summary,
                            "git_timestamp_ms": git_timestamp_ms,
                        }
                    )
    if args.use_tags:
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
                            }
                        )
                except packaging.version.InvalidVersion:
                    logging.info(
                        "Ignoring tag {} given we were not able to extract commit or version info from it.".format(
                            tag.name
                        )
                    )
                    pass
    by_description = "n/a"
    if args.use_branch:
        by_description = "from branch {}".format(repo.active_branch.name)
    if args.use_tags:
        by_description = "by tags"
    logging.info(
        "Will trigger {} distinct tests {}.".format(len(commits), by_description)
    )

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
    hash_regexp_string = re.compile(hash_regexp)
    filtered_hash_commits = []
    for cdict in commits:
        commit_hash = cdict["git_hash"]
        commit_summary = cdict["commit_summary"]
        match_obj = re.search(hash_regexp_string, commit_hash)
        if match_obj is None:
            logging.info(
                "Skipping {} given it does not match regex {}".format(
                    commit_hash, hash_regexp_string
                )
            )
        else:
            print(
                "Commit with hash: {} added. summary: {}".format(
                    commit_hash, commit_summary
                )
            )
            filtered_hash_commits.append(cdict)

    if args.dry_run is False:
        conn = redis.StrictRedis(
            host=args.redis_host,
            port=args.redis_port,
            password=args.redis_pass,
            username=args.redis_user,
            decode_responses=False,
        )

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
                    cdict["git_hash"], "redis", "redis", cdict, True, args.gh_token
                )
                if result is True:
                    result, reply_fields, error_msg = request_build_from_commit_info(
                        conn,
                        commit_dict,
                        {},
                        binary_key,
                        binary_value,
                        REDIS_BINS_EXPIRE_SECS,
                    )
                    logging.info(
                        "Successfully requested a build for commit: {}. Request stream id: {}.".format(
                            cdict["git_hash"], reply_fields["id"]
                        )
                    )
                else:
                    logging.error(error_msg)

    else:
        logging.info("Skipping actual work trigger ( dry-run )")
    if cleanUp is True:
        logging.info("Removing temporary redis dir {}.".format(redisDirPath))
        shutil.rmtree(redisDirPath)
