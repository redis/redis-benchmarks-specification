#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import argparse
import datetime
import logging
import shutil
import subprocess
import tempfile
import git
import redis

# logging settings
from redisbench_admin.cli import populate_with_poetry_data
from redisbench_admin.run.common import get_start_time_vars

from redis_benchmarks_specification.__common__.builder_schema import (
    get_commit_dict_from_sha,
    request_build_from_commit_info,
)
from redis_benchmarks_specification.__common__.env import (
    GH_REDIS_SERVER_HOST,
    GH_REDIS_SERVER_AUTH,
    GH_REDIS_SERVER_USER,
    GH_REDIS_SERVER_PORT,
    GH_TOKEN,
)
from redis_benchmarks_specification.__common__.package import get_version_string

logging.basicConfig(
    format="%(asctime)s %(levelname)-4s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

START_TIME_NOW_UTC, _, _ = get_start_time_vars()
START_TIME_LAST_YEAR_UTC = START_TIME_NOW_UTC - datetime.timedelta(days=7)


def main():
    _, _, project_version = populate_with_poetry_data()
    project_name = "redis-benchmarks-spec-cli"
    parser = argparse.ArgumentParser(
        description=get_version_string(project_name, project_version),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--redis_host", type=str, default=GH_REDIS_SERVER_HOST)
    parser.add_argument("--branch", type=str, default="unstable")
    parser.add_argument("--gh_token", type=str, default=GH_TOKEN)
    parser.add_argument("--redis_port", type=int, default=GH_REDIS_SERVER_PORT)
    parser.add_argument("--redis_pass", type=str, default=GH_REDIS_SERVER_AUTH)
    parser.add_argument("--redis_user", type=str, default=GH_REDIS_SERVER_USER)
    parser.add_argument(
        "--from-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        default=START_TIME_LAST_YEAR_UTC,
    )
    parser.add_argument(
        "--to-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        default=START_TIME_NOW_UTC,
    )
    parser.add_argument("--redis_repo", type=str, default=None)
    parser.add_argument("--trigger-unstable-commits", type=bool, default=True)
    parser.add_argument("--dry-run", type=bool, default=False)
    args = parser.parse_args()
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
    Commits = []
    for commit in repo.iter_commits():
        if (
            args.from_date
            <= datetime.datetime.utcfromtimestamp(commit.committed_datetime.timestamp())
            <= args.to_date
        ):
            print(commit.summary)
            Commits.append(commit)
    logging.info(
        "Will trigger {} distinct {} branch commit tests.".format(
            len(Commits), args.branch
        )
    )

    if args.dry_run is False:
        conn = redis.StrictRedis(
            host=args.redis_host,
            port=args.redis_port,
            password=args.redis_pass,
            username=args.redis_user,
            decode_responses=False,
        )
        for rep in range(0, 1):
            for commit in Commits:
                result, error_msg, commit_dict, _ = get_commit_dict_from_sha(
                    commit.hexsha, "redis", "redis", {}, True, args.gh_token
                )
                if result is True:
                    result, reply_fields, error_msg = request_build_from_commit_info(
                        conn, commit_dict, {}
                    )
                    logging.info(
                        "Successfully requested a build for commit: {}. Request stream id: {}. Commit summary: {}".format(
                            commit.hexsha, reply_fields["id"], commit.summary
                        )
                    )
                else:
                    logging.error(error_msg)

    else:
        logging.info("Skipping actual work trigger ( dry-run )")

    if cleanUp is True:
        logging.info("Removing temporary redis dir {}.".format(redisDirPath))
        shutil.rmtree(redisDirPath)
