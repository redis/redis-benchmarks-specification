#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
from urllib.error import URLError
from urllib.request import urlopen
from github import Github

import redis

from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
)


def commit_schema_to_stream(
    fields: dict,
    conn: redis.StrictRedis,
    gh_org,
    gh_repo,
    gh_token=None,
):
    """uses to the provided JSON dict of fields and pushes that info to the corresponding stream"""
    fields = fields
    reply_fields = dict(fields)
    result = False
    error_msg = None
    use_git_timestamp = False
    if "use_git_timestamp" in fields:
        use_git_timestamp = bool(fields["use_git_timestamp"])
    if "git_hash" not in fields:
        error_msg = "Missing required 'git_hash' field"
    else:
        (
            result,
            error_msg,
            fields,
            _,
            binary_key,
            binary_value,
        ) = get_commit_dict_from_sha(
            fields["git_hash"],
            gh_org,
            gh_repo,
            fields,
            use_git_timestamp,
            gh_token,
        )
        reply_fields["use_git_timestamp"] = fields["use_git_timestamp"]
        if "git_timestamp_ms" in fields:
            reply_fields["git_timestamp_ms"] = fields["git_timestamp_ms"]
        reply_fields["archived_zip"] = True
    if result is True:
        # 7 days expire
        binary_exp_secs = 24 * 60 * 60 * 7
        result, reply_fields, error_msg = request_build_from_commit_info(
            conn, fields, reply_fields, binary_key, binary_value, binary_exp_secs
        )

    return result, reply_fields, error_msg


def get_archive_zip_from_hash(gh_org, gh_repo, git_hash, fields):
    error_msg = None
    result = False
    binary_value = None
    bin_key = "zipped:source:{}/{}/archive/{}.zip".format(gh_org, gh_repo, git_hash)
    github_url = "https://github.com/{}/{}/archive/{}.zip".format(
        gh_org, gh_repo, git_hash
    )
    try:
        logging.info("Fetching data from {}".format(github_url))
        response = urlopen(github_url, timeout=5)
        content = response.read()
        fields["zip_archive_key"] = bin_key
        fields["zip_archive_len"] = len(bytes(content))
        binary_value = bytes(content)
        result = True
    except URLError as e:
        error_msg = "Catched URLError while fetching {} content. Error {}".format(
            github_url, e.__str__()
        )
        logging.error(error_msg)
        result = False

    return result, bin_key, binary_value, error_msg


def get_commit_dict_from_sha(
    git_hash,
    gh_org,
    gh_repo,
    commit_dict={},
    use_git_timestamp=False,
    gh_token=None,
    gh_branch=None,
):
    commit = None
    # using an access token
    if gh_token is not None:
        g = Github(gh_token)
        repo = g.get_repo("{}/{}".format(gh_org, gh_repo))
        commit = repo.get_commit(sha=git_hash)
        commit_dict["git_timestamp_ms"] = int(
            commit.commit.author.date.timestamp() * 1000.0
        )
    else:
        if "git_timestamp_ms" not in commit_dict:
            use_git_timestamp = False
    commit_dict["use_git_timestamp"] = str(use_git_timestamp)
    commit_dict["git_hash"] = git_hash
    if gh_branch is not None:
        commit_dict["git_branch"] = gh_branch

    result, binary_key, binary_value, error_msg = get_archive_zip_from_hash(
        gh_org,
        gh_repo,
        git_hash,
        commit_dict,
    )
    return result, error_msg, commit_dict, commit, binary_key, binary_value


def request_build_from_commit_info(
    conn, fields, reply_fields, binary_key, binary_value, binary_exp_secs
):
    """Generates a build event from the commit dictionary
    It expected the fields dictionary to contain at least the following keys:
        - "git_branch": reference to the branch that the commit refers to
        - "git_hash": reference to the commit hash
        - "zip_archive": containing the source code archived binary
        - "zip_archive_len": the length of the zip archived
        - "archived_zip": boolean value specifying if the archive is zipped ( always true for now )

    Parameters
    ----------
    conn : redis.Con
        The redis client connection
    fields : dict
        The input commit info dictionary
    reply_fields : dict
        The output reply commit info dictionary

    """
    result = True
    error_msg = None
    conn.set(binary_key, binary_value, ex=binary_exp_secs)
    for k, v in fields.items():
        if type(v) not in [str, int, float, bytes]:
            raise Exception(
                "Type of field {} is not bytes, string, int or float. Type ({}). Value={}".format(
                    k, type(v), v
                )
            )
    id = conn.xadd(STREAM_KEYNAME_GH_EVENTS_COMMIT.encode(), fields)
    reply_fields["id"] = id
    return result, reply_fields, error_msg


def get_branch_version_from_test_details(testDetails):
    git_branch = None
    git_version = None
    if b"git_branch" in testDetails:
        git_branch = testDetails[b"git_branch"]
    if b"ref_label" in testDetails:
        git_branch = testDetails[b"ref_label"]
    if b"git_version" in testDetails:
        git_version = testDetails[b"git_version"]
    if git_branch is not None:
        # remove event prefix
        if type(git_branch) == bytes:
            git_branch = git_branch.decode()
        if git_branch.startswith("/refs/heads/"):
            git_branch = git_branch.replace("/refs/heads/", "")
        if git_branch.startswith("refs/heads/"):
            git_branch = git_branch.replace("refs/heads/", "")
        if git_branch.startswith("/"):
            git_branch = git_branch[1:]
    if git_version is not None:
        if type(git_version) == bytes:
            git_version = git_version.decode()

    return git_branch, git_version
