#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
from json import loads
from urllib.error import URLError
from urllib.request import urlopen
from github import Github

import redis

from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
)


def commit_schema_to_stream(
    json_str: str,
    conn: redis.StrictRedis,
    gh_org="redis",
    gh_repo="redis",
    gh_token=None,
):
    """ uses to the provided JSON dict of fields and pushes that info to the corresponding stream  """
    fields = loads(json_str)
    reply_fields = loads(json_str)
    result = False
    error_msg = None
    use_git_timestamp = False
    if "use_git_timestamp" in fields:
        use_git_timestamp = bool(fields["use_git_timestamp"])
    if "git_hash" not in fields:
        error_msg = "Missing required 'git_hash' field"
    else:
        result, error_msg, fields, _ = get_commit_dict_from_sha(
            fields["git_hash"], gh_org, gh_repo, fields, use_git_timestamp, gh_token
        )
        reply_fields["use_git_timestamp"] = fields["use_git_timestamp"]
        if "git_timestamp_ms" in fields:
            reply_fields["git_timestamp_ms"] = fields["git_timestamp_ms"]
        reply_fields["archived_zip"] = True
    if result is True:
        result, reply_fields, error_msg = request_build_from_commit_info(
            conn, fields, reply_fields
        )

    return result, reply_fields, error_msg


def get_archive_zip_from_hash(gh_org, gh_repo, git_hash, fields):
    error_msg = None
    result = False
    github_url = "https://github.com/{}/{}/archive/{}.zip".format(
        gh_org, gh_repo, git_hash
    )
    try:
        response = urlopen(github_url, timeout=5)
        content = response.read()
        fields["zip_archive"] = bytes(content)
        fields["zip_archive_len"] = len(bytes(content))
        result = True
    except URLError as e:
        error_msg = "Catched URLError while fetching {} content. Error {}".format(
            github_url, e.__str__()
        )
        logging.error(error_msg)
        result = False
    return result, error_msg


def get_commit_dict_from_sha(
    git_hash,
    gh_org="redis",
    gh_repo="redis",
    commit_dict={},
    use_git_timestamp=False,
    gh_token=None,
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
        use_git_timestamp = False
    commit_dict["use_git_timestamp"] = str(use_git_timestamp)
    commit_dict["git_hash"] = git_hash

    result, error_msg = get_archive_zip_from_hash(
        gh_org, gh_repo, git_hash, commit_dict
    )
    return result, error_msg, commit_dict, commit


def request_build_from_commit_info(conn, fields, reply_fields):
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
    id = conn.xadd(STREAM_KEYNAME_GH_EVENTS_COMMIT.encode(), fields)
    reply_fields["id"] = id
    return result, reply_fields, error_msg
