import logging
from json import loads
from urllib.error import URLError
from urllib.request import urlopen

import redis
from marshmallow import Schema, fields

from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
)


def commit_schema_to_stream(json_str: str, conn: redis.StrictRedis):
    """ uses to the provided JSON dict of fields and pushes that info to the corresponding stream  """
    fields = loads(json_str)
    reply_fields = loads(json_str)
    result = False
    error_msg = None
    if "git_hash" not in fields:
        error_msg = "Missing required 'git_hash' field"
    else:
        github_url = "https://github.com/redis/redis/archive/{}.zip".format(
            fields["git_hash"]
        )
        try:
            response = urlopen(github_url, timeout=5)
            content = response.read()
            fields["zip_archive"] = bytes(content)
            fields["zip_archive_len"] = len(bytes(content))
            reply_fields["archived_zip"] = True
            result = True
        except URLError as e:
            error_msg = "Catched URLError while fetching {} content. Error {}".format(
                github_url, e.__str__()
            )
            logging.error(error_msg)
            result = False

    if result is True:
        id = conn.xadd(STREAM_KEYNAME_GH_EVENTS_COMMIT.encode(), fields)
        reply_fields["id"] = id

    return result, reply_fields, error_msg


class CommitSchema(Schema):
    git_branch = fields.String(required=False)
    git_tag = fields.String(required=False)
    git_hash = fields.String(required=True)
