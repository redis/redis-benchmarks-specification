#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import redis

from redis_benchmarks_specification.__common__.builder_schema import (
    commit_schema_to_stream,
)

from redis_benchmarks_specification.__builder__.builder import (
    builder_consumer_group_create,
)
from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
)
from utils.tests.test_builder import should_run_builder


def test_commit_schema_to_stream():
    try:
        if should_run_builder():
            conn = redis.StrictRedis(port=6379)
            conn.ping()
            conn.flushall()
            builder_consumer_group_create(conn, "0")
            events_in_pipe = conn.xlen(STREAM_KEYNAME_GH_EVENTS_COMMIT)
            if events_in_pipe == 0:

                result, reply_fields, error_msg = commit_schema_to_stream(
                    {
                        "git_hash": "021af7629590c638ae0d4867d4b397f6e0c38ec8",
                        "git_version": "5.0.13",
                    },
                    conn,
                    "redis",
                    "redis",
                )
                assert result == True
                assert error_msg == None
                assert STREAM_KEYNAME_GH_EVENTS_COMMIT.encode() in conn.keys()
                assert conn.xlen(STREAM_KEYNAME_GH_EVENTS_COMMIT) == 1
                assert "id" in reply_fields

    except redis.exceptions.ConnectionError:
        pass
