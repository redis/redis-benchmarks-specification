#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs
#  All rights reserved.
#
from redis_benchmarks_specification.__api__.api import commit_schema_to_stream
import redis

from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
)


def test_commit_schema_to_stream():
    result, reply_fields, error_msg = commit_schema_to_stream(
        '{"git_hashss":"0cf2df84d4b27af4bffd2bf3543838f09e10f874"}', None
    )
    assert result == False
    assert error_msg is not None
    try:
        conn = redis.StrictRedis()
        conn.ping()
        conn.flushall()
        result, reply_fields, error_msg = commit_schema_to_stream(
            '{"git_hash":"0cf2df84d4b27af4bffd2bf3543838f09e10f874"}', conn
        )
        assert result == True
        assert error_msg == None
        reply = conn.xread({STREAM_KEYNAME_GH_EVENTS_COMMIT: 0}, 1)
        assert (
            reply[0][1][0][1][b"git_hash"]
            == b"0cf2df84d4b27af4bffd2bf3543838f09e10f874"
        )

    except redis.exceptions.ConnectionError:
        pass
