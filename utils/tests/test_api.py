#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs
#  All rights reserved.
#

from redis_benchmarks_specification.__common__.builder_schema import (
    commit_schema_to_stream,
    get_commit_dict_from_sha,
)
import redis

from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
    GH_TOKEN,
)


def test_commit_schema_to_stream():
    result, reply_fields, error_msg = commit_schema_to_stream(
        {"git_hashss": "0cf2df84d4b27af4bffd2bf3543838f09e10f874"},
        None,
        "redis",
        "redis",
    )
    assert result == False
    assert error_msg is not None
    try:
        conn = redis.StrictRedis(port=6379)
        conn.ping()
        conn.flushall()
        result, reply_fields, error_msg = commit_schema_to_stream(
            {"git_hash": "0cf2df84d4b27af4bffd2bf3543838f09e10f874"},
            conn,
            "redis",
            "redis",
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


def test_get_commit_dict_from_sha():
    (
        result,
        error_msg,
        commit_dict,
        _,
        binary_key,
        binary_value,
    ) = get_commit_dict_from_sha(
        "492d8d09613cff88f15dcef98732392b8d509eb1", "redis", "redis", {}, True, GH_TOKEN
    )
    if GH_TOKEN is not None:
        assert commit_dict["git_timestamp_ms"] == 1629441465000
