#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs
#  All rights reserved.
#
import os

from redis_benchmarks_specification.__api__.api import commit_schema_to_stream
import redis

from redis_benchmarks_specification.__builder__.builder import (
    builder_consumer_group_create,
    builder_process_stream,
)
from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
    STREAM_KEYNAME_NEW_BUILD_EVENTS,
)


def test_commit_schema_to_stream_then_build():
    try:
        run_builder = True
        TST_BUILDER_X = os.getenv("TST_BUILDER_X", "1")
        if TST_BUILDER_X == "0":
            run_builder = False
        if run_builder:
            conn = redis.StrictRedis(port=16379)
            conn.ping()
            conn.flushall()
            builder_consumer_group_create(conn)
            assert conn.xlen(STREAM_KEYNAME_GH_EVENTS_COMMIT) == 0

            result, reply_fields, error_msg = commit_schema_to_stream(
                '{"git_hash":"0cf2df84d4b27af4bffd2bf3543838f09e10f874", "git_branch":"unstable"}',
                conn,
            )
            assert result == True
            assert error_msg == None
            assert STREAM_KEYNAME_GH_EVENTS_COMMIT.encode() in conn.keys()
            assert conn.xlen(STREAM_KEYNAME_GH_EVENTS_COMMIT) == 1
            assert "id" in reply_fields
            builders_folder = "./redis_benchmarks_specification/setups/builders"
            different_build_specs = ["gcc:8.5.0-amd64-debian-buster-default.yml"]
            previous_id = ">"
            previous_id, new_builds_count = builder_process_stream(
                builders_folder, conn, different_build_specs, previous_id
            )
            assert new_builds_count == 1
            assert conn.exists(STREAM_KEYNAME_NEW_BUILD_EVENTS)
            conn.save()

    except redis.exceptions.ConnectionError:
        pass
