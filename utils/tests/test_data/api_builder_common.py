from redis_benchmarks_specification.__common__.builder_schema import (
    commit_schema_to_stream,
)
from redis_benchmarks_specification.__builder__.builder import (
    builder_consumer_group_create,
    builder_process_stream,
)
from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
    STREAM_KEYNAME_NEW_BUILD_EVENTS,
    GH_TOKEN,
)


def flow_1_and_2_api_builder_checks(
    conn, build_spec_name="gcc:8.5.0-amd64-debian-buster-default"
):
    builder_consumer_group_create(conn)
    assert conn.xlen(STREAM_KEYNAME_GH_EVENTS_COMMIT) == 0
    result, reply_fields, error_msg = commit_schema_to_stream(
        {
            "git_hash": "0cf2df84d4b27af4bffd2bf3543838f09e10f874",
            "git_branch": "unstable",
            "use_git_timestamp": True,
        },
        conn,
        "redis",
        "redis",
        GH_TOKEN,
    )

    assert result == True
    assert error_msg == None
    assert STREAM_KEYNAME_GH_EVENTS_COMMIT.encode() in conn.keys()
    assert conn.xlen(STREAM_KEYNAME_GH_EVENTS_COMMIT) == 1
    assert "id" in reply_fields
    builders_folder = "./redis_benchmarks_specification/setups/builders"
    different_build_specs = ["{}.yml".format(build_spec_name)]
    previous_id = ">"
    previous_id, new_builds_count = builder_process_stream(
        builders_folder, conn, different_build_specs, previous_id
    )
    assert new_builds_count == 1
    return build_spec_name, reply_fields
