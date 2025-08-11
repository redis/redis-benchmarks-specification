#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs
#  All rights reserved.
#
import os
import logging
from pathlib import Path

import docker

from redis_benchmarks_specification.__cli__.args import spec_cli_args
from redis_benchmarks_specification.__cli__.cli import trigger_tests_cli_command_logic
from redis_benchmarks_specification.__common__.builder_schema import (
    commit_schema_to_stream,
    get_branch_version_from_test_details,
)
import redis

from redis_benchmarks_specification.__builder__.builder import (
    builder_consumer_group_create,
    builder_process_stream,
    build_spec_image_prefetch,
)
from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
    STREAM_KEYNAME_NEW_BUILD_EVENTS,
)
from redis_benchmarks_specification.__common__.timeseries import (
    get_ts_metric_name,
    get_overall_dashboard_keynames,
)
from redis_benchmarks_specification.__self_contained_coordinator__.runners import (
    build_runners_consumer_group_create,
)
from redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator import (
    self_contained_coordinator_blocking_read,
)
from redis_benchmarks_specification.__setups__.topologies import get_topologies


def test_build_spec_image_prefetch():
    builders_folder = "./redis_benchmarks_specification/setups/builders"
    different_build_specs = ["gcc:15.2.0-amd64-debian-bookworm-default.yml"]
    prefetched_images, total_fetched = build_spec_image_prefetch(
        builders_folder, different_build_specs
    )
    assert total_fetched >= 0 and total_fetched <= 2
    assert "gcc:15.2.0-bookworm" in prefetched_images


def test_commit_schema_to_stream_then_build():
    try:
        if should_run_builder():
            db_port = int(os.getenv("DATASINK_PORT", "6379"))
            conn = redis.StrictRedis(port=db_port)
            conn.ping()
            conn.flushall()
            builder_consumer_group_create(conn, "0")
            events_in_pipe = conn.xlen(STREAM_KEYNAME_GH_EVENTS_COMMIT)
            if events_in_pipe == 0:
                result, reply_fields, error_msg = commit_schema_to_stream(
                    {
                        "git_hash": "0cf2df84d4b27af4bffd2bf3543838f09e10f874",
                        "git_branch": "unstable",
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
            builders_folder = "./redis_benchmarks_specification/setups/builders"
            different_build_specs = ["gcc:15.2.0-amd64-debian-bookworm-default.yml"]
            previous_id = ">"
            (
                previous_id,
                new_builds_count,
                build_stream_fields_arr,
            ) = builder_process_stream(
                builders_folder, conn, different_build_specs, previous_id
            )
            assert new_builds_count == 1
            assert len(build_stream_fields_arr) == 1
            assert build_stream_fields_arr[0]["tests_regexp"] == ".*"
            assert conn.exists(STREAM_KEYNAME_NEW_BUILD_EVENTS)
            conn.save()

    except redis.exceptions.ConnectionError:
        pass


def should_run_builder():
    run_builder = True
    TST_BUILDER_X = os.getenv("TST_BUILDER_X", "0")
    if TST_BUILDER_X == "0":
        run_builder = False
    return run_builder


def test_commit_schema_to_stream_then_build_historical_redis():
    try:
        if should_run_builder():
            db_port = int(os.getenv("DATASINK_PORT", "6379"))
            conn = redis.StrictRedis(port=db_port)
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
            builders_folder = "./redis_benchmarks_specification/setups/builders"
            different_build_specs = ["gcc:15.2.0-amd64-debian-bookworm-default.yml"]
            previous_id = ">"
            previous_id, new_builds_count, _ = builder_process_stream(
                builders_folder, conn, different_build_specs, previous_id
            )
            assert new_builds_count == 1
            assert conn.exists(STREAM_KEYNAME_NEW_BUILD_EVENTS)
            conn.save()

    except redis.exceptions.ConnectionError:
        pass


def test_get_branch_version_from_test_details():
    testDetails = {b"ref_label": "/refs/heads/unstable"}
    git_branch, _ = get_branch_version_from_test_details(testDetails)
    assert git_branch == "unstable"
    testDetails = {b"ref_label": "unstable"}
    git_branch, _ = get_branch_version_from_test_details(testDetails)
    assert git_branch == "unstable"
    testDetails = {b"git_version": "555.555.555"}
    _, git_version = get_branch_version_from_test_details(testDetails)
    assert git_version == "555.555.555"


def test_cli_build():
    try:
        if should_run_builder():
            db_port = int(os.getenv("DATASINK_PORT", "6379"))
            conn = redis.StrictRedis(port=db_port)
            conn.ping()
            conn.flushall()

            builder_consumer_group_create(conn, "0")

            import argparse

            run_image = "debian:bookworm"
            github_org = "valkey-io"
            github_repo = "valkey"
            git_hash = "7795152fff06f8200f5e4239ff612b240f638e14"
            git_branch = "unstable"

            # should error due to missing --use-tags or --use-branch
            parser = argparse.ArgumentParser(
                description="test",
                formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            )
            parser = spec_cli_args(parser)
            run_args = [
                "--docker-dont-air-gap",
                "--server_name",
                "valkey",
                "--run_image",
                run_image,
                "--gh_org",
                github_org,
                "--gh_repo",
                github_repo,
                "--redis_port",
                "{}".format(db_port),
                "--use-branch",
                "--build_artifacts",
                "valkey-server",
                "--build_command",
                "sh -c 'make -j'",
                "--git_hash",
                git_hash,
                "--branch",
                git_branch,
            ]
            logging.info("running with args: {}".format(" ".join(run_args)))
            args = parser.parse_args(
                args=run_args,
            )
            try:
                trigger_tests_cli_command_logic(args, "tool", "v0")
            except SystemExit as e:
                assert e.code == 0
            assert STREAM_KEYNAME_GH_EVENTS_COMMIT.encode() in conn.keys()
            events_in_pipe = conn.xlen(STREAM_KEYNAME_GH_EVENTS_COMMIT)
            assert events_in_pipe > 0
            builders_folder = "./redis_benchmarks_specification/setups/builders"
            different_build_specs = ["gcc:15.2.0-amd64-debian-bookworm-default.yml"]
            previous_id = ">"
            previous_id, new_builds_count, _ = builder_process_stream(
                builders_folder, conn, different_build_specs, previous_id
            )
            assert new_builds_count == 1
            assert conn.exists(STREAM_KEYNAME_NEW_BUILD_EVENTS)

            running_platform = "fco-ThinkPad-T490"

            build_runners_consumer_group_create(conn, running_platform, "0")
            datasink_conn = redis.StrictRedis(port=db_port)
            docker_client = docker.from_env()
            home = str(Path.home())
            stream_id = ">"
            topologies_map = get_topologies(
                "./redis_benchmarks_specification/setups/topologies/topologies.yml"
            )
            # we use a benchmark spec with smaller CPU limit for client given github machines only contain 2 cores
            # and we need 1 core for DB and another for CLIENT
            testsuite_spec_files = [
                "./utils/tests/test_data/test-suites/memtier_benchmark-1Mkeys-100B-expire-use-case.yml"
            ]
            (
                result,
                stream_id,
                number_processed_streams,
                _,
            ) = self_contained_coordinator_blocking_read(
                conn,
                True,
                docker_client,
                home,
                stream_id,
                datasink_conn,
                testsuite_spec_files,
                topologies_map,
                running_platform,
                False,
                [],
                "",
                0,
                6399,
                1,
                False,
                1,
                None,
                "amd64",
                None,
                0,
                10000,
                "unstable",
                "",
                True,
            )
            assert result == True
            assert number_processed_streams == 1

            test_name = "memtier_benchmark-1Mkeys-100B-expire-use-case"
            tf_triggering_env = "ci"
            deployment_name = "oss-standalone"
            deployment_type = "oss-standalone"
            use_metric_context_path = False
            metric_context_path = None
            gh_org = github_org
            gh_repo = github_repo
            build_variant_name = "gcc:15.2.0-amd64-debian-bookworm-default"
            for metric_name in ["ALL_STATS.Totals.Latency", "ALL_STATS.Totals.Ops/sec"]:
                ts_key_name = get_ts_metric_name(
                    "by.branch",
                    "unstable",
                    gh_org,
                    gh_repo,
                    deployment_name,
                    deployment_type,
                    test_name,
                    tf_triggering_env,
                    metric_name,
                    metric_context_path,
                    use_metric_context_path,
                    build_variant_name,
                    running_platform,
                )
                rts = datasink_conn.ts()
                assert ts_key_name.encode() in conn.keys()
                assert len(rts.range(ts_key_name, 0, "+")) == 1
            (
                prefix,
                testcases_setname,
                deployment_name_setname,
                tsname_project_total_failures,
                tsname_project_total_success,
                running_platforms_setname,
                build_variant_setname,
                testcases_metric_context_path_setname,
                testcases_and_metric_context_path_setname,
                project_archs_setname,
                project_oss_setname,
                project_branches_setname,
                project_versions_setname,
                project_compilers_setname,
            ) = get_overall_dashboard_keynames(
                gh_org,
                gh_repo,
                tf_triggering_env,
                build_variant_name,
                running_platform,
                test_name,
            )

            assert datasink_conn.exists(testcases_setname)
            assert datasink_conn.exists(running_platforms_setname)
            assert datasink_conn.exists(build_variant_setname)
            assert datasink_conn.exists(testcases_and_metric_context_path_setname)
            assert datasink_conn.exists(testcases_metric_context_path_setname)
            assert build_variant_name.encode() in datasink_conn.smembers(
                build_variant_setname
            )
            assert test_name.encode() in datasink_conn.smembers(testcases_setname)
            assert running_platform.encode() in datasink_conn.smembers(
                running_platforms_setname
            )
            testcases_and_metric_context_path_members = [
                x.decode()
                for x in datasink_conn.smembers(
                    testcases_and_metric_context_path_setname
                )
            ]
            metric_context_path_members = [
                x.decode()
                for x in datasink_conn.smembers(testcases_metric_context_path_setname)
            ]
            assert len(testcases_and_metric_context_path_members) == len(
                metric_context_path_members
            )

            assert [x.decode() for x in datasink_conn.smembers(testcases_setname)] == [
                test_name
            ]

            assert "amd64".encode() in datasink_conn.smembers(project_archs_setname)
            assert "debian-bookworm".encode() in datasink_conn.smembers(
                project_oss_setname
            )
            assert "gcc".encode() in datasink_conn.smembers(project_compilers_setname)
            assert build_variant_name.encode() in datasink_conn.smembers(
                build_variant_setname
            )
            assert running_platform.encode() in datasink_conn.smembers(
                running_platforms_setname
            )

            assert len(datasink_conn.smembers(project_archs_setname)) == 1
            assert len(datasink_conn.smembers(project_oss_setname)) == 1
            assert len(datasink_conn.smembers(project_compilers_setname)) == 1
            assert len(datasink_conn.smembers(build_variant_setname)) == 1
            assert len(datasink_conn.smembers(running_platforms_setname)) == 1
            assert len(datasink_conn.smembers(testcases_setname)) == 1
            assert len(datasink_conn.smembers(project_branches_setname)) == 1
            assert len(datasink_conn.smembers(project_versions_setname)) == 1

            # get all keys
            all_keys = datasink_conn.keys("*")
            by_hash_keys = []
            for key in all_keys:
                if "/by.hash/" in key.decode():
                    by_hash_keys.append(key)

            # ensure we have by hash keys
            assert len(by_hash_keys) > 0
            for hash_key in by_hash_keys:
                # ensure we have both version and hash info on the key
                assert "branch" in rts.info(hash_key).labels
                assert "hash" in rts.info(hash_key).labels

    except redis.exceptions.ConnectionError:
        pass
