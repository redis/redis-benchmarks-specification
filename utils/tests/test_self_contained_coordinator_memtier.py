import os

import docker
import redis
import yaml
from pathlib import Path

from redisbench_admin.utils.remote import get_overall_dashboard_keynames
from redisbench_admin.utils.utils import get_ts_metric_name

from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_NEW_BUILD_EVENTS,
)
from redis_benchmarks_specification.__common__.spec import (
    extract_client_tool,
)

from redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator import (
    self_contained_coordinator_blocking_read,
    build_runners_consumer_group_create,
    prepare_memtier_benchmark_parameters,
)
from redis_benchmarks_specification.__setups__.topologies import get_topologies
from utils.tests.test_data.api_builder_common import flow_1_and_2_api_builder_checks


def test_self_contained_coordinator_blocking_read():
    try:
        run_coordinator = True
        TST_RUNNER_X = os.getenv("TST_RUNNER_X", "1")
        if TST_RUNNER_X == "0":
            run_coordinator = False
        if run_coordinator:
            conn = redis.StrictRedis(port=16379)
            conn.ping()
            use_rdb = True
            TST_RUNNER_USE_RDB = os.getenv("TST_RUNNER_USE_RDB", "1")
            build_variant_name = "gcc:8.5.0-amd64-debian-buster-default"
            expected_datapoint_ts = None
            if TST_RUNNER_USE_RDB == "0":
                use_rdb = False
            if use_rdb:
                conn.execute_command("DEBUG", "RELOAD", "NOSAVE")
            else:
                conn.flushall()
                build_variant_name, reply_fields = flow_1_and_2_api_builder_checks(conn)
                expected_datapoint_ts = reply_fields["git_timestamp_ms"]

            assert conn.exists(STREAM_KEYNAME_NEW_BUILD_EVENTS)
            assert conn.xlen(STREAM_KEYNAME_NEW_BUILD_EVENTS) > 0
            running_platform = "fco-ThinkPad-T490"

            build_runners_consumer_group_create(conn, running_platform, "0")
            datasink_conn = redis.StrictRedis(port=16379)
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
            )
            assert result == True
            assert number_processed_streams == 1
            tf_github_org = "redis"
            tf_github_repo = "redis"
            test_name = "memtier_benchmark-1Mkeys-100B-expire-use-case"
            tf_triggering_env = "ci"
            deployment_name = "oss-standalone"
            deployment_type = "oss-standalone"
            use_metric_context_path = True
            metric_context_path = "Gets"
            for metric_name in ["Latency", "Ops/sec"]:
                ts_key_name = get_ts_metric_name(
                    "by.branch",
                    "unstable",
                    tf_github_org,
                    tf_github_repo,
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
                assert len(rts.range(ts_key_name, 0, -1)) == 1
                if expected_datapoint_ts is not None:
                    assert rts.range(ts_key_name, 0, -1)[0][0] == expected_datapoint_ts
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
                tf_github_org,
                tf_github_repo,
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
            assert "debian-buster".encode() in datasink_conn.smembers(
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
            assert len(datasink_conn.smembers(project_versions_setname)) == 0

    except redis.exceptions.ConnectionError:
        pass


def test_self_contained_coordinator_skip_build_variant():
    try:
        run_coordinator = True
        TST_RUNNER_X = os.getenv("TST_RUNNER_X", "1")
        if TST_RUNNER_X == "0":
            run_coordinator = False
        if run_coordinator:
            conn = redis.StrictRedis(port=16379)
            conn.ping()
            use_rdb = True
            TST_RUNNER_USE_RDB = os.getenv("TST_RUNNER_USE_RDB", "1")
            build_variant_name = "gcc:8.5.0-amd64-debian-buster-default"
            expected_datapoint_ts = None
            if TST_RUNNER_USE_RDB == "0":
                use_rdb = False
            if use_rdb:
                conn.execute_command("DEBUG", "RELOAD", "NOSAVE")
            else:
                conn.flushall()
                build_variant_name, reply_fields = flow_1_and_2_api_builder_checks(conn)
                expected_datapoint_ts = reply_fields["git_timestamp_ms"]

            assert conn.exists(STREAM_KEYNAME_NEW_BUILD_EVENTS)
            assert conn.xlen(STREAM_KEYNAME_NEW_BUILD_EVENTS) > 0
            running_platform = "fco-ThinkPad-T490"

            build_runners_consumer_group_create(conn, running_platform, "0")
            datasink_conn = redis.StrictRedis(port=16379)
            docker_client = docker.from_env()
            home = str(Path.home())
            stream_id = ">"
            topologies_map = get_topologies(
                "./redis_benchmarks_specification/setups/topologies/topologies.yml"
            )
            # we use a benchmark spec with smaller CPU limit for client given github machines only contain 2 cores
            # and we need 1 core for DB and another for CLIENT
            testsuite_spec_files = [
                "./utils/tests/test_data/test-suites/memtier_benchmark-1Mkeys-100B-expire-use-case-with-variant.yml"
            ]
            (
                result,
                stream_id,
                number_processed_streams,
                num_process_test_suites,
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
            )
            assert result == True
            assert number_processed_streams == 1
            assert num_process_test_suites == 0

    except redis.exceptions.ConnectionError:
        pass


def test_prepare_memtier_benchmark_parameters():
    with open(
        "./redis_benchmarks_specification/test-suites/memtier_benchmark-1Mkeys-100B-expire-use-case.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        client_tool = extract_client_tool(benchmark_config)
        assert client_tool == "memtier_benchmark"
        local_benchmark_output_filename = "1.json"
        oss_api_enabled = False
        (_, benchmark_command_str,) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            local_benchmark_output_filename,
            oss_api_enabled,
        )
        assert (
            benchmark_command_str
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json "--data-size" "100" --command "SETEX __key__ 10 __value__" --command-key-pattern="R" --command "SET __key__ __value__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )
        oss_api_enabled = True
        (_, benchmark_command_str,) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            local_benchmark_output_filename,
            oss_api_enabled,
        )
        assert (
            benchmark_command_str
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json --cluster-mode "--data-size" "100" --command "SETEX __key__ 10 __value__" --command-key-pattern="R" --command "SET __key__ __value__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )
