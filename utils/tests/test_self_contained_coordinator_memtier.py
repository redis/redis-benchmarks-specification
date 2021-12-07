import os

import docker
import redis
import redistimeseries
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
            rts = redistimeseries.client.Client(port=16379)
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
            ) = self_contained_coordinator_blocking_read(
                conn,
                True,
                docker_client,
                home,
                stream_id,
                rts,
                testsuite_spec_files,
                topologies_map,
                running_platform,
            )
            assert result == True
            assert number_processed_streams == 1
            tf_github_org = "redis"
            tf_github_repo = "redis"
            test_name = "memtier_benchmark-1Mkeys-100B-expire-use-case"
            tf_triggering_env = "ci"
            deployment_type = "oss-standalone"
            metric_name = "rps"
            use_metric_context_path = True
            metric_context_path = "SET"

            ts_key_name = get_ts_metric_name(
                "by.branch",
                "unstable",
                tf_github_org,
                tf_github_repo,
                deployment_type,
                test_name,
                tf_triggering_env,
                metric_name,
                metric_context_path,
                use_metric_context_path,
                build_variant_name,
                running_platform,
            )

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
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json --command "SETEX __key__ 10 __value__" --command-key-pattern="R" --command "SET __key__ __value__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
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
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json --cluster-mode --command "SETEX __key__ 10 __value__" --command-key-pattern="R" --command "SET __key__ __value__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )
