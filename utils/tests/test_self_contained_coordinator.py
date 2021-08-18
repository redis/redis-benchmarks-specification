import os

import docker
import redis
import redistimeseries
import yaml
from pathlib import Path

from redisbench_admin.utils.utils import get_ts_metric_name

from redis_benchmarks_specification.__api__.api import commit_schema_to_stream
from redis_benchmarks_specification.__builder__.builder import (
    builder_consumer_group_create,
    builder_process_stream,
)
from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
    STREAM_KEYNAME_NEW_BUILD_EVENTS,
)
from redis_benchmarks_specification.__common__.spec import (
    extract_client_cpu_limit,
    extract_client_container_image,
)
from redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator import (
    generate_cpuset_cpus,
    self_contained_coordinator_blocking_read,
    build_runners_consumer_group_create,
)
from redis_benchmarks_specification.__setups__.topologies import get_topologies
from utils.tests.test_data.api_builder_common import flow_1_and_2_api_builder_checks


def test_extract_client_cpu_limit():
    with open(
        "./utils/tests/test_data/test-suites/redis-benchmark-full-suite-1Mkeys-100B.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        client_cpu_limit = extract_client_cpu_limit(benchmark_config)
        assert client_cpu_limit == 2


def test_extract_client_container_image():
    with open(
        "./utils/tests/test_data/test-suites/redis-benchmark-full-suite-1Mkeys-100B.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        client_container_image = extract_client_container_image(benchmark_config)
        assert client_container_image == "redis:6.2.4"


def test_generate_cpuset_cpus():
    db_cpuset_cpus, current_cpu_pos = generate_cpuset_cpus(2.0, 0)
    assert db_cpuset_cpus == "0,1"

    db_cpuset_cpus, current_cpu_pos = generate_cpuset_cpus(3, current_cpu_pos)
    assert db_cpuset_cpus == "2,3,4"


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
            if TST_RUNNER_USE_RDB == "0":
                use_rdb = False
            if use_rdb:
                conn.execute_command("DEBUG", "RELOAD", "NOSAVE")
            else:
                conn.flushall()
                flow_1_and_2_api_builder_checks(conn)

            assert conn.exists(STREAM_KEYNAME_NEW_BUILD_EVENTS)
            assert conn.xlen(STREAM_KEYNAME_NEW_BUILD_EVENTS) > 0

            build_runners_consumer_group_create(conn, "0")
            rts = redistimeseries.client.Client(port=16379)
            docker_client = docker.from_env()
            home = str(Path.home())
            stream_id = ">"
            topologies_map = get_topologies(
                "./redis_benchmarks_specification/setups/topologies/topologies.yml"
            )
            testsuite_spec_files = [
                "./redis_benchmarks_specification/test-suites/redis-benchmark-full-suite-1Mkeys-100B.yml"
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
            )
            assert result == True
            assert number_processed_streams == 1
            tf_github_org = "redis"
            tf_github_repo = "redis"
            test_name = "redis-benchmark-full-suite-1Mkeys-100B"
            tf_triggering_env = "ci"
            deployment_type = "oss-standalone"
            metric_name = "rps"
            use_metric_context_path = True
            metric_context_path = "MSET"

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
            )

            assert ts_key_name.encode() in conn.keys()

    except redis.exceptions.ConnectionError:
        pass
