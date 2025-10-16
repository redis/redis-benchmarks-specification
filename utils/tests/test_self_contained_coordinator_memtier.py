import os

import docker
import redis
import yaml
from pathlib import Path
import logging
import datetime

from redisbench_admin.utils.benchmark_config import get_defaults
from redisbench_admin.utils.remote import get_overall_dashboard_keynames
from redisbench_admin.utils.utils import get_ts_metric_name

from redis_benchmarks_specification.__builder__.builder import (
    generate_benchmark_stream_request,
)
from redis_benchmarks_specification.__cli__.args import spec_cli_args
from redis_benchmarks_specification.__cli__.cli import (
    trigger_tests_dockerhub_cli_command_logic,
)
from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_NEW_BUILD_EVENTS,
    get_arch_specific_stream_name,
)
from redis_benchmarks_specification.__common__.spec import (
    extract_client_tool,
)

from redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator import (
    self_contained_coordinator_blocking_read,
)
from redis_benchmarks_specification.__self_contained_coordinator__.clients import (
    prepare_memtier_benchmark_parameters,
)
from redis_benchmarks_specification.__self_contained_coordinator__.runners import (
    build_runners_consumer_group_create,
)
from redis_benchmarks_specification.__setups__.topologies import get_topologies
from utils.tests.test_data.api_builder_common import flow_1_and_2_api_builder_checks


def test_self_contained_coordinator_blocking_read():
    try:
        if run_coordinator_tests():
            db_port = int(os.getenv("DATASINK_PORT", "6379"))
            conn = redis.StrictRedis(port=db_port)
            conn.ping()
            expected_datapoint_ts = None
            conn.flushall()
            build_variant_name, reply_fields = flow_1_and_2_api_builder_checks(conn)
            if b"git_timestamp_ms" in reply_fields:
                expected_datapoint_ts = int(reply_fields[b"git_timestamp_ms"].decode())
            if "git_timestamp_ms" in reply_fields:
                expected_datapoint_ts = int(reply_fields["git_timestamp_ms"])

            assert conn.exists(STREAM_KEYNAME_NEW_BUILD_EVENTS)
            assert conn.xlen(STREAM_KEYNAME_NEW_BUILD_EVENTS) > 0
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
            tf_github_org = "redis"
            tf_github_repo = "redis"
            deployment_name = "oss-standalone"
            deployment_type = "oss-standalone"
            use_metric_context_path = False
            metric_context_path = None
            for metric_name in ["ALL_STATS.Totals.Latency", "ALL_STATS.Totals.Ops/sec"]:
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
                assert len(rts.range(ts_key_name, 0, "+")) == 1
                if expected_datapoint_ts is not None:
                    assert rts.range(ts_key_name, 0, "+")[0][0] == expected_datapoint_ts
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

    except redis.exceptions.ConnectionError:
        pass


def run_coordinator_tests():
    run_coordinator = True
    TST_RUNNER_X = os.getenv("TST_RUNNER_X", "0")
    if TST_RUNNER_X == "0":
        run_coordinator = False
    return run_coordinator


def run_coordinator_tests_dockerhub():
    run_coordinator = True
    TST_RUNNER_X = os.getenv("TST_RUNNER_DOCKERHUB_X", "1")
    if TST_RUNNER_X == "0":
        run_coordinator = False
    return run_coordinator


def test_self_contained_coordinator_dockerhub_preload():
    try:
        if run_coordinator_tests_dockerhub():
            db_port = int(os.getenv("DATASINK_PORT", "6379"))
            conn = redis.StrictRedis(port=db_port)
            conn.ping()
            conn.flushall()

            id = "dockerhub"
            redis_version = "7.4.0"
            run_image = f"redis:{redis_version}"
            build_arch = "amd64"
            testDetails = {}
            build_os = "test_build_os"
            build_stream_fields, result = generate_benchmark_stream_request(
                id,
                conn,
                run_image,
                build_arch,
                testDetails,
                build_os,
                existing_artifact_keys=None,
                git_version=redis_version,
            )
            build_stream_fields["mnt_point"] = ""
            if result is True:
                arch_specific_stream = get_arch_specific_stream_name(build_arch)
                benchmark_stream_id = conn.xadd(
                    arch_specific_stream, build_stream_fields
                )
                logging.info(
                    "sucessfully requested a new run {}. Stream id: {}".format(
                        build_stream_fields, benchmark_stream_id
                    )
                )

            build_variant_name = "gcc:15.2.0-amd64-debian-bookworm-default"
            expected_datapoint_ts = None

            arch_specific_stream = get_arch_specific_stream_name(build_arch)
            assert conn.exists(arch_specific_stream)
            assert conn.xlen(arch_specific_stream) > 0
            running_platform = "fco-ThinkPad-T490"

            build_runners_consumer_group_create(
                conn, running_platform, arch=build_arch, id="0"
            )
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
                "./utils/tests/test_data/test-suites/generic-touch.yml"
            ]
            defaults_filename = "./utils/tests/test_data/test-suites/defaults.yml"
            (
                _,
                _,
                default_metrics,
                _,
                _,
                _,
            ) = get_defaults(defaults_filename)

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
                "",
                0,
                6399,
                1,
                False,
                5,
                default_metrics,
                "amd64",
                None,
                0,
                10000,
                "unstable",
                "",
                True,
                False,
            )

            assert result == True
            assert number_processed_streams == 1
            assert num_process_test_suites == 1
            by_version_key = f"ci.benchmarks.redis/ci/redis/redis/memtier_benchmark-1Mkeys-generic-touch-pipeline-10/by.version/{redis_version}/benchmark_end/{running_platform}/oss-standalone/memory_maxmemory"
            assert datasink_conn.exists(by_version_key)
            rts = datasink_conn.ts()
            # check we have by version metrics
            assert "version" in rts.info(by_version_key).labels
            assert redis_version == rts.info(by_version_key).labels["version"]

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
                # assert "version" in rts.info(hash_key).labels
                assert "hash" in rts.info(hash_key).labels
            #  assert redis_version == rts.info(hash_key).labels["version"]

    except redis.exceptions.ConnectionError:
        pass


def test_self_contained_coordinator_dockerhub():
    try:
        if run_coordinator_tests_dockerhub():
            db_port = int(os.getenv("DATASINK_PORT", "6379"))
            conn = redis.StrictRedis(port=db_port)
            conn.ping()
            conn.flushall()

            id = "dockerhub"
            redis_version = "7.4.0"
            run_image = f"redis:{redis_version}"
            build_arch = "amd64"
            testDetails = {}
            build_os = "test_build_os"
            build_stream_fields, result = generate_benchmark_stream_request(
                id,
                conn,
                run_image,
                build_arch,
                testDetails,
                build_os,
                git_version=redis_version,
            )
            build_stream_fields["mnt_point"] = ""
            if result is True:
                arch_specific_stream = get_arch_specific_stream_name(build_arch)
                benchmark_stream_id = conn.xadd(
                    arch_specific_stream, build_stream_fields
                )
                logging.info(
                    "sucessfully requested a new run {}. Stream id: {}".format(
                        build_stream_fields, benchmark_stream_id
                    )
                )

            build_variant_name = "gcc:15.2.0-amd64-debian-bookworm-default"
            expected_datapoint_ts = None

            arch_specific_stream = get_arch_specific_stream_name(build_arch)
            assert conn.exists(arch_specific_stream)
            assert conn.xlen(arch_specific_stream) > 0
            running_platform = "fco-ThinkPad-T490"

            build_runners_consumer_group_create(
                conn, running_platform, arch=build_arch, id="0"
            )
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
                "./utils/tests/test_data/test-suites/test-memtier-dockerhub.yml"
            ]
            defaults_filename = "./utils/tests/test_data/test-suites/defaults.yml"
            (
                _,
                _,
                default_metrics,
                _,
                _,
                _,
            ) = get_defaults(defaults_filename)

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
                "",
                0,
                6399,
                1,
                False,
                5,
                default_metrics,
                "amd64",
                None,
                0,
                10000,
                "unstable",
                "",
                True,
                False,
            )

            assert result == True
            assert number_processed_streams == 1
            assert num_process_test_suites == 1
            by_version_key = f"ci.benchmarks.redis/ci/redis/redis/memtier_benchmark-1Mkeys-load-string-with-10B-values/by.version/{redis_version}/benchmark_end/{running_platform}/oss-standalone/memory_maxmemory"
            assert datasink_conn.exists(by_version_key)
            rts = datasink_conn.ts()
            # check we have by version metrics
            assert "version" in rts.info(by_version_key).labels
            assert redis_version == rts.info(by_version_key).labels["version"]

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
                assert "version" in rts.info(hash_key).labels
                assert "hash" in rts.info(hash_key).labels
                assert redis_version == rts.info(hash_key).labels["version"]

    except redis.exceptions.ConnectionError:
        pass


def test_self_contained_coordinator_dockerhub_iothreads():
    try:
        if run_coordinator_tests_dockerhub():
            db_port = int(os.getenv("DATASINK_PORT", "6379"))
            conn = redis.StrictRedis(port=db_port)
            conn.ping()
            conn.flushall()

            id = "dockerhub"
            redis_version = "7.4.0"
            run_image = f"redis:{redis_version}"
            build_arch = "amd64"
            testDetails = {}
            build_os = "test_build_os"
            build_stream_fields, result = generate_benchmark_stream_request(
                id,
                conn,
                run_image,
                build_arch,
                testDetails,
                build_os,
                git_version=redis_version,
            )
            build_stream_fields["mnt_point"] = ""
            if result is True:
                arch_specific_stream = get_arch_specific_stream_name(build_arch)
                benchmark_stream_id = conn.xadd(
                    arch_specific_stream, build_stream_fields
                )
                logging.info(
                    "sucessfully requested a new run {}. Stream id: {}".format(
                        build_stream_fields, benchmark_stream_id
                    )
                )

            build_variant_name = "gcc:15.2.0-amd64-debian-bookworm-default"
            expected_datapoint_ts = None

            arch_specific_stream = get_arch_specific_stream_name(build_arch)
            assert conn.exists(arch_specific_stream)
            assert conn.xlen(arch_specific_stream) > 0
            running_platform = "fco-ThinkPad-T490"

            build_runners_consumer_group_create(
                conn, running_platform, arch=build_arch, id="0"
            )
            datasink_conn = redis.StrictRedis(port=db_port)
            docker_client = docker.from_env()
            home = str(Path.home())
            stream_id = ">"
            topologies_map = get_topologies(
                "./utils/tests/test_data/test-suites/topologies.yml"
            )
            # we use a benchmark spec with smaller CPU limit for client given github machines only contain 2 cores
            # and we need 1 core for DB and another for CLIENT
            testsuite_spec_files = [
                "./utils/tests/test_data/test-suites/test-memtier-dockerhub-iothreads.yml"
            ]
            defaults_filename = "./utils/tests/test_data/test-suites/defaults.yml"
            (
                _,
                _,
                default_metrics,
                _,
                _,
                _,
            ) = get_defaults(defaults_filename)

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
                "",
                0,
                6399,
                1,
                False,
                5,
                default_metrics,
                "amd64",
                None,
                0,
                10000,
                "unstable",
                "",
                True,
                False,
            )

            assert result == True
            assert number_processed_streams == 1
            assert num_process_test_suites == 1
            by_version_key = f"ci.benchmarks.redis/ci/redis/redis/memtier_benchmark-1Mkeys-load-string-with-10B-values/by.version/{redis_version}/benchmark_end/{running_platform}/oss-standalone-02-io-threads/memory_maxmemory"
            assert datasink_conn.exists(by_version_key)
            rts = datasink_conn.ts()
            # check we have by version metrics
            assert "version" in rts.info(by_version_key).labels
            assert redis_version == rts.info(by_version_key).labels["version"]

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
                assert "version" in rts.info(hash_key).labels
                assert "oss-standalone-02-io-threads" in hash_key.decode()
                assert "hash" in rts.info(hash_key).labels
                assert redis_version == rts.info(hash_key).labels["version"]

    except redis.exceptions.ConnectionError:
        pass


def test_self_contained_coordinator_dockerhub_valkey():
    try:
        if run_coordinator_tests_dockerhub():
            db_port = int(os.getenv("DATASINK_PORT", "6379"))
            conn = redis.StrictRedis(port=db_port)
            conn.ping()
            conn.flushall()

            id = "dockerhub"
            redis_version = "7.2.6"
            run_image = f"valkey/valkey:{redis_version}-bookworm"
            github_org = "valkey"
            github_repo = "valkey"
            build_arch = "amd64"
            testDetails = {}
            build_os = "test_build_os"
            build_stream_fields, result = generate_benchmark_stream_request(
                id,
                conn,
                run_image,
                build_arch,
                testDetails,
                build_os,
                [],
                "sudo bash -c 'make -j'",
                git_version=redis_version,
            )
            build_stream_fields["github_repo"] = github_repo
            build_stream_fields["github_org"] = github_org
            build_stream_fields["server_name"] = github_repo
            build_stream_fields["mnt_point"] = ""
            logging.info(
                f"requesting stream with following info: {build_stream_fields}"
            )
            if result is True:
                arch_specific_stream = get_arch_specific_stream_name(build_arch)
                benchmark_stream_id = conn.xadd(
                    arch_specific_stream, build_stream_fields
                )
                logging.info(
                    "sucessfully requested a new run {}. Stream id: {}".format(
                        build_stream_fields, benchmark_stream_id
                    )
                )

            arch_specific_stream = get_arch_specific_stream_name(build_arch)
            assert conn.exists(arch_specific_stream)
            assert conn.xlen(arch_specific_stream) > 0
            running_platform = "fco-ThinkPad-T490"

            build_runners_consumer_group_create(
                conn, running_platform, arch=build_arch, id="0"
            )
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
                "./utils/tests/test_data/test-suites/test-memtier-dockerhub.yml"
            ]
            defaults_filename = "./utils/tests/test_data/test-suites/defaults.yml"
            (
                _,
                _,
                default_metrics,
                _,
                _,
                _,
            ) = get_defaults(defaults_filename)

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
                "",
                0,
                6399,
                1,
                False,
                5,
                default_metrics,
                "amd64",
                None,
                0,
                10000,
                "unstable",
                "",
                True,
                False,
            )

            assert result == True
            assert number_processed_streams == 1
            assert num_process_test_suites == 1
            by_version_key = f"ci.benchmarks.redis/ci/{github_org}/{github_repo}/memtier_benchmark-1Mkeys-load-string-with-10B-values/by.version/{redis_version}/benchmark_end/{running_platform}/oss-standalone/memory_maxmemory"
            assert datasink_conn.exists(by_version_key)
            rts = datasink_conn.ts()
            # check we have by version metrics
            assert "version" in rts.info(by_version_key).labels
            assert redis_version == rts.info(by_version_key).labels["version"]

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
                assert "version" in rts.info(hash_key).labels
                assert "hash" in rts.info(hash_key).labels
                assert redis_version == rts.info(hash_key).labels["version"]

    except redis.exceptions.ConnectionError:
        pass


def test_dockerhub_via_cli():
    if run_coordinator_tests_dockerhub():
        import argparse

        db_port = int(os.getenv("DATASINK_PORT", "6379"))
        conn = redis.StrictRedis(port=db_port)
        conn.ping()
        conn.flushall()
        redis_version = "7.2.6"
        run_image = f"valkey/valkey:{redis_version}-bookworm"
        github_org = "valkey"
        github_repo = "valkey"

        db_port = os.getenv("DATASINK_PORT", "6379")

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
        ]
        args = parser.parse_args(
            args=run_args,
        )
        try:
            trigger_tests_dockerhub_cli_command_logic(args, "tool", "v0")
        except SystemExit as e:
            assert e.code == 0

        # confirm request was made via the cli
        # CLI adds to arch-specific stream (defaults to amd64)
        build_arch = "amd64"
        arch_specific_stream = get_arch_specific_stream_name(build_arch)
        assert conn.exists(arch_specific_stream)
        assert conn.xlen(arch_specific_stream) > 0
        running_platform = "fco-ThinkPad-T490"

        build_runners_consumer_group_create(
            conn, running_platform, arch=build_arch, id="0"
        )
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
            "./utils/tests/test_data/test-suites/test-memtier-dockerhub.yml"
        ]
        defaults_filename = "./utils/tests/test_data/test-suites/defaults.yml"
        (
            _,
            _,
            default_metrics,
            _,
            _,
            _,
        ) = get_defaults(defaults_filename)

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
            "",
            0,
            6399,
            1,
            False,
            5,
            default_metrics,
            "amd64",
            None,
            0,
            10000,
            "unstable",
            "",
            True,
            False,
        )

        assert result == True
        assert number_processed_streams == 1
        assert num_process_test_suites == 1
        by_version_key = f"ci.benchmarks.redis/ci/{github_org}/{github_repo}/memtier_benchmark-1Mkeys-load-string-with-10B-values/by.version/{redis_version}/benchmark_end/{running_platform}/oss-standalone/memory_maxmemory"
        assert datasink_conn.exists(by_version_key)
        rts = datasink_conn.ts()
        # check we have by version metrics
        assert "version" in rts.info(by_version_key).labels
        assert redis_version == rts.info(by_version_key).labels["version"]

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
            assert "version" in rts.info(hash_key).labels
            assert "hash" in rts.info(hash_key).labels
            assert redis_version == rts.info(hash_key).labels["version"]


def test_dockerhub_via_cli_airgap():
    if run_coordinator_tests_dockerhub():
        import argparse

        db_port = int(os.getenv("DATASINK_PORT", "6379"))
        conn = redis.StrictRedis(port=db_port)
        conn.ping()
        conn.flushall()
        redis_version = "7.2.6"
        run_image = f"valkey/valkey:{redis_version}-bookworm"
        github_org = "valkey"
        github_repo = "valkey"

        db_port = os.getenv("DATASINK_PORT", "6379")

        # should error due to missing --use-tags or --use-branch
        parser = argparse.ArgumentParser(
            description="test",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser = spec_cli_args(parser)
        run_args = [
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
        ]
        args = parser.parse_args(
            args=run_args,
        )
        try:
            trigger_tests_dockerhub_cli_command_logic(args, "tool", "v0")
        except SystemExit as e:
            assert e.code == 0

        # confirm request was made via the cli
        # CLI adds to arch-specific stream (defaults to amd64)
        build_arch = "amd64"
        arch_specific_stream = get_arch_specific_stream_name(build_arch)
        assert conn.exists(arch_specific_stream)
        assert conn.xlen(arch_specific_stream) > 0
        running_platform = "fco-ThinkPad-T490"

        build_runners_consumer_group_create(
            conn, running_platform, arch=build_arch, id="0"
        )
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
            "./utils/tests/test_data/test-suites/test-memtier-dockerhub.yml"
        ]
        defaults_filename = "./utils/tests/test_data/test-suites/defaults.yml"
        (
            _,
            _,
            default_metrics,
            _,
            _,
            _,
        ) = get_defaults(defaults_filename)

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
            "",
            0,
            6399,
            1,
            True,
            5,
            default_metrics,
            "amd64",
            None,
            0,
            10000,
            "unstable",
            "",
            True,
            False,
        )

        assert result == True
        assert number_processed_streams == 1
        assert num_process_test_suites == 1
        by_version_key = f"ci.benchmarks.redis/ci/{github_org}/{github_repo}/memtier_benchmark-1Mkeys-load-string-with-10B-values/by.version/{redis_version}/benchmark_end/{running_platform}/oss-standalone/memory_maxmemory"
        assert datasink_conn.exists(by_version_key)
        rts = datasink_conn.ts()
        # check we have by version metrics
        key_labels = rts.info(by_version_key).labels
        logging.info(key_labels)
        assert "version" in key_labels
        assert redis_version == rts.info(by_version_key).labels["version"]

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
            # assert "version" in rts.info(hash_key).labels
            assert "hash" in rts.info(hash_key).labels
            assert redis_version == rts.info(hash_key).labels["version"]


def test_self_contained_coordinator_skip_build_variant():
    try:
        if run_coordinator_tests():
            db_port = int(os.getenv("DATASINK_PORT", "6379"))
            conn = redis.StrictRedis(port=db_port)
            conn.ping()
            build_variant_name = "gcc:15.2.0-amd64-debian-bookworm-default"
            expected_datapoint_ts = None
            conn.flushall()
            build_variant_name, reply_fields = flow_1_and_2_api_builder_checks(conn)
            if b"git_timestamp_ms" in reply_fields:
                expected_datapoint_ts = int(reply_fields[b"git_timestamp_ms"].decode())
            if "git_timestamp_ms" in reply_fields:
                expected_datapoint_ts = int(reply_fields["git_timestamp_ms"])

            assert conn.exists(STREAM_KEYNAME_NEW_BUILD_EVENTS)
            assert conn.xlen(STREAM_KEYNAME_NEW_BUILD_EVENTS) > 0
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
        (
            _,
            benchmark_command_str,
        ) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            local_benchmark_output_filename,
            oss_api_enabled,
        )
        assert (
            benchmark_command_str
            == 'memtier_benchmark --json-out-file 1.json --port 12000 --server localhost "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 120 --key-minimum 1 --key-maximum 1000000'
        )
        oss_api_enabled = True
        (
            _,
            benchmark_command_str,
        ) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            local_benchmark_output_filename,
            oss_api_enabled,
        )
        assert (
            benchmark_command_str
            == 'memtier_benchmark --json-out-file 1.json --port 12000 --server localhost --cluster-mode "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 120 --key-minimum 1 --key-maximum 1000000'
        )


def test_self_contained_coordinator_blocking_read_valkey():
    try:
        if run_coordinator_tests():
            db_port = int(os.getenv("DATASINK_PORT", "6379"))
            conn = redis.StrictRedis(port=db_port)
            conn.ping()
            expected_datapoint_ts = None
            conn.flushall()
            gh_org = "valkey-io"
            gh_repo = "valkey"
            build_spec_name = "gcc:15.2.0-amd64-debian-bookworm-default"
            git_hash = "7795152fff06f8200f5e4239ff612b240f638e14"
            git_branch = "unstable"
            build_artifacts = ["valkey-server"]

            build_variant_name, reply_fields = flow_1_and_2_api_builder_checks(
                conn,
                build_spec_name,
                gh_org,
                gh_repo,
                git_hash,
                git_branch,
                "sh -c 'make -j'",
                build_artifacts,
                "valkey",
            )
            if b"git_timestamp_ms" in reply_fields:
                expected_datapoint_ts = int(reply_fields[b"git_timestamp_ms"].decode())
            if "git_timestamp_ms" in reply_fields:
                expected_datapoint_ts = int(reply_fields["git_timestamp_ms"])

            assert conn.exists(STREAM_KEYNAME_NEW_BUILD_EVENTS)
            assert conn.xlen(STREAM_KEYNAME_NEW_BUILD_EVENTS) > 0
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
                if expected_datapoint_ts is not None:
                    assert rts.range(ts_key_name, 0, "+")[0][0] == expected_datapoint_ts
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

    except redis.exceptions.ConnectionError:
        pass


def test_self_contained_coordinator_duplicated_ts():
    try:
        if run_coordinator_tests_dockerhub():
            db_port = int(os.getenv("DATASINK_PORT", "6379"))
            conn = redis.StrictRedis(port=db_port)
            conn.ping()
            conn.flushall()

            id = "dockerhub"
            redis_version = "7.4.0"
            run_image = f"redis:{redis_version}"
            build_arch = "amd64"
            testDetails = {}
            build_os = "test_build_os"

            # generate 2 stream requests with the same timestamp
            timestamp = int(datetime.datetime.now().timestamp())
            arch_specific_stream = get_arch_specific_stream_name(build_arch)
            for _ in range(0, 2):
                build_stream_fields, result = generate_benchmark_stream_request(
                    id,
                    conn,
                    run_image,
                    build_arch,
                    testDetails,
                    build_os,
                    git_timestamp_ms=timestamp,
                    use_git_timestamp=True,
                    existing_artifact_keys=None,
                    git_version=redis_version,
                )
                build_stream_fields["mnt_point"] = ""
                if result is True:
                    benchmark_stream_id = conn.xadd(
                        arch_specific_stream, build_stream_fields
                    )
                    logging.info(
                        "sucessfully requested a new run {}. Stream id: {}".format(
                            build_stream_fields, benchmark_stream_id
                        )
                    )

            assert conn.exists(arch_specific_stream)
            assert conn.xlen(arch_specific_stream) == 2

            running_platform = "fco-ThinkPad-T490"

            # process the 2 stream requests
            for _ in range(0, 2):

                build_runners_consumer_group_create(
                    conn, running_platform, arch=build_arch, id="0"
                )
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
                    "./utils/tests/test_data/test-suites/test-memtier-dockerhub.yml"
                ]
                defaults_filename = "./utils/tests/test_data/test-suites/defaults.yml"
                (
                    _,
                    _,
                    default_metrics,
                    _,
                    _,
                    _,
                ) = get_defaults(defaults_filename)

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
                    "",
                    0,
                    6399,
                    1,
                    False,
                    5,
                    default_metrics,
                    "amd64",
                    None,
                    0,
                    10000,
                    "unstable",
                    "",
                    True,
                    False,
                )
                assert result == True
                assert number_processed_streams == 1
                assert num_process_test_suites == 1

            stat_key = f"ci.benchmarks.redis/by.version/ci/redis/redis/memtier_benchmark-1Mkeys-load-string-with-10B-values/dockerhub/{running_platform}/oss-standalone/{redis_version}/ALL_STATS.Totals.Ops/sec"
            assert datasink_conn.exists(stat_key)
            rts = datasink_conn.ts()

            rts_info = rts.info(stat_key)

            # we have two datapoints
            assert rts_info.total_samples == 2

            # first was inserted on the original timestamp
            assert rts_info.first_timestamp == timestamp

            # the second has clashed, so it was resolved by adding 1ms to the timestamp
            assert rts_info.last_timestamp == timestamp + 1

    except redis.exceptions.ConnectionError:
        pass
