import argparse

import redis
import yaml

from redis_benchmarks_specification.__common__.package import get_version_string
from redis_benchmarks_specification.__common__.runner import extract_testsuites
from redis_benchmarks_specification.__common__.spec import extract_client_tool
from redis_benchmarks_specification.__runner__.args import create_client_runner_args
from redis_benchmarks_specification.__runner__.runner import (
    prepare_memtier_benchmark_parameters,
    run_client_runner_logic,
)


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
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )
        (_, benchmark_command_str,) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            local_benchmark_output_filename,
            oss_api_enabled,
            False,
            False,
            None,
            None,
            None,
            None,
            5,
        )
        assert (
            benchmark_command_str
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time=5'
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
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json --cluster-mode "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )

        oss_api_enabled = False
        tls_enabled = False
        tls_skip_verify = True
        tls_cert = None
        tls_key = None

        # ensure that when tls is disabled we dont change the args
        (_, benchmark_command_str,) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            local_benchmark_output_filename,
            oss_api_enabled,
            tls_enabled,
            tls_skip_verify,
            tls_cert,
            tls_key,
        )
        assert (
            benchmark_command_str
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )

        tls_enabled = True
        (_, benchmark_command_str,) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            local_benchmark_output_filename,
            oss_api_enabled,
            tls_enabled,
            tls_skip_verify,
            tls_cert,
            tls_key,
        )
        assert (
            benchmark_command_str
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json --tls --tls-skip-verify "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )

        tls_skip_verify = False
        (_, benchmark_command_str,) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            local_benchmark_output_filename,
            oss_api_enabled,
            tls_enabled,
            tls_skip_verify,
            tls_cert,
            tls_key,
        )
        assert (
            benchmark_command_str
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json --tls "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )

        tls_skip_verify = False
        tls_cert = "cert.file"
        tls_key = "key.file"
        (_, benchmark_command_str,) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            local_benchmark_output_filename,
            oss_api_enabled,
            tls_enabled,
            tls_skip_verify,
            tls_cert,
            tls_key,
        )
        assert (
            benchmark_command_str
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json --tls --cert cert.file --key key.file "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )

        tls_cacert = "cacert.file"
        (_, benchmark_command_str,) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            local_benchmark_output_filename,
            oss_api_enabled,
            tls_enabled,
            tls_skip_verify,
            tls_cert,
            tls_key,
            tls_cacert,
        )
        assert (
            benchmark_command_str
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json --tls --cert cert.file --key key.file --cacert cacert.file "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )


def test_run_client_runner_logic():
    project_name = "tool"
    project_version = "v0"
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_client_runner_args(
        get_version_string(project_name, project_version)
    )
    db_host = "localhost"
    db_port = "6380"
    datasink_port = "16379"
    db_port_int = int(db_port)
    datasink_port_int = int(db_port)
    args = parser.parse_args(
        args=[
            "--test",
            "../../utils/tests/test_data/test-suites/memtier_benchmark-2keys-stream-5-entries-xread-all-entries.yml",
            "--db_server_host",
            "{}".format(db_host),
            "--db_server_port",
            "{}".format(db_port),
            "--flushall_on_every_test_start",
        ]
    )
    try:
        run_client_runner_logic(args, "tool", "", "v0")
    except SystemExit as e:
        assert e.code == 0

    r = redis.Redis(host=db_host, port=db_port_int)
    total_keys = r.info("keyspace")["db0"]["keys"]
    assert total_keys == 2

    # run while pushing to redistimeseries
    args = parser.parse_args(
        args=[
            "--test",
            "../../utils/tests/test_data/test-suites/memtier_benchmark-2keys-stream-5-entries-xread-all-entries.yml",
            "--datasink_push_results_redistimeseries",
            "--datasink_redistimeseries_host",
            "{}".format(db_host),
            "--datasink_redistimeseries_port",
            "{}".format(datasink_port),
            "--db_server_host",
            "{}".format(db_host),
            "--db_server_port",
            "{}".format(db_port),
            "--flushall_on_every_test_start",
        ]
    )
    try:
        run_client_runner_logic(args, "tool", "", "v0")
    except SystemExit as e:
        assert e.code == 0

    r = redis.Redis(host=db_host, port=db_port_int)
    total_keys = r.info("keyspace")["db0"]["keys"]
    assert total_keys == 2
    rts = redis.Redis(host=db_host, port=db_port_int)
    total_keys = rts.info("keyspace")["db0"]["keys"]
    assert total_keys > 0


def test_extract_testsuites():
    project_name = "tool"
    project_version = "v0"
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_client_runner_args(
        get_version_string(project_name, project_version)
    )
    args = parser.parse_args(
        args=[
            "--test-suites-folder",
            "./utils/tests/test_data/test-suites",
        ]
    )
    tests = extract_testsuites(args)
    assert len(tests) == 4

    args = parser.parse_args(
        args=[
            "--test-suites-folder",
            "./utils/tests/test_data/test-suites",
            "--tests-regex",
            ".*\.yml",
        ]
    )
    tests = extract_testsuites(args)
    assert len(tests) == 4

    args = parser.parse_args(
        args=[
            "--test-suites-folder",
            "./utils/tests/test_data/test-suites",
            "--tests-regex",
            ".*expire.*",
        ]
    )
    tests = extract_testsuites(args)
    assert len(tests) == 2
