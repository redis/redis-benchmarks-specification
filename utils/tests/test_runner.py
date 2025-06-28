import argparse
import os

import redis
import yaml

from redis_benchmarks_specification.__common__.package import get_version_string
from redis_benchmarks_specification.__common__.runner import extract_testsuites
from redis_benchmarks_specification.__common__.spec import (
    extract_client_tool,
    extract_client_configs,
    extract_client_container_images,
    extract_client_tools,
    extract_client_cpu_limit,
    extract_client_container_image,
)
from redis_benchmarks_specification.__runner__.args import create_client_runner_args
from redis_benchmarks_specification.__runner__.runner import (
    prepare_memtier_benchmark_parameters,
    run_client_runner_logic,
    parse_size,
    run_multiple_clients,
    prepare_pubsub_sub_bench_parameters,
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
        (_, benchmark_command_str, _) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            None,
            local_benchmark_output_filename,
            oss_api_enabled,
        )
        assert (
            benchmark_command_str
            == 'memtier_benchmark --json-out-file 1.json --port 12000 --server localhost "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )
        (_, benchmark_command_str, _) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            None,
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
            == 'memtier_benchmark --json-out-file 1.json --port 12000 --server localhost "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time=5'
        )
        oss_api_enabled = True
        (_, benchmark_command_str, _) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            None,
            local_benchmark_output_filename,
            oss_api_enabled,
        )
        assert (
            benchmark_command_str
            == 'memtier_benchmark --json-out-file 1.json --port 12000 --server localhost --cluster-mode "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )

        oss_api_enabled = False
        tls_enabled = False
        tls_skip_verify = True
        tls_cert = None
        tls_key = None

        # ensure that when tls is disabled we dont change the args
        (_, benchmark_command_str, _) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            None,
            local_benchmark_output_filename,
            oss_api_enabled,
            tls_enabled,
            tls_skip_verify,
            tls_cert,
            tls_key,
        )
        assert (
            benchmark_command_str
            == 'memtier_benchmark --json-out-file 1.json --port 12000 --server localhost "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )

        tls_enabled = True
        (_, benchmark_command_str, _) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            None,
            local_benchmark_output_filename,
            oss_api_enabled,
            tls_enabled,
            tls_skip_verify,
            tls_cert,
            tls_key,
        )
        assert (
            benchmark_command_str
            == 'memtier_benchmark --json-out-file 1.json --port 12000 --server localhost --tls --tls-skip-verify "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )

        tls_skip_verify = False
        (_, benchmark_command_str, _) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            None,
            local_benchmark_output_filename,
            oss_api_enabled,
            tls_enabled,
            tls_skip_verify,
            tls_cert,
            tls_key,
        )
        assert (
            benchmark_command_str
            == 'memtier_benchmark --json-out-file 1.json --port 12000 --server localhost --tls "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )

        tls_skip_verify = False
        tls_cert = "cert.file"
        tls_key = "key.file"
        (_, benchmark_command_str, _) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            None,
            local_benchmark_output_filename,
            oss_api_enabled,
            tls_enabled,
            tls_skip_verify,
            tls_cert,
            tls_key,
        )
        assert (
            benchmark_command_str
            == 'memtier_benchmark --json-out-file 1.json --port 12000 --server localhost --tls --cert cert.file --key key.file "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )

        tls_cacert = "cacert.file"
        (_, benchmark_command_str, _) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            None,
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
            == 'memtier_benchmark --json-out-file 1.json --port 12000 --server localhost --tls --cert cert.file --key key.file --cacert cacert.file "--data-size" "100" --command "SETEX __key__ 10 __data__" --command-key-pattern="R" --command "SET __key__ __data__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )


def test_extract_client_configs():
    # Test single clientconfig format
    with open(
        "./redis_benchmarks_specification/test-suites/memtier_benchmark-1Mkeys-100B-expire-use-case.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        client_configs = extract_client_configs(benchmark_config)
        assert len(client_configs) == 1
        assert "tool" in client_configs[0]
        assert client_configs[0]["tool"] == "memtier_benchmark"

    # Test multiple clientconfigs format (create a test config)
    test_config = {
        "clientconfigs": [
            {
                "run_image": "redislabs/memtier_benchmark:edge",
                "tool": "memtier_benchmark",
                "arguments": '--command="ZSCAN zset:100 0" --hide-histogram --test-time 120',
                "resources": {"requests": {"cpus": "4", "memory": "2g"}},
            },
            {
                "run_image": "redislabs/memtier_benchmark:edge",
                "tool": "memtier_benchmark",
                "arguments": '--command="ZRANGE zset:100 0 -1" --hide-histogram --test-time 120',
                "resources": {"requests": {"cpus": "2", "memory": "1g"}},
            },
        ]
    }
    client_configs = extract_client_configs(test_config)
    assert len(client_configs) == 2
    assert client_configs[0]["tool"] == "memtier_benchmark"
    assert client_configs[1]["tool"] == "memtier_benchmark"


def test_extract_client_container_images():
    # Test single clientconfig format
    with open(
        "./redis_benchmarks_specification/test-suites/memtier_benchmark-1Mkeys-100B-expire-use-case.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        images = extract_client_container_images(benchmark_config)
        assert len(images) == 1
        assert "redislabs/memtier_benchmark" in images[0]

    # Test multiple clientconfigs format
    test_config = {
        "clientconfigs": [
            {
                "run_image": "redislabs/memtier_benchmark:edge",
                "tool": "memtier_benchmark",
                "arguments": '--command="ZSCAN zset:100 0"',
                "resources": {"requests": {"cpus": "4", "memory": "2g"}},
            },
            {
                "run_image": "redislabs/memtier_benchmark:latest",
                "tool": "memtier_benchmark",
                "arguments": '--command="ZRANGE zset:100 0 -1"',
                "resources": {"requests": {"cpus": "2", "memory": "1g"}},
            },
        ]
    }
    images = extract_client_container_images(test_config)
    assert len(images) == 2
    assert images[0] == "redislabs/memtier_benchmark:edge"
    assert images[1] == "redislabs/memtier_benchmark:latest"


def test_extract_client_tools():
    # Test single clientconfig format
    with open(
        "./redis_benchmarks_specification/test-suites/memtier_benchmark-1Mkeys-100B-expire-use-case.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        tools = extract_client_tools(benchmark_config)
        assert len(tools) == 1
        assert tools[0] == "memtier_benchmark"

    # Test multiple clientconfigs format
    test_config = {
        "clientconfigs": [
            {
                "run_image": "redislabs/memtier_benchmark:edge",
                "tool": "memtier_benchmark",
                "arguments": '--command="ZSCAN zset:100 0"',
                "resources": {"requests": {"cpus": "4", "memory": "2g"}},
            },
            {
                "run_image": "redislabs/redis-benchmark:latest",
                "tool": "redis-benchmark",
                "arguments": "-t set,get -n 1000",
                "resources": {"requests": {"cpus": "2", "memory": "1g"}},
            },
        ]
    }
    tools = extract_client_tools(test_config)
    assert len(tools) == 2
    assert tools[0] == "memtier_benchmark"
    assert tools[1] == "redis-benchmark"


def test_extract_client_cpu_limit():
    # Test single clientconfig format
    with open(
        "./redis_benchmarks_specification/test-suites/memtier_benchmark-1Mkeys-100B-expire-use-case.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        cpu_limit = extract_client_cpu_limit(benchmark_config)
        assert cpu_limit >= 1  # Should be at least 1 CPU

    # Test multiple clientconfigs format - should sum CPU limits
    test_config = {
        "clientconfigs": [
            {
                "run_image": "redislabs/memtier_benchmark:edge",
                "tool": "memtier_benchmark",
                "arguments": '--command="ZSCAN zset:100 0"',
                "resources": {"requests": {"cpus": "2.5", "memory": "2g"}},
            },
            {
                "run_image": "redislabs/memtier_benchmark:latest",
                "tool": "memtier_benchmark",
                "arguments": '--command="ZRANGE zset:100 0 -1"',
                "resources": {"requests": {"cpus": "1.5", "memory": "1g"}},
            },
        ]
    }
    cpu_limit = extract_client_cpu_limit(test_config)
    assert cpu_limit == 4  # ceil(2.5 + 1.5) = 4


def test_extract_client_configs_edge_cases():
    # Test empty config
    empty_config = {}
    client_configs = extract_client_configs(empty_config)
    assert len(client_configs) == 0

    # Test config with missing fields
    incomplete_config = {
        "clientconfigs": [
            {
                "run_image": "redislabs/memtier_benchmark:edge",
                # Missing tool and arguments
                "resources": {"requests": {"cpus": "2", "memory": "1g"}},
            }
        ]
    }
    client_configs = extract_client_configs(incomplete_config)
    assert len(client_configs) == 1
    assert "run_image" in client_configs[0]


def test_extract_client_container_images_edge_cases():
    # Test config without run_image
    config_no_image = {
        "clientconfig": {
            "tool": "memtier_benchmark",
            "arguments": '--command="SET key value"',
            "resources": {"requests": {"cpus": "2", "memory": "1g"}},
        }
    }
    images = extract_client_container_images(config_no_image)
    assert len(images) == 1
    assert images[0] is None

    # Test empty clientconfigs
    empty_configs = {"clientconfigs": []}
    images = extract_client_container_images(empty_configs)
    assert len(images) == 0


def test_extract_client_tools_edge_cases():
    # Test config without tool
    config_no_tool = {
        "clientconfig": {
            "run_image": "redislabs/memtier_benchmark:edge",
            "arguments": '--command="SET key value"',
            "resources": {"requests": {"cpus": "2", "memory": "1g"}},
        }
    }
    tools = extract_client_tools(config_no_tool)
    assert len(tools) == 1
    assert tools[0] is None


def test_prepare_memtier_benchmark_parameters_variations():
    """Test memtier benchmark parameter preparation with different configurations"""
    with open(
        "./redis_benchmarks_specification/test-suites/memtier_benchmark-1Mkeys-100B-expire-use-case.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        client_tool = extract_client_tool(benchmark_config)

        # Test with TLS enabled
        (_, benchmark_command_str, _) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            "password123",
            "test_output.json",
            False,  # oss_cluster_api_enabled
            True,  # tls_enabled
            False,  # tls_skip_verify
            "cert.pem",
            "key.pem",
            "ca.pem",
            "3",  # resp_version (should be string)
            0,  # override_memtier_test_time (use default)
            1,  # override_test_runs (use default)
            "",  # unix_socket (should be empty string, not None)
        )
        assert "--tls" in benchmark_command_str
        assert "--cert cert.pem" in benchmark_command_str
        assert "--key key.pem" in benchmark_command_str
        assert "--cacert ca.pem" in benchmark_command_str
        assert "--protocol resp3" in benchmark_command_str

        # Test with Unix socket
        (_, benchmark_command_str, _) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            None,
            "test_output.json",
            False,  # oss_cluster_api_enabled
            False,  # tls_enabled
            False,  # tls_skip_verify
            None,
            None,
            None,
            "2",  # resp_version
            120,  # override_memtier_test_time
            5,  # override_test_runs
            "/tmp/redis.sock",  # unix_socket
        )
        assert "--unix-socket /tmp/redis.sock" in benchmark_command_str
        assert "--test-time=120" in benchmark_command_str
        assert "--run-count=5" in benchmark_command_str

        # Test with password
        (_, benchmark_command_str, _) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            12000,
            "localhost",
            "mypassword",
            "test_output.json",
            False,  # oss_cluster_api_enabled
            False,  # tls_enabled
            False,  # tls_skip_verify
            None,
            None,
            None,
            "2",  # resp_version
            0,  # override_memtier_test_time (use default)
            1,  # override_test_runs (use default)
            "",  # unix_socket
        )
        assert "--authenticate mypassword" in benchmark_command_str


def test_parse_size():
    """Test the parse_size utility function"""
    # Test basic units
    assert parse_size("100") == 100
    assert parse_size("100B") == 100
    assert parse_size("1KB") == 1024
    assert parse_size("1MB") == 1024 * 1024
    assert parse_size("1GB") == 1024 * 1024 * 1024
    assert parse_size("2TB") == 2 * 1024 * 1024 * 1024 * 1024

    # Test decimal values
    assert parse_size("1.5KB") == int(1.5 * 1024)
    assert parse_size("2.5MB") == int(2.5 * 1024 * 1024)

    # Test short forms
    assert parse_size("1K") == 1024
    assert parse_size("1M") == 1024 * 1024
    assert parse_size("1G") == 1024 * 1024 * 1024

    # Test with spaces
    assert parse_size("1 KB") == 1024
    assert parse_size("2 MB") == 2 * 1024 * 1024

    # Test case insensitive
    assert parse_size("1kb") == 1024
    assert parse_size("1mb") == 1024 * 1024


def test_parse_size_edge_cases():
    """Test parse_size with edge cases and different formats"""
    # Test decimal numbers
    assert parse_size("0.5KB") == int(0.5 * 1024)
    assert parse_size("1.25MB") == int(1.25 * 1024 * 1024)

    # Test different unit formats
    assert parse_size("1KIB") == 1000  # Decimal units
    assert parse_size("1MIB") == 1000000
    assert parse_size("1GIB") == 1000000000

    # Test short forms
    assert parse_size("1KI") == 1000
    assert parse_size("1MI") == 1000000
    assert parse_size("1GI") == 1000000000
    assert parse_size("1TI") == 1000000000000

    # Test with extra whitespace
    assert parse_size("  1 KB  ") == 1024
    assert parse_size("\t2\tMB\t") == 2 * 1024 * 1024

    # Test string input
    assert parse_size(str(1024)) == 1024


def test_create_client_runner_args():
    """Test the argument parser creation"""
    version_string = "test-version-1.0"
    parser = create_client_runner_args(version_string)

    # Test that parser is created successfully
    assert parser is not None

    # Test parsing basic arguments
    args = parser.parse_args(
        [
            "--test",
            "test.yml",
            "--db_server_host",
            "localhost",
            "--db_server_port",
            "6379",
        ]
    )

    assert args.test == "test.yml"
    assert args.db_server_host == "localhost"
    assert args.db_server_port == 6379  # Port is parsed as integer

    # Test parsing with optional arguments
    args = parser.parse_args(
        [
            "--test",
            "test.yml",
            "--db_server_host",
            "localhost",
            "--db_server_port",
            "6379",
            "--flushall_on_every_test_start",
            "--benchmark_local_install",
        ]
    )

    assert args.flushall_on_every_test_start is True
    assert args.benchmark_local_install is True


def test_extract_client_container_image_legacy():
    """Test the legacy extract_client_container_image function"""
    # Test with run_image present
    config = {"clientconfig": {"run_image": "redis:latest", "tool": "redis-benchmark"}}
    image = extract_client_container_image(config)
    assert image == "redis:latest"

    # Test with missing run_image
    config_no_image = {"clientconfig": {"tool": "memtier_benchmark"}}
    image = extract_client_container_image(config_no_image)
    assert image is None

    # Test with missing clientconfig entirely
    empty_config = {}
    image = extract_client_container_image(empty_config)
    assert image is None

    # Test with custom keyname
    config_custom = {"myclient": {"run_image": "custom:image"}}
    image = extract_client_container_image(config_custom, keyname="myclient")
    assert image == "custom:image"


def test_extract_client_tool_legacy():
    """Test the legacy extract_client_tool function"""
    # Test with tool present
    config = {
        "clientconfig": {"tool": "memtier_benchmark", "run_image": "redis:latest"}
    }
    tool = extract_client_tool(config)
    assert tool == "memtier_benchmark"

    # Test with missing tool
    config_no_tool = {"clientconfig": {"run_image": "redis:latest"}}
    tool = extract_client_tool(config_no_tool)
    assert tool is None

    # Test with missing clientconfig entirely
    empty_config = {}
    tool = extract_client_tool(empty_config)
    assert tool is None

    # Test with custom keyname
    config_custom = {"myclient": {"tool": "redis-benchmark"}}
    tool = extract_client_tool(config_custom, keyname="myclient")
    assert tool == "redis-benchmark"


def test_run_multiple_clients_error_handling():
    """Test error handling in run_multiple_clients"""
    import pytest

    # Test with empty config (no client configurations)
    empty_config = {}

    # Mock the required parameters
    mock_args = type("MockArgs", (), {"benchmark_local_install": False})()

    with pytest.raises(ValueError, match="No client configurations found"):
        run_multiple_clients(
            empty_config,
            None,  # docker_client
            "/tmp",  # temporary_dir_client
            "/mnt",  # client_mnt_point
            "/workdir",  # benchmark_tool_workdir
            "0-3",  # client_cpuset_cpus
            6379,  # port
            "localhost",  # host
            None,  # password
            False,  # oss_cluster_api_enabled
            False,  # tls_enabled
            False,  # tls_skip_verify
            None,  # test_tls_cert
            None,  # test_tls_key
            None,  # test_tls_cacert
            "2",  # resp_version
            0,  # override_memtier_test_time
            1,  # override_test_runs
            "",  # unix_socket
            mock_args,  # args
        )


def test_create_client_runner_args_all_options():
    """Test argument parser with all possible options"""
    version_string = "test-version-1.0"
    parser = create_client_runner_args(version_string)

    # Test parsing with all optional arguments
    args = parser.parse_args(
        [
            "--test",
            "test.yml",
            "--db_server_host",
            "redis.example.com",
            "--db_server_port",
            "6380",
            "--flushall_on_every_test_start",
            "--benchmark_local_install",
            "--cluster-mode",
            "--unix-socket",
            "/tmp/redis.sock",
            "--override-memtier-test-time",
            "60",
            "--override-test-runs",
            "3",
        ]
    )

    assert args.test == "test.yml"
    assert args.db_server_host == "redis.example.com"
    assert args.db_server_port == 6380
    assert args.flushall_on_every_test_start is True
    assert args.benchmark_local_install is True
    assert args.cluster_mode is True
    assert args.unix_socket == "/tmp/redis.sock"
    assert args.override_memtier_test_time == 60
    assert args.override_test_runs == 3


def test_create_client_runner_args_defaults():
    """Test argument parser default values"""
    version_string = "test-version-1.0"
    parser = create_client_runner_args(version_string)

    # Test parsing with minimal required arguments
    args = parser.parse_args(
        [
            "--test",
            "test.yml",
            "--db_server_host",
            "localhost",
            "--db_server_port",
            "6379",
        ]
    )

    # Check default values
    assert args.flushall_on_every_test_start is False
    assert args.benchmark_local_install is False
    assert args.cluster_mode is False
    assert args.unix_socket == ""
    assert args.override_memtier_test_time == 0
    assert args.override_test_runs == 1


# Removed test_prepare_benchmark_parameters_redis_benchmark as it tests external functionality


def test_prepare_memtier_benchmark_parameters_resp_versions():
    """Test memtier benchmark with different RESP versions"""
    with open(
        "./redis_benchmarks_specification/test-suites/memtier_benchmark-1Mkeys-100B-expire-use-case.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        client_tool = extract_client_tool(benchmark_config)

        # Test RESP2 (default)
        (_, benchmark_command_str, _) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            6379,
            "localhost",
            None,
            "test_output.json",
            False,  # oss_cluster_api_enabled
            False,  # tls_enabled
            False,  # tls_skip_verify
            None,
            None,
            None,
            "2",  # resp_version
            0,
            1,
            "",
        )
        # RESP2 should not add any protocol flags
        assert "--protocol resp2" not in benchmark_command_str

        # Test RESP3
        (_, benchmark_command_str, _) = prepare_memtier_benchmark_parameters(
            benchmark_config["clientconfig"],
            client_tool,
            6379,
            "localhost",
            None,
            "test_output.json",
            False,  # oss_cluster_api_enabled
            False,  # tls_enabled
            False,  # tls_skip_verify
            None,
            None,
            None,
            "3",  # resp_version
            0,
            1,
            "",
        )
        assert "--protocol resp3" in benchmark_command_str


def test_prepare_pubsub_sub_bench_parameters():
    """Test pubsub-sub-bench parameter preparation"""
    # Create a test client config for pubsub-sub-bench
    client_config = {
        "tool": "pubsub-sub-bench",
        "arguments": "-clients 10 -messages 1000 -subscribers-per-channel 5",
        "run_image": "filipe958/pubsub-sub-bench:latest",
        "resources": {"requests": {"cpus": "2", "memory": "1g"}},
    }

    # Test basic parameter preparation
    (_, benchmark_command_str, _) = prepare_pubsub_sub_bench_parameters(
        client_config,
        "pubsub-sub-bench",
        6379,
        "localhost",
        None,  # password
        "test_output.json",
        False,  # oss_cluster_api_enabled
        False,  # tls_enabled
        False,  # tls_skip_verify
        None,
        None,
        None,  # TLS certs
        "2",  # resp_version
        0,  # override_test_time
        "",  # unix_socket
        None,  # username
    )

    # Verify basic parameters
    assert "pubsub-sub-bench" in benchmark_command_str
    assert "-json-out-file test_output.json" in benchmark_command_str
    assert "-host localhost" in benchmark_command_str
    assert "-port 6379" in benchmark_command_str
    assert "-resp 2" in benchmark_command_str

    # Verify user arguments are appended
    assert "-clients 10" in benchmark_command_str
    assert "-messages 1000" in benchmark_command_str
    assert "-subscribers-per-channel 5" in benchmark_command_str


def test_prepare_pubsub_sub_bench_parameters_with_auth():
    """Test pubsub-sub-bench with authentication"""
    client_config = {
        "tool": "pubsub-sub-bench",
        "arguments": "-test-time 60",
        "run_image": "filipe958/pubsub-sub-bench:latest",
    }

    # Test with password only
    (_, benchmark_command_str, _) = prepare_pubsub_sub_bench_parameters(
        client_config,
        "pubsub-sub-bench",
        6379,
        "redis.example.com",
        "mypassword",
        "output.json",
        False,
        False,
        False,
        None,
        None,
        None,
        "3",  # RESP3
        120,  # test_time override
        "",
        None,
    )

    assert "-host redis.example.com" in benchmark_command_str
    assert "-port 6379" in benchmark_command_str
    assert "-a mypassword" in benchmark_command_str
    assert "-resp 3" in benchmark_command_str
    assert "-test-time 120" in benchmark_command_str

    # Test with username and password (ACL style)
    (_, benchmark_command_str, _) = prepare_pubsub_sub_bench_parameters(
        client_config,
        "pubsub-sub-bench",
        6379,
        "redis.example.com",
        "mypassword",
        "output.json",
        False,
        False,
        False,
        None,
        None,
        None,
        "2",
        0,
        "",
        "myuser",  # username
    )

    assert "-user myuser" in benchmark_command_str
    assert "-a mypassword" in benchmark_command_str


def test_prepare_pubsub_sub_bench_parameters_cluster_mode():
    """Test pubsub-sub-bench with cluster mode"""
    client_config = {
        "tool": "pubsub-sub-bench",
        "arguments": "-channel-minimum 1 -channel-maximum 100",
        "run_image": "filipe958/pubsub-sub-bench:latest",
    }

    (_, benchmark_command_str, _) = prepare_pubsub_sub_bench_parameters(
        client_config,
        "pubsub-sub-bench",
        6379,
        "cluster.redis.com",
        None,
        "cluster_output.json",
        True,  # oss_cluster_api_enabled
        False,
        False,
        None,
        None,
        None,
        None,
        0,
        "",
        None,
    )

    assert "-oss-cluster-api-distribute-subscribers" in benchmark_command_str
    assert "-channel-minimum 1" in benchmark_command_str
    assert "-channel-maximum 100" in benchmark_command_str


def test_prepare_pubsub_sub_bench_parameters_unix_socket():
    """Test pubsub-sub-bench with unix socket (should fall back to host/port)"""
    client_config = {
        "tool": "pubsub-sub-bench",
        "arguments": "-verbose",
        "run_image": "filipe958/pubsub-sub-bench:latest",
    }

    (_, benchmark_command_str, _) = prepare_pubsub_sub_bench_parameters(
        client_config,
        "pubsub-sub-bench",
        6379,
        "localhost",
        None,
        "unix_output.json",
        False,
        False,
        False,
        None,
        None,
        None,
        None,
        0,
        "/tmp/redis.sock",  # unix_socket
        None,
    )

    # Should still use host/port since pubsub-sub-bench doesn't support unix sockets
    assert "-host localhost" in benchmark_command_str
    assert "-port 6379" in benchmark_command_str
    assert "-verbose" in benchmark_command_str


def test_extract_client_configs_pubsub_sub_bench():
    """Test client config extraction with pubsub-sub-bench tool"""
    # Test multiple pubsub-sub-bench configs
    test_config = {
        "clientconfigs": [
            {
                "run_image": "filipe958/pubsub-sub-bench:latest",
                "tool": "pubsub-sub-bench",
                "arguments": "-clients 5 -mode subscribe",
                "resources": {"requests": {"cpus": "1", "memory": "512m"}},
            },
            {
                "run_image": "filipe958/pubsub-sub-bench:edge",
                "tool": "pubsub-sub-bench",
                "arguments": "-clients 10 -mode ssubscribe",
                "resources": {"requests": {"cpus": "2", "memory": "1g"}},
            },
        ]
    }

    client_configs = extract_client_configs(test_config)
    client_tools = extract_client_tools(test_config)
    client_images = extract_client_container_images(test_config)

    assert len(client_configs) == 2
    assert len(client_tools) == 2
    assert len(client_images) == 2

    assert client_tools[0] == "pubsub-sub-bench"
    assert client_tools[1] == "pubsub-sub-bench"
    assert "subscribe" in client_configs[0]["arguments"]
    assert "ssubscribe" in client_configs[1]["arguments"]
    assert client_images[0] == "filipe958/pubsub-sub-bench:latest"
    assert client_images[1] == "filipe958/pubsub-sub-bench:edge"


def test_prepare_pubsub_sub_bench_parameters_override_test_time():
    """Test pubsub-sub-bench with test-time override"""
    client_config = {
        "tool": "pubsub-sub-bench",
        "arguments": "-clients 10 -test-time 60 -verbose",  # User specifies 60s
        "run_image": "filipe958/pubsub-sub-bench:latest",
    }

    # Test with override_test_time=30 (should override the user's 60s)
    (_, benchmark_command_str, _) = prepare_pubsub_sub_bench_parameters(
        client_config,
        "pubsub-sub-bench",
        6379,
        "localhost",
        None,
        "output.json",
        False,
        False,
        False,
        None,
        None,
        None,
        "2",
        30,  # override_test_time=30
        "",
        None,
    )

    # Should have our override time, not the user's time
    assert "-test-time 30" in benchmark_command_str
    assert "-test-time 60" not in benchmark_command_str  # User's time should be removed
    assert "-clients 10" in benchmark_command_str  # Other args should remain
    assert "-verbose" in benchmark_command_str


def test_create_client_runner_args_container_timeout_buffer():
    """Test that container timeout buffer argument is properly configured"""
    from redis_benchmarks_specification.__runner__.args import create_client_runner_args

    # Test default value
    parser = create_client_runner_args("test")
    args = parser.parse_args([])
    assert args.container_timeout_buffer == 60  # Default should be 60 seconds

    # Test custom value
    args = parser.parse_args(["--container-timeout-buffer", "120"])
    assert args.container_timeout_buffer == 120


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
    db_port = os.getenv("DATASINK_PORT", "6379")
    datasink_port = os.getenv("DATASINK_PORT", "6379")
    db_port_int = int(db_port)
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
    assert total_keys >= 2

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
    assert total_keys >= 2
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
    assert len(tests) == 9

    args = parser.parse_args(
        args=[
            "--test-suites-folder",
            "./utils/tests/test_data/test-suites",
            "--tests-regex",
            r".*\.yml",
        ]
    )
    tests = extract_testsuites(args)
    assert len(tests) == 9

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
