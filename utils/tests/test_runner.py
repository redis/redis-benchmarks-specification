import yaml

from redis_benchmarks_specification.__common__.spec import extract_client_tool
from redis_benchmarks_specification.__runner__.runner import (
    prepare_memtier_benchmark_parameters,
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
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json "--data-size" "100" --command "SETEX __key__ 10 __value__" --command-key-pattern="R" --command "SET __key__ __value__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
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
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json --tls --tls-skip-verify "--data-size" "100" --command "SETEX __key__ 10 __value__" --command-key-pattern="R" --command "SET __key__ __value__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
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
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json --tls "--data-size" "100" --command "SETEX __key__ 10 __value__" --command-key-pattern="R" --command "SET __key__ __value__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
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
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json --tls --cert cert.file --key key.file "--data-size" "100" --command "SETEX __key__ 10 __value__" --command-key-pattern="R" --command "SET __key__ __value__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
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
            == 'memtier_benchmark --port 12000 --server localhost --json-out-file 1.json --tls --cert cert.file --key key.file --cacert cacert.file "--data-size" "100" --command "SETEX __key__ 10 __value__" --command-key-pattern="R" --command "SET __key__ __value__" --command-key-pattern="R" --command "GET __key__" --command-key-pattern="R" --command "DEL __key__" --command-key-pattern="R"  -c 50 -t 2 --hide-histogram --test-time 300'
        )
