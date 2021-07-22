from redis_benchmarks_specification.builder.schema import get_build_config

def test_get_build_config():
    config_files = [
        "./tests/test_data/icc-2021.3.0-amd64-ubuntu18.04-libc.yml",
    ]
    for filename in config_files:
        get_build_config(filename)

