from redis_benchmarks_specification.__builder__.schema import get_build_config


def test_get_build_config():
    config_files = [
        "./utils/tests/test_data/icc-2021.3.0-amd64-ubuntu18.04-monotonic-clock.yml",
    ]
    for filename in config_files:
        build_config, id = get_build_config(filename)
        assert id == "icc-2021.3.0-amd64-ubuntu18.04-monotonic-clock"
        assert build_config["env"]["CC"] == "icc"
