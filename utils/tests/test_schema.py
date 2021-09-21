from redis_benchmarks_specification.__builder__.schema import (
    get_build_config,
    get_build_config_metadata,
)


def test_get_build_config():
    config_files = [
        "./utils/tests/test_data/icc-2021.3.0-amd64-ubuntu18.04-libc.yml",
    ]
    for filename in config_files:
        build_config, id = get_build_config(filename)
        assert id == "icc-2021.3.0-amd64-ubuntu18.04-libc"
        assert build_config["env"]["MALLOC"] == "libc"


def test_get_build_config_metadata():
    build_config, _ = get_build_config(
        "./utils/tests/test_data/icc-2021.3.0-amd64-ubuntu18.04-libc.yml"
    )
    build_config_metadata = get_build_config_metadata(build_config)
    for k in ["arch", "compiler", "compiler_version", "os"]:
        assert k in build_config_metadata.keys()
    assert len(build_config_metadata.keys()) == 4
