import yaml

from redis_benchmarks_specification.__common__.spec import (
    extract_client_cpu_limit,
    extract_client_container_image,
)
from redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator import (
    generate_cpuset_cpus,
)


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
