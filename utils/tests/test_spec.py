#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import yaml

from redis_benchmarks_specification.__common__.spec import (
    extract_build_variant_variations,
)


def test_extract_build_variant_variations():
    with open(
        "./utils/tests/test_data/test-suites/redis-benchmark-full-suite-1Mkeys-100B.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        build_variants = extract_build_variant_variations(benchmark_config)
        assert build_variants == None

    with open(
        "./redis_benchmarks_specification/test-suites/memtier_benchmark-1Mkeys-100B-expire-use-case.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        build_variants = extract_build_variant_variations(benchmark_config)
        assert "gcc:15.2.0-amd64-debian-bookworm-default" in build_variants


def test_extract_redis_dbconfig_parameters():
    with open(
        "./redis_benchmarks_specification/test-suites/memtier_benchmark-1Mkeys-100B-expire-use-case.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        build_variants = extract_build_variant_variations(benchmark_config)
        assert "gcc:15.2.0-amd64-debian-bookworm-default" in build_variants
