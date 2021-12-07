import math


def extract_client_cpu_limit(benchmark_config):
    db_cpu_limit = benchmark_config["clientconfig"]["resources"]["requests"]["cpus"]
    ceil_db_cpu_limit = math.ceil(float(db_cpu_limit))
    return ceil_db_cpu_limit


def extract_build_variant_variations(benchmark_config, keyname="build-variants"):
    result = None
    if keyname in benchmark_config:
        result = benchmark_config[keyname]
    return result


def extract_client_container_image(benchmark_config, keyname="clientconfig"):
    client_container_image = None
    if keyname in benchmark_config:
        if "run_image" in benchmark_config[keyname]:
            client_container_image = benchmark_config[keyname]["run_image"]
    return client_container_image


def extract_client_tool(benchmark_config, keyname="clientconfig"):
    client_tool = None
    if keyname in benchmark_config:
        if "tool" in benchmark_config[keyname]:
            client_tool = benchmark_config[keyname]["tool"]
    return client_tool
