import math


def extract_client_cpu_limit(benchmark_config):
    db_cpu_limit = benchmark_config["clientconfig"]["resources"]["requests"]["cpus"]
    ceil_db_cpu_limit = math.ceil(float(db_cpu_limit))
    return ceil_db_cpu_limit


def extract_client_container_image(benchmark_config):
    client_container_image = None
    if "clientconfig" in benchmark_config:
        if "run_image" in benchmark_config["clientconfig"]:
            client_container_image = benchmark_config["clientconfig"]["run_image"]
    return client_container_image
