def prepare_memtier_benchmark_parameters(
    clientconfig,
    full_benchmark_path,
    port,
    server,
    local_benchmark_output_filename,
    oss_cluster_api_enabled,
):
    benchmark_command = [
        full_benchmark_path,
        "--port",
        "{}".format(port),
        "--server",
        "{}".format(server),
        "--json-out-file",
        local_benchmark_output_filename,
    ]
    if oss_cluster_api_enabled is True:
        benchmark_command.append("--cluster-mode")
    benchmark_command_str = " ".join(benchmark_command)
    if "arguments" in clientconfig:
        benchmark_command_str = benchmark_command_str + " " + clientconfig["arguments"]

    return None, benchmark_command_str


def prepare_vector_db_benchmark_parameters(
    clientconfig,
    full_benchmark_path,
    port,
    server,
):
    benchmark_command = []
    if port is not None:
        benchmark_command.extend(["REDIS_PORT={}".format(port)])
    benchmark_command.extend(
        [
            full_benchmark_path,
            "--host",
            f"{server}",
        ]
    )
    benchmark_command.extend(
        ["--engines", clientconfig.get("engines", "redis-m-8-ef-16")]
    )
    benchmark_command.extend(
        ["--datasets", clientconfig.get("datasets", "glove-100-angular")]
    )
    benchmark_command_str = " ".join(benchmark_command)
    return None, benchmark_command_str
