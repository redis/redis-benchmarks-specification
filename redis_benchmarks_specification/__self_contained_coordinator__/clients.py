import logging


def prepare_memtier_benchmark_parameters(
    clientconfig,
    full_benchmark_path,
    port,
    server,
    local_benchmark_output_filename,
    oss_cluster_api_enabled,
    password=None,
):
    benchmark_command = [
        full_benchmark_path,
        "--json-out-file",
        local_benchmark_output_filename,
        "--port",
        "{}".format(port),
        "--server",
        "{}".format(server),
    ]

    # Add password authentication if provided
    if password is not None and password != "":
        benchmark_command.extend(["--authenticate", password])
        logging.info("Memtier benchmark will use password authentication")

    if oss_cluster_api_enabled is True:
        benchmark_command.append("--cluster-mode")
    benchmark_command_str = " ".join(benchmark_command)
    if "arguments" in clientconfig:
        benchmark_command_str = benchmark_command_str + " " + clientconfig["arguments"]

    return None, benchmark_command_str


def prepare_vector_db_benchmark_parameters(
    clientconfig, full_benchmark_path, port, server, password, client_mnt_point
):
    benchmark_command = []
    # if port is not None:
    #     benchmark_command.extend(["REDIS_PORT={}".format(port)])
    # if password is not None:
    #     benchmark_command.extend(["REDIS_AUTH={}".format(password)])
    benchmark_command.extend(
        [
            full_benchmark_path,
            "--host",
            f"{server}",
        ]
    )
    benchmark_command.extend(["--engines", clientconfig.get("engines", "redis-test")])
    benchmark_command.extend(
        ["--datasets", clientconfig.get("datasets", "glove-100-angular")]
    )
    benchmark_command_str = " ".join(benchmark_command)
    benchmark_command_str = f"bash -c 'ITERATIONS=1 {benchmark_command_str} && mv /code/results {client_mnt_point}.'"
    return None, benchmark_command_str
