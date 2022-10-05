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
