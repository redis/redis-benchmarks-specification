import datetime
import logging

from redisbench_admin.run.common import get_start_time_vars
from redisbench_admin.run.run import calculate_client_tool_duration_and_check
from redisbench_admin.utils.local import get_local_run_full_filename

from redis_benchmarks_specification.__common__.spec import (
    extract_client_container_image,
    extract_client_tool,
)
from redis_benchmarks_specification.__self_contained_coordinator__.clients import (
    prepare_memtier_benchmark_parameters,
)


def data_prepopulation_step(
    benchmark_config,
    benchmark_tool_workdir,
    client_cpuset_cpus,
    docker_client,
    git_hash,
    port,
    temporary_dir,
    test_name,
    redis_password,
):
    # setup the benchmark
    (
        start_time,
        start_time_ms,
        start_time_str,
    ) = get_start_time_vars()
    local_benchmark_output_filename = get_local_run_full_filename(
        start_time_str,
        git_hash,
        "preload__" + test_name,
        "oss-standalone",
    )
    preload_image = extract_client_container_image(
        benchmark_config["dbconfig"], "preload_tool"
    )
    preload_tool = extract_client_tool(benchmark_config["dbconfig"], "preload_tool")
    full_benchmark_path = "/usr/local/bin/{}".format(preload_tool)
    client_mnt_point = "/mnt/client/"
    if "memtier_benchmark" in preload_tool:
        (
            _,
            preload_command_str,
        ) = prepare_memtier_benchmark_parameters(
            benchmark_config["dbconfig"]["preload_tool"],
            full_benchmark_path,
            port,
            "localhost",
            local_benchmark_output_filename,
            False,
            redis_password,
        )

        logging.info(
            "Using docker image {} as benchmark PRELOAD image (cpuset={}) with the following args: {}".format(
                preload_image,
                client_cpuset_cpus,
                preload_command_str,
            )
        )
        # run the benchmark
        preload_start_time = datetime.datetime.now()

        client_container_stdout = docker_client.containers.run(
            image=preload_image,
            volumes={
                temporary_dir: {
                    "bind": client_mnt_point,
                    "mode": "rw",
                },
            },
            auto_remove=True,
            privileged=True,
            working_dir=benchmark_tool_workdir,
            command=preload_command_str,
            network_mode="host",
            detach=False,
            cpuset_cpus=client_cpuset_cpus,
        )

        preload_end_time = datetime.datetime.now()
        preload_duration_seconds = calculate_client_tool_duration_and_check(
            preload_end_time, preload_start_time, "Preload", False
        )
        logging.info(
            "Tool {} seconds to load data. Output {}".format(
                preload_duration_seconds,
                client_container_stdout,
            )
        )
