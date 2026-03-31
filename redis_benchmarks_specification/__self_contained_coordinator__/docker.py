import logging
import time

import docker
import redis

from redis_benchmarks_specification.__self_contained_coordinator__.cpuset import (
    generate_cpuset_cpus,
)


def generate_standalone_redis_server_args(
    binary,
    port,
    dbdir,
    configuration_parameters=None,
    redis_arguments="",
    password=None,
):
    added_params = ["port", "protected-mode", "dir", "requirepass", "logfile"]
    # start redis-server
    command = [
        binary,
        "--protected-mode",
        "no",
        "--port",
        "{}".format(port),
    ]

    # Add password authentication if provided
    if password is not None and password != "":
        command.extend(["--requirepass", password])
        logging.info("Redis server will be started with password authentication")
    if dbdir != "":
        command.extend(["--dir", dbdir])
        command.extend(["--logfile", f"{dbdir}redis.log"])
    if configuration_parameters is not None:
        for parameter, parameter_value in configuration_parameters.items():
            if parameter not in added_params:
                command.extend(
                    [
                        "--{}".format(parameter),
                        parameter_value,
                    ]
                )
    if redis_arguments != "":
        redis_arguments_arr = redis_arguments.split(" ")
        logging.info(f"adding redis arguments {redis_arguments_arr}")
        command.extend(redis_arguments_arr)
    return command


def teardown_containers(redis_containers, container_type):
    for container in redis_containers:
        try:
            container.stop()
        except docker.errors.NotFound:
            logging.info(
                "When trying to stop {} container with id {} and image {} it was already stopped".format(
                    container_type, container.id, container.image
                )
            )
            pass


def spin_docker_standalone_redis(
    ceil_db_cpu_limit,
    current_cpu_pos,
    docker_client,
    redis_configuration_parameters,
    redis_containers,
    redis_proc_start_port,
    run_image,
    temporary_dir,
    password=None,
):
    mnt_point = "/mnt/redis/"
    command = generate_standalone_redis_server_args(
        "{}redis-server".format(mnt_point),
        redis_proc_start_port,
        mnt_point,
        redis_configuration_parameters,
        "",
        password,
    )
    command_str = " ".join(command)
    db_cpuset_cpus, current_cpu_pos = generate_cpuset_cpus(
        ceil_db_cpu_limit, current_cpu_pos
    )
    logging.info(
        "Running redis-server on docker image {} (cpuset={}) with the following args: {}".format(
            run_image, db_cpuset_cpus, command_str
        )
    )
    container = docker_client.containers.run(
        image=run_image,
        volumes={
            temporary_dir: {"bind": mnt_point, "mode": "rw"},
        },
        auto_remove=True,
        privileged=True,
        working_dir=mnt_point,
        command=command_str,
        network_mode="host",
        detach=True,
        cpuset_cpus=db_cpuset_cpus,
        pid_mode="host",
    )
    redis_containers.append(container)
    return current_cpu_pos


def spin_up_redis_replicas(
    replica_count,
    primary_port,
    current_cpu_pos,
    docker_client,
    redis_containers,
    run_image,
    temporary_dir,
    mnt_point,
    redis_configuration_parameters,
    redis_arguments,
    password,
    start_redis_container_fn,
    replication_sync_timeout=60,
):
    """Start replica Redis containers and configure replication to the primary.

    Returns:
        tuple: (replica_conns, replica_pids, current_cpu_pos)
    """
    replica_conns = []
    replica_pids = []
    for i in range(1, replica_count + 1):
        replica_port = primary_port + i
        command = generate_standalone_redis_server_args(
            "{}redis-server".format(mnt_point),
            replica_port,
            mnt_point,
            redis_configuration_parameters,
            redis_arguments,
            password,
        )
        command_str = " ".join(command)
        db_cpuset_cpus, current_cpu_pos = generate_cpuset_cpus(1, current_cpu_pos)
        logging.info(
            "Starting replica {}/{} on port {} (cpuset={})".format(
                i, replica_count, replica_port, db_cpuset_cpus
            )
        )
        start_redis_container_fn(
            command_str,
            db_cpuset_cpus,
            docker_client,
            mnt_point,
            redis_containers,
            run_image,
            temporary_dir,
        )
        replica_r = redis.StrictRedis(port=replica_port, password=password)
        replica_r.ping()
        replica_r.replicaof("localhost", primary_port)
        logging.info("Replica {} issued REPLICAOF localhost {}".format(i, primary_port))
        # Wait for replication link to come up
        elapsed = 0
        poll_interval = 1
        while elapsed < replication_sync_timeout:
            repl_info = replica_r.info("replication")
            if repl_info.get("master_link_status") == "up":
                logging.info(
                    "Replica {} replication link is up (took {}s)".format(i, elapsed)
                )
                break
            time.sleep(poll_interval)
            elapsed += poll_interval
        else:
            logging.warning(
                "Replica {} replication link did not come up within {}s".format(
                    i, replication_sync_timeout
                )
            )
        replica_info = replica_r.info()
        replica_pid = replica_info.get("process_id")
        if replica_pid is not None:
            replica_pids.append(replica_pid)
        replica_conns.append(replica_r)
    return replica_conns, replica_pids, current_cpu_pos
