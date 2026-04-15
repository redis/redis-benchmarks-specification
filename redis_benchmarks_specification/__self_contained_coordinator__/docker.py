import logging
import time

import docker
import redis

from redis_benchmarks_specification.__self_contained_coordinator__.cpuset import (
    generate_cpuset_cpus,
)


def inject_replication_sync_metrics(
    results_dict, replica_sync_times_seconds, sync_full_during_benchmark
):
    """Inject replication full-sync metrics into a memtier-style results_dict.

    Adds two metrics under results_dict["ALL STATS"]["Totals"]:
    - ReplicationFullSyncSeconds: max sync time across replicas (initial topology setup)
    - ReplicationFullSyncCountDuringBench: count of full syncs during benchmark window

    Returns True on success, False on failure. Safe to call with None or
    non-dict results_dict (returns False).
    """
    if not isinstance(results_dict, dict):
        return False
    try:
        if "ALL STATS" not in results_dict:
            results_dict["ALL STATS"] = {}
        if "Totals" not in results_dict["ALL STATS"]:
            results_dict["ALL STATS"]["Totals"] = {}
        if replica_sync_times_seconds:
            results_dict["ALL STATS"]["Totals"]["ReplicationFullSyncSeconds"] = max(
                replica_sync_times_seconds
            )
        results_dict["ALL STATS"]["Totals"]["ReplicationFullSyncCountDuringBench"] = (
            int(sync_full_during_benchmark)
        )
        return True
    except Exception as e:
        logging.warning("Failed to inject sync metrics: {}".format(e))
        return False


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


def generate_cluster_redis_server_args(
    binary,
    port,
    dbdir,
    configuration_parameters=None,
    redis_arguments="",
    password=None,
):
    """Generate redis-server args with cluster mode enabled."""
    command = generate_standalone_redis_server_args(
        binary, port, dbdir, configuration_parameters, redis_arguments, password
    )
    command.extend(
        [
            "--cluster-enabled",
            "yes",
            "--cluster-config-file",
            "nodes-{}.conf".format(port),
            "--cluster-node-timeout",
            "5000",
        ]
    )
    return command


def _default_start_redis_container(
    command_str,
    db_cpuset_cpus,
    docker_client,
    mnt_point,
    redis_containers,
    run_image,
    temporary_dir,
):
    """Default container start function used when no custom one is provided."""
    volumes = {}
    if mnt_point:
        volumes = {temporary_dir: {"bind": mnt_point, "mode": "rw"}}
    container = docker_client.containers.run(
        image=run_image,
        volumes=volumes,
        auto_remove=True,
        privileged=True,
        working_dir=mnt_point if mnt_point else "/",
        command=command_str,
        network_mode="host",
        detach=True,
        cpuset_cpus=db_cpuset_cpus,
        pid_mode="host",
    )
    time.sleep(5)
    redis_containers.append(container)
    return container


def spin_docker_cluster_redis(
    primary_count,
    ceil_db_cpu_limit,
    current_cpu_pos,
    docker_client,
    redis_configuration_parameters,
    redis_containers,
    redis_proc_start_port,
    run_image,
    temporary_dir,
    start_redis_container_fn=None,
    mnt_point="/mnt/redis/",
    redis_arguments="",
    password=None,
):
    """Start N Redis instances in cluster mode and form a cluster.

    Returns:
        tuple: (cluster_conns, cluster_pids, current_cpu_pos)
    """
    if start_redis_container_fn is None:
        start_redis_container_fn = _default_start_redis_container
    per_node_cpu = max(1, ceil_db_cpu_limit // primary_count)
    cluster_conns = []
    cluster_pids = []

    # Start each cluster node
    for i in range(primary_count):
        node_port = redis_proc_start_port + i
        node_redis_arguments = redis_arguments
        # Per-node filenames to avoid conflicts
        if i > 0:
            node_redis_arguments = (
                (
                    "{} --dbfilename cluster-node-{}-dump.rdb"
                    " --appendfilename cluster-node-{}-appendonly.aof"
                    " --logfile cluster-node-{}-redis.log"
                )
                .format(redis_arguments, node_port, node_port, node_port)
                .strip()
            )
        command = generate_cluster_redis_server_args(
            "{}redis-server".format(mnt_point) if mnt_point else "redis-server",
            node_port,
            mnt_point if mnt_point else "",
            redis_configuration_parameters,
            node_redis_arguments,
            password,
        )
        command_str = " ".join(command)
        db_cpuset_cpus, current_cpu_pos = generate_cpuset_cpus(
            per_node_cpu, current_cpu_pos
        )
        logging.info(
            "Starting cluster node {}/{} on port {} (cpuset={})".format(
                i + 1, primary_count, node_port, db_cpuset_cpus
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
        r = redis.StrictRedis(port=node_port, password=password)
        r.ping()
        cluster_conns.append(r)
        node_info = r.info()
        node_pid = node_info.get("process_id")
        if node_pid is not None:
            cluster_pids.append(node_pid)

    # CLUSTER MEET: make all nodes aware of each other via node 0
    first = cluster_conns[0]
    for i in range(1, primary_count):
        node_port = redis_proc_start_port + i
        first.execute_command("CLUSTER", "MEET", "127.0.0.1", str(node_port))
        logging.info("CLUSTER MEET 127.0.0.1 {}".format(node_port))

    # Distribute 16384 slots evenly across primaries
    slots_per_node = 16384 // primary_count
    for i, conn in enumerate(cluster_conns):
        start_slot = i * slots_per_node
        end_slot = ((i + 1) * slots_per_node - 1) if i < primary_count - 1 else 16383
        # Batch ADDSLOTS in groups of 500 to avoid arg-length limits
        batch_size = 500
        slot = start_slot
        while slot <= end_slot:
            batch_end = min(slot + batch_size - 1, end_slot)
            slot_args = [str(s) for s in range(slot, batch_end + 1)]
            conn.execute_command("CLUSTER", "ADDSLOTS", *slot_args)
            slot = batch_end + 1
        logging.info(
            "Assigned slots [{}-{}] to node {} (port {})".format(
                start_slot, end_slot, i, redis_proc_start_port + i
            )
        )

    # Wait for cluster_state:ok
    timeout = 60
    poll_interval = 0.5
    start = time.monotonic()
    while True:
        elapsed = time.monotonic() - start
        if elapsed >= timeout:
            raise RuntimeError(
                "Cluster did not reach 'ok' state within {}s".format(timeout)
            )
        cluster_info = first.execute_command("CLUSTER", "INFO")
        if isinstance(cluster_info, bytes):
            cluster_info = cluster_info.decode()
        if "cluster_state:ok" in cluster_info:
            logging.info(
                "Cluster is ready (cluster_state:ok) after {:.1f}s".format(elapsed)
            )
            break
        time.sleep(poll_interval)

    return cluster_conns, cluster_pids, current_cpu_pos


def spin_up_redis_replicas(
    replica_count,
    primary_port,
    current_cpu_pos,
    docker_client,
    redis_containers,
    run_image,
    temporary_dir,
    mnt_point,
    replica_cpu_limit,
    redis_configuration_parameters,
    redis_arguments,
    password,
    start_redis_container_fn,
    replication_sync_timeout=600,
    server_name="redis",
):
    """Start replica Redis containers and configure replication to the primary.

    Returns:
        tuple: (replica_conns, replica_pids, current_cpu_pos, sync_times_seconds)

    sync_times_seconds is a list of float seconds, one per replica, measuring
    the wall-clock time from container start to master_link_status=up.
    Use this as a benchmark metric for full-sync performance testing.
    """
    replica_conns = []
    replica_pids = []
    sync_times_seconds = []
    for i in range(1, replica_count + 1):
        replica_port = primary_port + i
        # Append --replicaof and --masterauth to redis_arguments so the replica
        # can authenticate to the primary and parca-agent can label it as a replica.
        # Use per-replica filenames to avoid conflicts with the primary.
        replica_redis_arguments = "{} --replicaof localhost {}".format(
            redis_arguments, primary_port
        ).strip()
        if password is not None and password != "":
            replica_redis_arguments += " --masterauth {}".format(password)
        replica_redis_arguments += (
            " --dbfilename replica-port-{}-dump.rdb"
            " --appendfilename replica-port-{}-appendonly.aof"
            " --logfile replica-port-{}-redis.log"
        ).format(replica_port, replica_port, replica_port)
        command = generate_standalone_redis_server_args(
            "{}{}-server".format(mnt_point, server_name),
            replica_port,
            mnt_point,
            redis_configuration_parameters,
            replica_redis_arguments,
            password,
        )
        command_str = " ".join(command)
        db_cpuset_cpus, current_cpu_pos = generate_cpuset_cpus(
            replica_cpu_limit, current_cpu_pos
        )
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
        logging.info(
            "Replica {} started with --replicaof localhost {}".format(i, primary_port)
        )
        # Wait for replication link to come up. Use monotonic clock for
        # high-resolution measurement of full-sync time.
        sync_start = time.monotonic()
        poll_interval = (
            0.1  # 100ms — fast enough to be accurate, slow enough not to thrash
        )
        sync_seconds = None
        while True:
            elapsed = time.monotonic() - sync_start
            if elapsed >= replication_sync_timeout:
                break
            repl_info = replica_r.info("replication")
            if repl_info.get("master_link_status") == "up":
                sync_seconds = elapsed
                logging.info(
                    "Replica {} replication link is up (full sync took {:.3f}s)".format(
                        i, sync_seconds
                    )
                )
                break
            time.sleep(poll_interval)
        if sync_seconds is None:
            logging.warning(
                "Replica {} replication link did not come up within {}s".format(
                    i, replication_sync_timeout
                )
            )
            sync_seconds = float(replication_sync_timeout)
        sync_times_seconds.append(sync_seconds)
        replica_info = replica_r.info()
        replica_pid = replica_info.get("process_id")
        if replica_pid is not None:
            replica_pids.append(replica_pid)
        replica_conns.append(replica_r)
    return replica_conns, replica_pids, current_cpu_pos, sync_times_seconds
