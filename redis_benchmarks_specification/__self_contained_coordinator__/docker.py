import logging
import math
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
    - ReplicationFullSyncSecondsV2: max sync time across replicas (initial topology setup)
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
            results_dict["ALL STATS"]["Totals"]["ReplicationFullSyncSecondsV2"] = max(
                replica_sync_times_seconds
            )
        results_dict["ALL STATS"]["Totals"]["ReplicationFullSyncCountDuringBench"] = (
            int(sync_full_during_benchmark)
        )
        return True
    except Exception as e:
        logging.warning("Failed to inject sync metrics: {}".format(e))
        return False


def measure_replica_full_sync(
    replica_conn, primary_port, timeout=600, poll_interval=0.05
):
    """Time an explicit REPLICAOF through the first observed usable link.

    Startup/PING is outside the interval. The measurement includes the command,
    handshake, configured diskless delay, transfer and loading. Observation delay
    includes polling, INFO latency and any timeout/reconnect gaps; this is not
    an exact server event timestamp.
    A fresh, empty standalone replica prevents reuse of a previous replication ID.
    The caller must configure finite socket timeouts on this connection.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("Replication timeout must be finite and positive")
    if not math.isfinite(poll_interval) or poll_interval <= 0:
        raise ValueError("Replication poll interval must be finite and positive")
    if (
        replica_conn.info("replication").get("role") != "master"
        or replica_conn.dbsize() != 0
    ):
        raise ValueError(
            "Initial full-sync timing requires a fresh empty standalone replica"
        )
    start = time.monotonic()
    replica_conn.execute_command("REPLICAOF", "localhost", primary_port)
    while True:
        if time.monotonic() - start >= timeout:
            raise TimeoutError("Initial replica full sync exceeded its deadline")
        try:
            info = replica_conn.info("replication")
        except (redis.TimeoutError, redis.BusyLoadingError):
            # Loading a large RDB can temporarily delay or reject INFO. Keep the
            # original deadline; a late response must not create a valid sample.
            info = {}
        elapsed = time.monotonic() - start
        if elapsed >= timeout:
            raise TimeoutError("Initial replica full sync exceeded its deadline")
        if (
            info.get("role") == "slave"
            and info.get("master_link_status") == "up"
            and info.get("master_sync_in_progress") == 0
        ):
            return elapsed
        time.sleep(min(poll_interval, timeout - elapsed))


def generate_standalone_dragonfly_server_args(
    binary,
    port,
    dbdir,
    configuration_parameters=None,
    redis_arguments="",
    password=None,
):
    """Build a launch command for a Dragonfly server (gflags-based CLI).

    Dragonfly parses its flags with Abseil/gflags and ABORTS on any unknown flag, so we
    cannot pass the redis-style args (``--protected-mode``, ``--logfile``, ``--save``,
    ``--maxmemory-policy``, ``--io-threads`` ...). We emit only valid Dragonfly gflags and
    translate / drop the redis ``configuration-parameters`` accordingly. Snapshotting to a
    file is disabled (``--dbfilename=``) and the server is pinned to a single proactor
    thread by default so a standalone run is comparable to single-threaded redis-server
    (a topology may override via a ``--proactor_threads=N`` token in ``redis_arguments``).
    """
    # Allow a topology to override the proactor-thread count; default to 1 (apples-to-apples
    # vs single-threaded redis-server). Accept both the gflags "=" form (--proactor_threads=N)
    # and the space form (--proactor_threads N); warn loudly if a proactor override is present
    # but un-parseable so a benchmark never silently runs on the wrong thread count.
    proactor_threads = "1"
    if redis_arguments != "":
        toks = redis_arguments.split(" ")
        parsed = False
        for i, tok in enumerate(toks):
            if tok.startswith("--proactor_threads="):
                proactor_threads = tok.split("=", 1)[1]
                parsed = True
            elif tok == "--proactor_threads" and i + 1 < len(toks):
                proactor_threads = toks[i + 1]
                parsed = True
        if "proactor_threads" in redis_arguments and not parsed:
            logging.warning(
                "redis_arguments contains 'proactor_threads' but no value was parsed; "
                "defaulting Dragonfly to --proactor_threads=1. redis_arguments=%r",
                redis_arguments,
            )
    command = [
        binary,
        "--port",
        "{}".format(port),
        "--logtostderr",  # replaces redis --logfile
        "--proactor_threads={}".format(proactor_threads),
        "--dbfilename=",  # no snapshot file (the Dragonfly analogue of redis save "")
        "--version_check=false",  # disable the once-a-day phone-home (jitter / egress dep)
        "--default_lua_flags=allow-undeclared-keys",  # tolerate EVAL-based init_commands
    ]
    if password is not None and password != "":
        command.extend(["--requirepass", password])
        logging.info("Dragonfly server will be started with password authentication")
    if dbdir != "":
        command.extend(["--dir", dbdir])
    # Translate the handful of redis config params that have a Dragonfly equivalent; all
    # other redis-only params are silently dropped so Dragonfly does not abort on startup.
    dragonfly_param_map = {"maxmemory": "maxmemory"}
    if configuration_parameters is not None:
        for parameter, parameter_value in configuration_parameters.items():
            if parameter in dragonfly_param_map:
                command.append(
                    "--{}={}".format(dragonfly_param_map[parameter], parameter_value)
                )
            else:
                logging.info(
                    "Dropping redis-only config parameter '{}' for Dragonfly launch".format(
                        parameter
                    )
                )
    return command


def generate_standalone_redis_server_args(
    binary,
    port,
    dbdir,
    configuration_parameters=None,
    redis_arguments="",
    password=None,
    server_name="redis",
):
    # Dragonfly speaks RESP/port-6379 but uses a gflags CLI that aborts on redis-style
    # flags — dispatch to a server-specific arg generator. Default path (redis/valkey,
    # which share redis-server semantics) is unchanged.
    if server_name == "dragonfly":
        return generate_standalone_dragonfly_server_args(
            binary,
            port,
            dbdir,
            configuration_parameters,
            redis_arguments,
            password,
        )
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


def start_redis_container(
    command_str,
    db_cpuset_cpus,
    docker_client,
    mnt_point,
    redis_containers,
    run_image,
    temporary_dir,
    auto_remove=False,
):
    """Start a Redis container with the given configuration.

    Used for standalone, cluster, and replica container startup.
    """
    logging.info(
        "Running redis-server on docker image {} (cpuset={}) with the following args: {}".format(
            run_image, db_cpuset_cpus, command_str
        )
    )
    volumes = {}
    if mnt_point != "":
        volumes = {temporary_dir: {"bind": mnt_point, "mode": "rw"}}
        logging.info(f"setting volume as follow: {volumes}. working_dir={mnt_point}")
    container = docker_client.containers.run(
        image=run_image,
        volumes=volumes,
        auto_remove=auto_remove,
        privileged=True,
        working_dir=mnt_point,
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
    mnt_point="/mnt/redis/",
    redis_arguments="",
    password=None,
    server_name="redis",
):
    """Start N Redis instances in cluster mode and form a cluster.

    Returns:
        tuple: (cluster_conns, current_cpu_pos)
    """
    # gflags-based servers (e.g. dragonfly) are standalone-only for now. The cluster path
    # emits redis-style flags (--cluster-enabled ...) such a server aborts on, and uses a
    # bare PING. Fail loudly rather than silently mis-launch / hang.
    if server_name == "dragonfly":
        raise NotImplementedError(
            "server_name='{}' is only supported for standalone topologies; "
            "cluster mode is not yet implemented for gflags-based servers.".format(
                server_name
            )
        )
    executable = "{}{}-server".format(mnt_point, server_name)
    per_node_cpu = max(1, ceil_db_cpu_limit // primary_count)
    cluster_conns = []

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
            executable,
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
        start_redis_container(
            command_str,
            db_cpuset_cpus,
            docker_client,
            mnt_point,
            redis_containers,
            run_image,
            temporary_dir,
            auto_remove=True,
        )
        r = redis.StrictRedis(port=node_port, password=password)
        r.ping()
        cluster_conns.append(r)

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

    return cluster_conns, current_cpu_pos


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
    replication_sync_timeout=600,
    server_name="redis",
    expected_keyspacelen=None,
):
    """Start replica Redis containers and configure replication to the primary.

    Returns:
        tuple: (replica_conns, current_cpu_pos, sync_times_seconds)

    sync_times_seconds is a list of float seconds, one per replica, measuring
    elapsed time from explicit REPLICAOF to the first observed usable link.
    Replicas synchronize serially; this is not a concurrent fan-out measurement.
    Failures raise and must never be exported as durations.
    """
    # gflags-based servers (e.g. dragonfly) are standalone-only for now; replica topologies
    # pass redis-style --replicaof/--masterauth flags such a server aborts on. Fail loudly.
    if server_name == "dragonfly":
        raise NotImplementedError(
            "server_name='{}' is only supported for standalone topologies; "
            "replica mode is not yet implemented for gflags-based servers.".format(
                server_name
            )
        )
    replica_conns = []
    sync_times_seconds = []
    for i in range(1, replica_count + 1):
        replica_port = primary_port + i
        # Defer replication until the process is ready, so container startup
        # cannot hide the beginning (or all) of a short full sync.
        # Keep the replicaof argv marker used by process-role classifiers while
        # disabling replication until the timed REPLICAOF command below.
        replica_redis_arguments = "{} --replicaof no one".format(
            redis_arguments
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
        start_redis_container(
            command_str,
            db_cpuset_cpus,
            docker_client,
            mnt_point,
            redis_containers,
            run_image,
            temporary_dir,
            auto_remove=True,
        )
        replica_r = redis.StrictRedis(port=replica_port, password=password)
        timing_conn = redis.StrictRedis(
            port=replica_port,
            password=password,
            socket_timeout=1,
            socket_connect_timeout=1,
        )
        primary_timing_conn = redis.StrictRedis(
            port=primary_port,
            password=password,
            socket_timeout=1,
            socket_connect_timeout=1,
        )
        try:
            timing_conn.ping()
            if (
                expected_keyspacelen is not None
                and primary_timing_conn.dbsize() != expected_keyspacelen
            ):
                raise ValueError(
                    "Primary key count does not match the declared full-sync dataset"
                )
            full_syncs_before = primary_timing_conn.info("stats")["sync_full"]
            sync_seconds = measure_replica_full_sync(
                timing_conn, primary_port, replication_sync_timeout
            )
            full_syncs_after = primary_timing_conn.info("stats")["sync_full"]
            if full_syncs_after - full_syncs_before != 1:
                raise ValueError(
                    "Initial-sync sample did not contain exactly one full sync"
                )
            if (
                expected_keyspacelen is not None
                and timing_conn.dbsize() != expected_keyspacelen
            ):
                raise ValueError(
                    "Replica key count does not match the declared full-sync dataset"
                )
        finally:
            timing_conn.close()
            primary_timing_conn.close()
        logging.info("Replica %s initial full sync completed in %.3fs", i, sync_seconds)
        sync_times_seconds.append(sync_seconds)
        replica_conns.append(replica_r)
    return replica_conns, current_cpu_pos, sync_times_seconds
