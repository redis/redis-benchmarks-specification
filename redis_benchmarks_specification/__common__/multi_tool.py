"""Shared multi-tool benchmark engine.

Extracted from ``__runner__/runner.py`` so that both the standalone runner and
the self-contained coordinator can launch multi-tool ``clientconfigs`` suites
(e.g. a measuring ``memtier_benchmark`` client alongside a long-lived
``bcast-listener`` background helper) using the same proven launch / wait /
aggregate / teardown logic.

The per-tool ``prepare_*_parameters`` functions intentionally STAY in
``__runner__/runner.py`` and are reached here via a lazy import inside
``prepare_client_run_specs`` to avoid a ``__common__`` -> ``__runner__`` import
cycle.
"""

import json
import logging
import math
import os
import re
from dataclasses import dataclass, field
from typing import List, Optional

import docker

from redis_benchmarks_specification.__common__.spec import (
    extract_client_configs,
    extract_client_container_images,
    extract_client_tools,
)


@dataclass
class ClientRunSpec:
    client_index: int
    client_tool: str  # e.g. "memtier_benchmark", "bcast-listener"
    client_image: str
    command_str: str  # fully-prepared benchmark command
    output_filename: str  # basename, e.g. benchmark_output_{i}.json
    is_background: bool = False  # "bcast-listener" in client_tool
    environment: Optional[dict] = None  # vector-db env vars
    working_dir: Optional[str] = None  # vector-db => /app
    extra_volumes: Optional[dict] = None  # vector-db => /app/results mount
    cpus: float = 0.0  # this client's resources.requests.cpus (for cpuset split)


@dataclass
class MultiToolResult:
    aggregated_stdout: str  # JSON string (memtier-based merge)
    results: list  # per-client {client_index, stdout, config, tool, image}
    memtier_output_filename: Optional[str]  # basename of the measured client's JSON
    background_failures: list = field(default_factory=list)


def remove_started_containers(containers):
    """Force-remove every container started so far (best effort).

    Used to clean up after a mid-launch failure so partially-started runs do
    not leak running containers (notably long-lived background helpers).
    """
    for started in containers:
        try:
            started["container"].remove(force=True)
        except Exception as cleanup_error:
            logging.warning(
                f"Error removing partially-started client "
                f"{started.get('client_index')}: {cleanup_error}"
            )


def stop_background_helper(container_info):
    """Stop a background-helper container (e.g. bcast-listener) and report
    whether it had already exited before we stopped it.

    Background helpers hold their connections open until killed, so under
    normal operation they are still ``running`` when the measuring clients
    finish and we stop them with SIGTERM. If a helper has exited on its own
    (auth error, OOM, crash) it means the listeners were NOT active for the
    full measured window, so the run must fail.

    Always attempts to remove the container (no leak, even on failure).

    Returns a failure tuple ``(client_index, client_tool, status, exit_code)``
    if the helper exited/disappeared during the run, otherwise ``None``.
    """
    container = container_info["container"]
    client_index = container_info["client_index"]
    client_tool = container_info["client_tool"]
    failure = None
    try:
        try:
            container.reload()
            status = container.status
        except docker.errors.NotFound:
            status = "not-found"
        except Exception as e:
            logging.warning(
                f"Could not query background helper {client_index} status: {e}"
            )
            status = "unknown"

        if status == "running":
            logging.info(
                f"Stopping background helper client {client_index} ({client_tool})"
            )
            try:
                container.stop(timeout=10)  # SIGTERM, then SIGKILL after 10s
            except docker.errors.NotFound:
                pass
        elif status == "not-found":
            logging.error(
                f"Background helper {client_index} ({client_tool}) container is "
                f"gone -- it did not survive the run; results are unreliable."
            )
            failure = (client_index, client_tool, status, None)
        else:
            # Exited/crashed during the measured window.
            exit_code = None
            try:
                exit_code = container.attrs.get("State", {}).get("ExitCode")
            except Exception:
                pass
            logging.error(
                f"Background helper {client_index} ({client_tool}) exited early "
                f"(status={status}, exit_code={exit_code}) -- tracking listeners "
                f"were NOT active for the full measured window; results are "
                f"unreliable."
            )
            failure = (client_index, client_tool, status, exit_code)

        try:
            bg_stdout = container.logs().decode("utf-8")
            logging.info(
                f"Background helper {client_index} ({client_tool}) logs:\n{bg_stdout}"
            )
        except Exception:
            pass
    except Exception as e:
        logging.warning(f"Error handling background helper {client_index}: {e}")
    finally:
        try:
            container.remove(force=True)
        except Exception as cleanup_error:
            logging.warning(
                f"Background helper {client_index} cleanup error: {cleanup_error}"
            )
    return failure


def prepare_client_run_specs(
    benchmark_config,
    *,
    host,
    port,
    password,
    oss_cluster_api_enabled,
    tls_enabled,
    tls_skip_verify,
    test_tls_cert,
    test_tls_key,
    test_tls_cacert,
    resp_version,
    override_memtier_test_time,
    override_test_runs,
    unix_socket,
    benchmark_tool_workdir,
    benchmark_local_install=False,
    memtier_bin_path="memtier_benchmark",
    client_mnt_point="/mnt/client/",
):
    """Normalize ``benchmark_config`` (single ``clientconfig`` OR multi
    ``clientconfigs``) into a list of :class:`ClientRunSpec`.

    Uses ``extract_client_configs`` / ``extract_client_tools`` /
    ``extract_client_container_images`` from ``__common__.spec`` (so len==1 and
    len==N share one path) and dispatches per tool to the runner's
    ``prepare_*`` functions (memtier / pubsub-sub-bench / bcast-listener /
    vector-db / else generic). Ports ``runner.py:677-792``.

    The ``prepare_*`` functions are lazily imported from ``__runner__.runner``
    inside this function body to avoid a ``__common__`` -> ``__runner__`` import
    cycle.
    """
    # Lazy import to avoid __common__ -> __runner__ import cycle.
    from redis_benchmarks_specification.__runner__.runner import (
        prepare_benchmark_parameters,
        prepare_bcast_listener_parameters,
        prepare_memtier_benchmark_parameters,
        prepare_pubsub_sub_bench_parameters,
        prepare_vector_db_benchmark_parameters,
    )

    client_configs = extract_client_configs(benchmark_config)
    client_images = extract_client_container_images(benchmark_config)
    client_tools = extract_client_tools(benchmark_config)

    if not client_configs:
        raise ValueError("No client configurations found")

    run_specs = []

    for client_index, (client_config, client_tool, client_image) in enumerate(
        zip(client_configs, client_tools, client_images)
    ):
        local_benchmark_output_filename = f"benchmark_output_{client_index}.json"
        client_env_vars = None

        # Prepare benchmark command for this client
        if "memtier_benchmark" in client_tool:
            # Set benchmark path based on local install option
            if benchmark_local_install:
                full_benchmark_path = memtier_bin_path
            else:
                full_benchmark_path = f"/usr/local/bin/{client_tool}"

            (
                _,
                benchmark_command_str,
                arbitrary_command,
            ) = prepare_memtier_benchmark_parameters(
                client_config,
                full_benchmark_path,
                port,
                host,
                password,
                local_benchmark_output_filename,
                oss_cluster_api_enabled,
                tls_enabled,
                tls_skip_verify,
                test_tls_cert,
                test_tls_key,
                test_tls_cacert,
                resp_version,
                override_memtier_test_time,
                override_test_runs,
                unix_socket,
            )
        elif "pubsub-sub-bench" in client_tool:
            (
                _,
                benchmark_command_str,
                arbitrary_command,
            ) = prepare_pubsub_sub_bench_parameters(
                client_config,
                client_tool,
                port,
                host,
                password,
                local_benchmark_output_filename,
                oss_cluster_api_enabled,
                tls_enabled,
                tls_skip_verify,
                test_tls_cert,
                test_tls_key,
                test_tls_cacert,
                resp_version,
                override_memtier_test_time,
                unix_socket,
                None,  # username
            )
        elif "bcast-listener" in client_tool:
            (
                _,
                benchmark_command_str,
                arbitrary_command,
            ) = prepare_bcast_listener_parameters(
                client_config,
                client_tool,
                port,
                host,
                password,
                local_benchmark_output_filename,
                oss_cluster_api_enabled,
                tls_enabled,
                tls_skip_verify,
                test_tls_cert,
                test_tls_key,
                test_tls_cacert,
                resp_version,
                override_memtier_test_time,
                unix_socket,
                None,  # username
            )
        elif "vector-db-benchmark" in client_tool:
            (
                _,
                benchmark_command_str,
                arbitrary_command,
                client_env_vars,
            ) = prepare_vector_db_benchmark_parameters(
                client_config,
                client_tool,
                port,
                host,
                password,
                local_benchmark_output_filename,
                oss_cluster_api_enabled,
                tls_enabled,
                tls_skip_verify,
                test_tls_cert,
                test_tls_key,
                test_tls_cacert,
                resp_version,
                override_memtier_test_time,
                unix_socket,
                None,  # username
            )
        else:
            # Handle other benchmark tools
            (
                benchmark_command,
                benchmark_command_str,
            ) = prepare_benchmark_parameters(
                {**benchmark_config, "clientconfig": client_config},
                client_tool,
                port,
                host,
                local_benchmark_output_filename,
                False,
                benchmark_tool_workdir,
                False,
            )

        is_background = "bcast-listener" in client_tool

        try:
            client_cpus = float(
                client_config.get("resources", {}).get("requests", {}).get("cpus", 0)
            )
        except (TypeError, ValueError):
            client_cpus = 0.0

        working_dir = None
        environment = None
        extra_volumes = None
        if "vector-db-benchmark" in client_tool:
            working_dir = "/app"  # vector-db-benchmark needs to run from /app
            environment = client_env_vars
            # vector-db-benchmark also mounts the results directory at
            # /app/results (the source dir is resolved at launch time).
            extra_volumes = {"/app/results": {"mode": "rw"}}

        run_specs.append(
            ClientRunSpec(
                client_index=client_index,
                client_tool=client_tool,
                client_image=client_image,
                command_str=benchmark_command_str,
                output_filename=local_benchmark_output_filename,
                is_background=is_background,
                environment=environment,
                working_dir=working_dir,
                extra_volumes=extra_volumes,
                cpus=client_cpus,
            )
        )

    return run_specs


def _expand_cpuset(cpuset_cpus):
    """Parse a cpuset string ('4-11', '4,5,6', '4-7,12') into an ordered list of ints."""
    ids = []
    if not cpuset_cpus:
        return ids
    for part in str(cpuset_cpus).split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            ids.extend(range(int(a), int(b) + 1))
        else:
            ids.append(int(part))
    return ids


def partition_cpuset(cpuset_cpus, run_specs):
    """Give each client container a DISJOINT contiguous sub-range of the aggregate
    client cpuset, sized by its requested cpus (rounded up), so a measuring client
    (memtier) and a background helper (e.g. bcast-listener) do not contend for the
    same cores -- which would contaminate the measurement.

    Returns a dict {client_index: 'a,b,c'} when a clean disjoint split is possible.
    Returns None (caller shares the full cpuset for all clients, the legacy
    behavior) when the split is not possible: a single client, no cpuset, any
    client without a positive cpus request, or the per-client ceils not fitting
    the aggregate.
    """
    cores = _expand_cpuset(cpuset_cpus)
    if not cores or len(run_specs) < 2:
        return None
    needs = []
    for spec in run_specs:
        try:
            n = int(math.ceil(float(getattr(spec, "cpus", 0) or 0)))
        except (TypeError, ValueError):
            n = 0
        if n <= 0:
            return None  # cannot size this container's pin -> share the full set
        needs.append(n)
    if sum(needs) > len(cores):
        logging.warning(
            "multi_tool: sum of client cpu requests (ceil=%d) exceeds the client "
            "cpuset size (%d cores: %s); sharing the full cpuset across all client "
            "containers (no isolation)",
            sum(needs),
            len(cores),
            cpuset_cpus,
        )
        return None
    partition = {}
    pos = 0
    for spec, n in zip(run_specs, needs):
        partition[spec.client_index] = ",".join(str(x) for x in cores[pos : pos + n])
        pos += n
    return partition


def run_client_configs(
    docker_client,
    run_specs: List[ClientRunSpec],
    *,
    cpuset_cpus,
    temporary_dir_client,
    client_mnt_point="/mnt/client/",
    inject_user=True,
    default_timeout=300,
    timeout_buffer=60,
    on_container_started=None,
    raise_on_nonzero=True,
):
    """Launch every :class:`ClientRunSpec` detached, wait for the FOREGROUND
    (measuring) clients, stop BACKGROUND helpers in a ``finally`` (no leak),
    force-remove all, raise if a background helper exited early, then aggregate
    JSON (memtier is the measured base; pubsub/vector merge on top).

    ``on_container_started(container, spec)`` is an optional caller hook (e.g.
    the coordinator's profiler attach). ``inject_user`` adds ``user=uid:gid``
    (runner passes ``True``; coordinator passes ``False`` to preserve current
    behavior). Ports ``runner.py:864-1129``.
    """
    containers = []
    results = []

    # Pin each client container to a DISJOINT slice of the client cpuset so a
    # measuring client (memtier) and a background helper (e.g. bcast-listener) do
    # not contend for the same cores. Falls back to the shared cpuset when a clean
    # split isn't possible (single client, missing cpu requests, doesn't fit).
    cpuset_partition = partition_cpuset(cpuset_cpus, run_specs)
    if cpuset_partition:
        logging.info("multi_tool: cpuset split per client -> %s", cpuset_partition)

    # Start all containers simultaneously (detached)
    for spec in run_specs:
        client_index = spec.client_index
        client_tool = spec.client_tool
        client_image = spec.client_image
        benchmark_command_str = spec.command_str
        spec_cpuset = (
            cpuset_partition.get(client_index, cpuset_cpus)
            if cpuset_partition
            else cpuset_cpus
        )
        try:
            # Calculate container timeout
            container_timeout = default_timeout
            if "test-time" in benchmark_command_str:
                # Handle both --test-time (memtier) and -test-time (pubsub-sub-bench)
                test_time_match = re.search(
                    r"--?test-time[=\s]+(\d+)", benchmark_command_str
                )
                if test_time_match:
                    test_time = int(test_time_match.group(1))
                    container_timeout = test_time + timeout_buffer
                    logging.info(
                        f"Client {client_index}: Set container timeout to "
                        f"{container_timeout}s (test-time: {test_time}s + "
                        f"{timeout_buffer}s buffer)"
                    )

            logging.info(
                f"Starting client {client_index} with docker image {client_image} "
                f"(cpuset={spec_cpuset}) with args: {benchmark_command_str}"
            )

            # Set working directory based on tool
            working_dir = spec.working_dir if spec.working_dir else client_mnt_point

            # Prepare container volumes
            volumes = {
                temporary_dir_client: {
                    "bind": client_mnt_point,
                    "mode": "rw",
                },
            }
            # For vector-db-benchmark, also mount the results directory.
            if spec.extra_volumes:
                for bind_target, mount_opts in spec.extra_volumes.items():
                    volumes[temporary_dir_client] = {
                        "bind": bind_target,
                        "mode": mount_opts.get("mode", "rw"),
                    }

            container_kwargs = {
                "image": client_image,
                "volumes": volumes,
                "auto_remove": False,
                "privileged": True,
                "working_dir": working_dir,
                "command": benchmark_command_str,
                "network_mode": "host",
                "detach": True,
                "cpuset_cpus": spec_cpuset,
            }

            # Only add user for non-vector-db-benchmark tools to avoid
            # permission issues (vector-db sets a working_dir of /app).
            if inject_user and spec.working_dir != "/app":
                container_kwargs["user"] = f"{os.getuid()}:{os.getgid()}"

            # Add environment variables (e.g. vector-db-benchmark).
            if spec.environment:
                container_kwargs["environment"] = spec.environment

            container = docker_client.containers.run(**container_kwargs)

            containers.append(
                {
                    "container": container,
                    "client_index": client_index,
                    "client_tool": client_tool,
                    "client_image": client_image,
                    "benchmark_command_str": benchmark_command_str,
                    "timeout": container_timeout,
                    "output_filename": spec.output_filename,
                    # Background helpers (e.g. bcast-listener) never self-exit;
                    # they are stopped after the measuring clients finish and
                    # are excluded from metrics aggregation.
                    "is_background": spec.is_background,
                }
            )

            if on_container_started is not None:
                on_container_started(container, spec)

        except Exception as e:
            error_msg = f"Error starting client {client_index}: {e}"
            logging.error(error_msg)
            logging.error(f"Image: {client_image}, Tool: {client_tool}")
            logging.error(f"Command: {benchmark_command_str}")
            # A later container failing to start must not leak the ones already
            # started in this loop -- especially long-lived background helpers
            # (bcast-listener) that would otherwise keep tracking connections
            # open until manual cleanup. Force-remove everything started so far.
            remove_started_containers(containers)
            # Fail fast on container startup errors
            raise RuntimeError(f"Failed to start client {client_index}: {e}")

    # Wait for the FOREGROUND (measuring) containers to complete, then stop
    # any BACKGROUND helpers. Background helpers (e.g. bcast-listener) hold
    # their connections open until killed and never self-exit, so they must
    # not be waited on (that would block until the timeout and then fail the
    # whole run); they are stopped once the measuring clients are done.
    foreground = [c for c in containers if not c.get("is_background")]
    background = [c for c in containers if c.get("is_background")]
    logging.info(
        f"Waiting for {len(foreground)} foreground container(s) to complete "
        f"({len(background)} background helper(s) running)..."
    )

    # A background helper that exits on its own BEFORE we stop it means the
    # listeners were not active for the full measured window -> the result is
    # invalid. Tracked here and raised after cleanup.
    background_failures = []

    try:
        for container_info in foreground:
            container = container_info["container"]
            client_index = container_info["client_index"]
            client_tool = container_info["client_tool"]
            client_image = container_info["client_image"]
            benchmark_command_str = container_info["benchmark_command_str"]

            try:
                # Wait for container to complete
                exit_code = container.wait(timeout=container_info["timeout"])
                client_stdout = container.logs().decode("utf-8")

                # Check if container succeeded
                if raise_on_nonzero and exit_code.get("StatusCode", 1) != 0:
                    logging.error(
                        f"Client {client_index} failed with exit code: {exit_code}"
                    )
                    logging.error(f"Client {client_index} stdout/stderr:")
                    logging.error(client_stdout)
                    # Fail fast on container execution errors
                    raise RuntimeError(
                        f"Client {client_index} ({client_tool}) failed with "
                        f"exit code {exit_code}"
                    )

                logging.info(
                    f"Client {client_index} completed successfully with "
                    f"exit code: {exit_code}"
                )

                results.append(
                    {
                        "client_index": client_index,
                        "stdout": client_stdout,
                        "config": run_specs[client_index].__dict__,
                        "tool": client_tool,
                        "image": client_image,
                    }
                )

            except Exception as e:
                # Get logs even if wait failed
                try:
                    client_stdout = container.logs().decode("utf-8")
                    logging.error(f"Client {client_index} logs:")
                    logging.error(client_stdout)
                except Exception:
                    logging.error(f"Could not retrieve logs for client {client_index}")

                raise RuntimeError(f"Client {client_index} ({client_tool}) failed: {e}")

            finally:
                # Clean up container
                try:
                    container.remove(force=True)
                except Exception as cleanup_error:
                    logging.warning(
                        f"Client {client_index} cleanup error: {cleanup_error}"
                    )

    finally:
        # ALWAYS stop + remove background helpers, even if a measuring client
        # raised above, so listener containers never leak. Also detect helpers
        # that exited on their own before we stopped them (auth error, OOM,
        # crash) -- those invalidate the measurement.
        for container_info in background:
            failure = stop_background_helper(container_info)
            if failure is not None:
                background_failures.append(failure)

    # If a background helper died before the benchmark finished, abort rather
    # than report memtier numbers as if the listeners were active. (Only reached
    # when the foreground loop did not already raise.)
    if background_failures:
        details = ", ".join(
            f"client {i} ({t}) status={s} exit_code={c}"
            for i, t, s, c in background_failures
        )
        raise RuntimeError(
            f"Background helper(s) exited before the benchmark finished: {details}. "
            f"Tracking listeners were not active for the full measured window; "
            f"aborting."
        )

    logging.info(f"Successfully completed {len(foreground)} measuring client(s)")

    # Aggregate results by reading JSON output files
    aggregated_stdout = ""
    memtier_output_filename = None
    successful_results = [r for r in results if "error" not in r]

    if successful_results:
        # Try to read and aggregate JSON output files
        aggregated_json = {}
        memtier_json = None
        pubsub_json = None
        vector_json = None

        for result in successful_results:
            client_index = result["client_index"]
            tool = result["tool"]

            # Look for JSON output file
            json_filename = f"benchmark_output_{client_index}.json"
            json_filepath = os.path.join(temporary_dir_client, json_filename)

            if os.path.exists(json_filepath):
                try:
                    with open(json_filepath, "r") as f:
                        client_json = json.load(f)

                    if "memtier_benchmark" in tool:
                        # Store memtier JSON
                        memtier_json = client_json
                        memtier_output_filename = json_filename
                        logging.info(
                            f"Successfully read memtier JSON output from "
                            f"client {client_index}"
                        )
                    elif "pubsub-sub-bench" in tool:
                        # Store pubsub JSON
                        pubsub_json = client_json
                        logging.info(
                            f"Successfully read pubsub-sub-bench JSON output "
                            f"from client {client_index}"
                        )
                    elif "vector-db-benchmark" in tool:
                        # For vector-db-benchmark, look for summary JSON file
                        summary_files = [
                            f
                            for f in os.listdir(temporary_dir_client)
                            if f.endswith("-summary.json")
                        ]
                        if summary_files:
                            summary_filepath = os.path.join(
                                temporary_dir_client, summary_files[0]
                            )
                            try:
                                with open(summary_filepath, "r") as f:
                                    vector_json = json.load(f)
                                logging.info(
                                    f"Successfully read vector-db-benchmark JSON "
                                    f"output from {summary_files[0]}"
                                )
                            except Exception as e:
                                logging.warning(
                                    f"Failed to read vector-db-benchmark JSON "
                                    f"from {summary_files[0]}: {e}"
                                )
                        else:
                            logging.warning(
                                f"No vector-db-benchmark summary JSON file found "
                                f"for client {client_index}"
                            )

                    logging.info(
                        f"Successfully read JSON output from client "
                        f"{client_index} ({tool})"
                    )

                except Exception as e:
                    logging.warning(
                        f"Failed to read JSON from client {client_index}: {e}"
                    )
                    # Fall back to stdout
                    pass
            else:
                logging.warning(
                    f"JSON output file not found for client {client_index}: "
                    f"{json_filepath}"
                )

        # Merge JSON outputs from all tools
        if memtier_json and pubsub_json and vector_json:
            # Use memtier as base and add other metrics
            aggregated_json = memtier_json.copy()
            aggregated_json.update(pubsub_json)
            aggregated_json.update(vector_json)
            aggregated_stdout = json.dumps(aggregated_json, indent=2)
            logging.info(
                "Using merged JSON results from memtier, pubsub-sub-bench, and "
                "vector-db-benchmark clients"
            )
        elif memtier_json and pubsub_json:
            # Use memtier as base and add pubsub metrics
            aggregated_json = memtier_json.copy()
            aggregated_json.update(pubsub_json)
            aggregated_stdout = json.dumps(aggregated_json, indent=2)
            logging.info(
                "Using merged JSON results from memtier and pubsub-sub-bench clients"
            )
        elif memtier_json and vector_json:
            # Use memtier as base and add vector metrics
            aggregated_json = memtier_json.copy()
            aggregated_json.update(vector_json)
            aggregated_stdout = json.dumps(aggregated_json, indent=2)
            logging.info(
                "Using merged JSON results from memtier and vector-db-benchmark "
                "clients"
            )
        elif pubsub_json and vector_json:
            # Use pubsub as base and add vector metrics
            aggregated_json = pubsub_json.copy()
            aggregated_json.update(vector_json)
            aggregated_stdout = json.dumps(aggregated_json, indent=2)
            logging.info(
                "Using merged JSON results from pubsub-sub-bench and "
                "vector-db-benchmark clients"
            )
        elif memtier_json:
            # Only memtier available
            aggregated_json = memtier_json
            aggregated_stdout = json.dumps(aggregated_json, indent=2)
            logging.info("Using JSON results from memtier client only")
        elif pubsub_json:
            # Only pubsub available
            aggregated_json = pubsub_json
            aggregated_stdout = json.dumps(aggregated_json, indent=2)
            logging.info("Using JSON results from pubsub-sub-bench client only")
        elif vector_json:
            # Only vector-db-benchmark available
            aggregated_json = vector_json
            aggregated_stdout = json.dumps(aggregated_json, indent=2)
            logging.info("Using JSON results from vector-db-benchmark client only")
        else:
            # Fall back to concatenated stdout
            aggregated_stdout = "\n".join([r["stdout"] for r in successful_results])
            logging.warning(
                "No JSON results found, falling back to concatenated stdout"
            )

    return MultiToolResult(
        aggregated_stdout=aggregated_stdout,
        results=results,
        memtier_output_filename=memtier_output_filename,
        background_failures=background_failures,
    )
