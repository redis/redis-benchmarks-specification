import json
import logging
import os

import redis
import oyaml as yaml
import csv

from redis_benchmarks_specification.__common__.runner import get_benchmark_specs


# logging settings
logging.basicConfig(
    format="%(asctime)s %(levelname)-4s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


def clean_number(value):
    """Cleans and converts numeric values from CSV, handling B (billion), M (million), K (thousand)."""
    try:
        value = value.replace(",", "").strip()  # Remove commas and spaces

        # Determine the scale factor
        multiplier = 1
        if value.endswith("B"):
            multiplier = 1_000_000_000  # Billion
            value = value[:-1]  # Remove "B"
        elif value.endswith("M"):
            multiplier = 1_000_000  # Million
            value = value[:-1]  # Remove "M"
        elif value.endswith("K"):
            multiplier = 1_000  # Thousand
            value = value[:-1]  # Remove "K"

        return int(float(value) * multiplier)  # Convert to full number
    except ValueError:
        logging.error(f"Skipping invalid count value: {value}")
        return 0  # Default to 0 if invalid


def clean_percentage(value):
    """Parse percentage values like '17.810220866%'"""
    try:
        value = value.replace("%", "").strip()
        return float(value)
    except ValueError:
        logging.error(f"Skipping invalid percentage value: {value}")
        return 0.0


def format_number_with_suffix(value):
    """Format large numbers with B/M/K suffixes for readability"""
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"
    elif value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value / 1_000:.1f}K"
    else:
        return str(value)


def get_arg_value(args, flag, default):
    """Extract integer values safely from CLI arguments"""
    if flag in args:
        try:
            val = (
                args[args.index(flag) + 1].lstrip("=").strip()
            )  # Remove any leading '='
            return int(val)  # Convert to integer safely
        except (IndexError, ValueError):
            logging.error(f"Failed to extract {flag}, using default: {default}")
    return default  # Return default if not found or invalid


def generate_stats_cli_command_logic(args, project_name, project_version):
    logging.info(
        "Using: {project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )
    commands_json_file = os.path.abspath(args.commands_json_file)
    include_modules = args.commandstats_csv_include_modules
    logging.info("Reading commands.json file from {}".format(commands_json_file))
    commands_json = {}
    tracked_commands_json = {}
    groups_json = {}
    total_commands = 0
    total_tracked_commands = 0
    with open(commands_json_file, "r") as groups_json_file_fd:
        commands_json = json.load(groups_json_file_fd)
        total_commands = len(commands_json.keys())
    groups_json_file = os.path.abspath(args.groups_json_file)
    logging.info("Reading groups.json file from {}".format(groups_json_file))
    with open(groups_json_file, "r") as groups_json_file_fd:
        groups_json = json.load(groups_json_file_fd)
    testsuites_folder = os.path.abspath(args.test_suites_folder)
    logging.info("Using test-suites folder dir {}".format(testsuites_folder))
    testsuite_spec_files = get_benchmark_specs(testsuites_folder)
    logging.info(
        "There are a total of {} test-suites being run in folder {}".format(
            len(testsuite_spec_files), testsuites_folder
        )
    )
    priority_json = {}
    if args.commands_priority_file != "":
        with open(args.commands_priority_file, "r") as fd:
            logging.info(
                "Reading {} file with priority by commandstats".format(
                    args.commands_priority_file
                )
            )
            priority_json = json.load(fd)
    tracked_groups = []
    tracked_groups_hist = {}

    # ACL category tracking for benchmark YAML files
    benchmark_read_commands = {}
    benchmark_write_commands = {}
    benchmark_fast_commands = {}
    benchmark_slow_commands = {}
    benchmark_total_command_count = 0

    # Group-based read/write tracking for benchmarks
    benchmark_group_read = {}  # group -> count
    benchmark_group_write = {}  # group -> count
    benchmark_group_total = {}  # group -> total count

    # ACL category tracking for commandstats CSV
    csv_read_commands = {}
    csv_write_commands = {}
    csv_fast_commands = {}
    csv_slow_commands = {}
    csv_total_command_count = 0

    # Group-based read/write tracking for CSV
    csv_group_read = {}  # group -> count
    csv_group_write = {}  # group -> count
    csv_group_total = {}  # group -> total count

    # Percentage validation tracking
    csv_provided_percentages = {}  # command -> provided percentage
    csv_original_counts = {}  # command -> original count from CSV

    override_enabled = args.override_tests
    fail_on_required_diff = args.fail_on_required_diff
    overall_result = True
    test_names = []
    pipelines = {}
    connections = {}
    data_sizes = {}
    defaults_filename = args.defaults_filename

    for test_file in testsuite_spec_files:
        if defaults_filename in test_file:
            continue
        benchmark_config = {}
        requires_override = False
        test_result = True
        tested_groups_match_origin = True

        with open(test_file, "r") as stream:

            try:
                benchmark_config = yaml.safe_load(stream)
                test_name = benchmark_config["name"]
                if test_name in test_names:
                    logging.error(
                        "Duplicate testname detected! {} is already present in {}".format(
                            test_name, test_names
                        )
                    )
                    test_result = False

                # Validate client configuration format
                has_clientconfig = "clientconfig" in benchmark_config
                has_clientconfigs = "clientconfigs" in benchmark_config

                if has_clientconfig and has_clientconfigs:
                    logging.error(
                        "Test {} has both 'clientconfig' and 'clientconfigs'. Only one format is allowed.".format(
                            test_name
                        )
                    )
                    test_result = False
                elif not has_clientconfig and not has_clientconfigs:
                    logging.error(
                        "Test {} is missing client configuration. Must have either 'clientconfig' or 'clientconfigs'.".format(
                            test_name
                        )
                    )
                    test_result = False

                test_names.append(test_name)
                group = ""
                is_memtier = False

                ## defaults
                pipeline_size = 1
                clients = 50
                threads = 4
                data_size = 32

                if "memtier" in test_name:
                    is_memtier = True
                tested_groups = []
                origin_tested_groups = []
                if "tested-groups" in benchmark_config:
                    origin_tested_groups = benchmark_config["tested-groups"]
                origin_tested_commands = []

                tested_commands = []
                if "tested-commands" in benchmark_config:
                    origin_tested_commands = benchmark_config["tested-commands"]
                else:
                    logging.warn("dont have test commands in {}".format(test_name))

                for tested_command in origin_tested_commands:
                    tested_commands.append(tested_command.lower())
                if is_memtier:
                    # Handle both clientconfig and clientconfigs formats
                    if "clientconfigs" in benchmark_config:
                        # For multiple configs, use the first one for stats analysis
                        # TODO: Consider aggregating stats from all configs
                        arguments = benchmark_config["clientconfigs"][0]["arguments"]
                        arg_list = (
                            benchmark_config["clientconfigs"][0]["arguments"]
                            .replace('"', "")
                            .split()
                        )
                    else:
                        # Legacy single clientconfig format
                        arguments = benchmark_config["clientconfig"]["arguments"]
                        arg_list = (
                            benchmark_config["clientconfig"]["arguments"]
                            .replace('"', "")
                            .split()
                        )

                    data_size = get_arg_value(arg_list, "--data-size", data_size)
                    data_size = get_arg_value(arg_list, "-d", data_size)

                    # Extract values using the safer parsing function
                    pipeline_size = get_arg_value(arg_list, "--pipeline", pipeline_size)
                    pipeline_size = get_arg_value(
                        arg_list, "-P", pipeline_size
                    )  # Support short form

                    # Extract values using the safer parsing function
                    clients = get_arg_value(arg_list, "--clients", clients)
                    clients = get_arg_value(
                        arg_list, "-c", clients
                    )  # Support short form

                    threads = get_arg_value(arg_list, "--threads", threads)
                    threads = get_arg_value(
                        arg_list, "-t", threads
                    )  # Support short form

                    arguments_split = arguments.split("--command")

                    if len(arguments_split) == 1:
                        # this means no arbitrary command is being used so we default to memtier default group, which is 'string'
                        tested_groups.append("string")

                    for command_part in arguments_split[1:]:
                        command_part = command_part.strip()
                        command_p = command_part.split(" ", 1)[0]
                        command = command_p.replace(" ", "")
                        command = command.replace("=", "")
                        command = command.replace('"', "")
                        command = command.replace("'", "")
                        if "-key-pattern" in command:
                            continue
                        # Skip command-ratio and other memtier arguments that start with -
                        if command.startswith("-"):
                            continue
                        command = command.lower()
                        if command not in tested_commands:
                            tested_commands.append(command)
                        command_json = {}
                        if command in commands_json:
                            command_json = commands_json[command]
                        elif command.upper() in commands_json:
                            command_json = commands_json[command.upper()]
                        else:
                            logging.error(
                                "command {} not in commands.json".format(command)
                            )
                        if command not in tracked_commands_json:
                            tracked_commands_json[command] = command_json

                        # Only process if command_json has group information
                        if "group" in command_json:
                            group = command_json["group"]
                            if group not in tested_groups:

                                tested_groups.append(group)
                            if group not in tracked_groups:
                                tracked_groups.append(group)
                                tracked_groups_hist[group] = 0
                            tracked_groups_hist[group] = tracked_groups_hist[group] + 1

                            # Track ACL categories for read/write and fast/slow analysis
                            if "acl_categories" in command_json:
                                acl_categories = command_json["acl_categories"]
                                benchmark_total_command_count += 1

                                # Track total by group (all commands)
                                if group not in benchmark_group_total:
                                    benchmark_group_total[group] = 0
                                benchmark_group_total[group] += 1

                                # Track read/write commands
                                is_read = False
                                is_write = False

                                if "@read" in acl_categories:
                                    is_read = True
                                elif "@write" in acl_categories:
                                    is_write = True
                                elif "_ro" in command.lower():
                                    # Commands with _ro suffix are read-only (like EVALSHA_RO)
                                    is_read = True
                                elif "@pubsub" in acl_categories:
                                    # Pubsub commands: SUBSCRIBE/UNSUBSCRIBE are read, PUBLISH is write
                                    if command.lower() in [
                                        "subscribe",
                                        "unsubscribe",
                                        "psubscribe",
                                        "punsubscribe",
                                    ]:
                                        is_read = True
                                    else:
                                        is_write = (
                                            True  # PUBLISH and other pubsub commands
                                        )
                                else:
                                    # Commands without explicit read/write ACL but not _ro are assumed write
                                    # This covers cases like EVALSHA which can modify data
                                    is_write = True

                                if is_read:
                                    if command not in benchmark_read_commands:
                                        benchmark_read_commands[command] = 0
                                    benchmark_read_commands[command] += 1

                                    # Track by group
                                    if group not in benchmark_group_read:
                                        benchmark_group_read[group] = 0
                                    benchmark_group_read[group] += 1

                                elif is_write:
                                    if command not in benchmark_write_commands:
                                        benchmark_write_commands[command] = 0
                                    benchmark_write_commands[command] += 1

                                    # Track by group
                                    if group not in benchmark_group_write:
                                        benchmark_group_write[group] = 0
                                    benchmark_group_write[group] += 1

                                # Track fast/slow commands
                                if "@fast" in acl_categories:
                                    if command not in benchmark_fast_commands:
                                        benchmark_fast_commands[command] = 0
                                    benchmark_fast_commands[command] += 1
                                elif "@slow" in acl_categories:
                                    if command not in benchmark_slow_commands:
                                        benchmark_slow_commands[command] = 0
                                    benchmark_slow_commands[command] += 1

                # Calculate total connections
                total_connections = clients * threads

                if pipeline_size not in pipelines:
                    pipelines[pipeline_size] = 0
                pipelines[pipeline_size] = pipelines[pipeline_size] + 1

                if total_connections not in connections:
                    connections[total_connections] = 0
                connections[total_connections] = connections[total_connections] + 1

                if data_size not in data_sizes:
                    data_sizes[data_size] = 0
                data_sizes[data_size] = data_sizes[data_size] + 1

                if sorted(tested_commands) != sorted(origin_tested_commands):
                    requires_override = True
                    benchmark_config["tested-commands"] = tested_commands
                    logging.warn(
                        "there is a difference between specified test-commands in the yaml (name={}) and the ones we've detected {}!={}".format(
                            test_name,
                            sorted(origin_tested_commands),
                            sorted(tested_commands),
                        )
                    )

                priority = None
                # maximum priority of all tested commands
                priority_json_value = None
                for command in tested_commands:
                    if command in priority_json:
                        priority_v = priority_json[command]
                        if priority_json_value is None:
                            priority_json_value = priority_v
                        if priority_v > priority_json_value:
                            priority_json_value = priority_v

                if "priority" in benchmark_config:
                    priority = benchmark_config["priority"]
                else:
                    if priority_json_value is not None:
                        requires_override = True
                        logging.warn(
                            "dont have priority in {}, but the commands in the test have max priority of {}".format(
                                test_name, priority_json_value
                            )
                        )
                        priority = priority_json_value
                if priority is not None:
                    benchmark_config["priority"] = priority

                resources = {}
                if "resources" in benchmark_config["dbconfig"]:
                    resources = benchmark_config["dbconfig"]["resources"]
                else:
                    benchmark_config["dbconfig"]["resources"] = resources

                resources_requests = {}
                if "requests" in resources:
                    resources_requests = benchmark_config["dbconfig"]["resources"][
                        "requests"
                    ]
                else:
                    benchmark_config["dbconfig"]["resources"][
                        "requests"
                    ] = resources_requests

                if "memory" not in resources_requests:
                    benchmark_config["dbconfig"]["resources"]["requests"][
                        "memory"
                    ] = "1g"
                    requires_override = True
                    logging.warn(
                        "dont have resources.requests.memory in {}. Setting 1GB default".format(
                            test_name
                        )
                    )

                if sorted(tested_groups) != sorted(origin_tested_groups):
                    tested_groups_match_origin = False
                    benchmark_config["tested-groups"] = tested_groups
                    logging.warn(
                        "there is a difference between specified test-groups in the yaml (name={}) and the ones we've detected {}!={}".format(
                            test_name,
                            sorted(origin_tested_groups),
                            sorted(tested_groups),
                        )
                    )

            except Exception as e:
                logging.error(
                    "while loading file {} and error was returned: {}".format(
                        test_file, e.__str__()
                    )
                )
                test_result = False
                pass

        if requires_override:
            test_result = False
        overall_result &= test_result

        if not tested_groups_match_origin:
            if len(tested_groups) > 0:
                overall_result = False
            else:
                logging.warn(
                    "difference between specified and detected test-groups was ignored since command info is not available in this benchmark version"
                )

        if (requires_override or not tested_groups_match_origin) and override_enabled:
            logging.info(
                "Saving a new version of the file {} with the overrided data".format(
                    test_file
                )
            )
            with open(test_file, "w") as file:
                yaml.dump(benchmark_config, file, sort_keys=False, width=100000)
    total_tracked_commands_pct = "n/a"

    module_names = {
        "ft": "redisearch",
        "search": "redisearch",
        "_ft": "redisearch",
        "graph": "redisgraph",
        "ts": "redistimeseries",
        "timeseries": "redistimeseries",
        "json": "redisjson",
        "bf": "redisbloom",
        "cf": "redisbloom",
        "topk": "redisbloom",
        "cms": "redisbloom",
        "tdigest": "redisbloom",
    }

    group_usage_calls = {}
    group_usage_usecs = {}

    if args.commandstats_csv != "":
        logging.info(
            "Reading commandstats csv {} to determine commands/test coverage".format(
                args.commandstats_csv
            )
        )
        from csv import reader

        rows = []
        priority = {}
        priority_usecs = {}

        # open file in read mode
        total_count = 0
        total_usecs = 0
        total_tracked_count = 0
        with open(
            args.commandstats_csv, "r", encoding="utf8", errors="ignore"
        ) as read_obj:
            # pass the file object to reader() to get the reader object
            csv_reader = reader(x.replace("\0", "") for x in read_obj)
            # Iterate over each row in the csv using reader object
            for row in csv_reader:
                if len(row) <= 2:
                    continue
                if "cmdstat_" not in row[0]:
                    continue
                # row variable is a list that represents a row in csv
                cmdstat = row[0]
                cmdstat = cmdstat.lower()
                if "cmdstat_" not in cmdstat:
                    continue
                cmdstat = cmdstat.replace("cmdstat_", "")
                count = clean_number(row[1])
                usecs = None
                if len(row) > 2:
                    usecs = clean_number(row[2])
                    total_usecs += usecs

                # Parse percentage and original count if available
                provided_percentage = None
                original_count = None
                if len(row) > 3:
                    provided_percentage = clean_percentage(row[3])
                if len(row) > 4:
                    original_count = clean_number(row[4])

                if count == 0:
                    continue
                tracked = False
                module = False
                cmd = cmdstat.upper()
                group = "n/a"
                deprecated = False
                if "." in cmdstat:
                    module = True
                    cmd_module_prefix = cmdstat.split(".")[0]
                    if cmd_module_prefix in module_names:
                        group = module_names[cmd_module_prefix]
                    else:
                        logging.error(
                            "command with a module prefix does not have module name {}".format(
                                cmd_module_prefix
                            )
                        )
                if cmd in commands_json:
                    command_json = commands_json[cmd]
                    group = command_json["group"]
                    if "deprecated_since" in command_json:
                        deprecated = True

                    # Track ACL categories for commandstats CSV data
                    if "acl_categories" in command_json:
                        acl_categories = command_json["acl_categories"]

                        # Use original count if available, otherwise use parsed count
                        tracking_count = (
                            original_count if original_count is not None else count
                        )
                        csv_total_command_count += tracking_count

                        # Track total by group (all commands)
                        if group not in csv_group_total:
                            csv_group_total[group] = 0
                        csv_group_total[group] += tracking_count

                        # Track read/write commands
                        is_read = False
                        is_write = False

                        if "@read" in acl_categories:
                            is_read = True
                        elif "@write" in acl_categories:
                            is_write = True
                        elif "_ro" in cmd.lower():
                            # Commands with _ro suffix are read-only (like EVALSHA_RO)
                            is_read = True
                        elif "@pubsub" in acl_categories:
                            # Pubsub commands: SUBSCRIBE/UNSUBSCRIBE are read, PUBLISH is write
                            if cmd.lower() in [
                                "subscribe",
                                "unsubscribe",
                                "psubscribe",
                                "punsubscribe",
                            ]:
                                is_read = True
                            else:
                                is_write = True  # PUBLISH and other pubsub commands
                        else:
                            # Commands without explicit read/write ACL but not _ro are assumed write
                            # This covers cases like EVALSHA which can modify data
                            is_write = True

                        if is_read:
                            if cmd.lower() not in csv_read_commands:
                                csv_read_commands[cmd.lower()] = 0
                            csv_read_commands[cmd.lower()] += tracking_count

                            # Track by group
                            if group not in csv_group_read:
                                csv_group_read[group] = 0
                            csv_group_read[group] += tracking_count

                        elif is_write:
                            if cmd.lower() not in csv_write_commands:
                                csv_write_commands[cmd.lower()] = 0
                            csv_write_commands[cmd.lower()] += tracking_count

                            # Track by group
                            if group not in csv_group_write:
                                csv_group_write[group] = 0
                            csv_group_write[group] += tracking_count

                        # Track fast/slow commands
                        if "@fast" in acl_categories:
                            if cmd.lower() not in csv_fast_commands:
                                csv_fast_commands[cmd.lower()] = 0
                            csv_fast_commands[cmd.lower()] += tracking_count
                        elif "@slow" in acl_categories:
                            if cmd.lower() not in csv_slow_commands:
                                csv_slow_commands[cmd.lower()] = 0
                            csv_slow_commands[cmd.lower()] += tracking_count

                if module is False or include_modules:
                    # Use original count if available and different from parsed count
                    final_count = count
                    if original_count is not None and original_count != count:
                        logging.warning(
                            f"Using original count for {cmd}: {original_count:,} instead of parsed {count:,}"
                        )
                        final_count = original_count

                    priority[cmd.lower()] = final_count
                    if type(usecs) == int:
                        priority_usecs[cmd.lower()] = usecs

                    # Store percentage and original count for validation
                    if provided_percentage is not None:
                        csv_provided_percentages[cmd.lower()] = provided_percentage
                    if original_count is not None:
                        csv_original_counts[cmd.lower()] = original_count

                if cmdstat in tracked_commands_json:
                    tracked = True
                if module is False or include_modules:
                    row = [cmdstat, group, count, usecs, tracked, deprecated]
                    rows.append(row)
                if group not in group_usage_calls:
                    group_usage_calls[group] = {}
                    group_usage_calls[group]["call"] = 0
                if group not in group_usage_usecs:
                    group_usage_usecs[group] = {}
                    group_usage_usecs[group]["usecs"] = 0
                if type(count) == int:
                    group_usage_calls[group]["call"] = (
                        group_usage_calls[group]["call"] + count
                    )
                if type(usecs) == int:
                    group_usage_usecs[group]["usecs"] = (
                        group_usage_usecs[group]["usecs"] + usecs
                    )
                if group == "n/a":
                    logging.warn("Unable to detect group in {}".format(cmd))

        priority_list = sorted(((priority[cmd], cmd) for cmd in priority), reverse=True)

        priority_json = {}
        top_10_missing = []
        top_30_missing = []
        top_50_missing = []
        # first pass on count
        for x in priority_list:
            count = x[0]
            total_count += count

        for group_name, group in group_usage_calls.items():
            call = group["call"]
            pct = call / total_count
            group["pct"] = pct

        for group_name, group in group_usage_usecs.items():
            usecs = group["usecs"]
            pct = usecs / total_usecs
            group["pct"] = pct

        for pos, x in enumerate(priority_list, 1):
            count = x[0]
            cmd = x[1]
            priority_json[cmd] = pos
            pct = count / total_count
            if cmd not in tracked_commands_json:
                if pos <= 10:
                    top_10_missing.append(cmd)
                if pos <= 30:
                    top_30_missing.append(cmd)
                if pos <= 50:
                    top_50_missing.append(cmd)
            else:
                total_tracked_count += count

        if args.commands_priority_file != "":
            with open(args.commands_priority_file, "w") as fd:
                logging.info(
                    "Updating {} file with priority by commandstats".format(
                        args.commands_priority_file
                    )
                )
                json.dump(priority_json, fd, indent=True)

        if args.group_csv != "":
            header = [
                "group",
                "count",
                "usecs",
                "usec_per_call",
                "% count",
                "% usecs",
            ]
            with open(args.group_csv, "w", encoding="UTF8", newline="") as f:
                writer = csv.writer(f)

                # write the header
                writer.writerow(header)
                for group_name, group_usage_info in group_usage_calls.items():
                    count = group_usage_info["call"]
                    call_pct = group_usage_info["pct"]
                    usecs = group_usage_usecs[group_name]["usecs"]
                    usecs_pct = group_usage_usecs[group_name]["pct"]
                    usecs_per_call = usecs / count

                    writer.writerow(
                        [group_name, count, usecs, usecs_per_call, call_pct, usecs_pct]
                    )

        if args.summary_csv != "":
            header = [
                "command",
                "group",
                "count",
                "usecs",
                "tracked",
                "deprecated",
                "usec_per_call",
                "% count",
                "% usecs",
                "diff count usecs",
            ]

            with open(args.summary_csv, "w", encoding="UTF8", newline="") as f:
                writer = csv.writer(f)

                # write the header
                writer.writerow(header)
                for row in rows:
                    # write the data
                    count = row[2]
                    usec = row[3]
                    pct = count / total_count
                    pct_usec = "n/a"
                    usec_per_call = "n/a"
                    diff_pct = "n/a"
                    if usec is not None:
                        pct_usec = usec / total_usecs
                        usec_per_call = float(usec) / float(count)
                        diff_pct = pct_usec - pct
                    row.append(usec_per_call)
                    row.append(pct)
                    row.append(pct_usec)
                    row.append(diff_pct)
                    writer.writerow(row)

        if total_tracked_count > 0:
            total_tracked_commands_pct = "{0:.3g} %".format(
                total_tracked_count / total_count * 100.0
            )

        logging.info("Total commands: {}".format(total_commands))
        total_tracked_commands = len(tracked_commands_json.keys())
        logging.info("Total tracked commands: {}".format(total_tracked_commands))
        logging.info(
            "Total tracked commands pct: {}".format(total_tracked_commands_pct)
        )
        all_groups = groups_json.keys()
        total_groups = len(all_groups)
        logging.info("Total groups: {}".format(total_groups))
        total_tracked_groups = len(tracked_groups)
        logging.info("Total tracked groups: {}".format(total_tracked_groups))
        logging.info(
            "Total untracked groups: {}".format(total_groups - total_tracked_groups)
        )
        logging.info("Printing untracked groups:")
        for group_name in all_groups:
            if group_name not in tracked_groups:
                logging.info("                         - {}".format(group_name))
        logging.info("Top 10 fully tracked?: {}".format(len(top_10_missing) == 0))
        logging.info("Top 30 fully tracked?: {}".format(len(top_30_missing) == 0))
        if len(top_30_missing) > 0:
            logging.info(
                f"\t\tTotal missing for Top 30: {len(top_30_missing)}. {top_30_missing}"
            )

        logging.info("Top 50 fully tracked?: {}".format(len(top_50_missing) == 0))
        if len(top_50_missing) > 0:
            logging.info(
                f"\t\tTotal missing for Top 50: {len(top_50_missing)}. {top_50_missing}"
            )

    if overall_result is False and fail_on_required_diff:
        logging.error(
            "Failing given there were changes required to be made and --fail-on-required-diff was enabled"
        )
        exit(1)

    if args.push_stats_redis:
        logging.info(
            "Pushing stats to redis at: {}:{}".format(args.redis_host, args.redis_port)
        )
        conn = redis.StrictRedis(
            host=args.redis_host,
            port=args.redis_port,
            password=args.redis_pass,
            username=args.redis_user,
            decode_responses=False,
        )

        tested_groups_key = "gh/redis/redis:set:tested_groups"
        tested_commands_key = "gh/redis/redis:set:tested_commands"
        for group in tracked_groups:
            conn.sadd(tested_groups_key, group)
        for command in list(tracked_commands_json.keys()):
            conn.sadd(tested_commands_key, command)

    logging.info(f"There is a total of : {len(tracked_groups)} tracked command groups.")
    logging.info(
        f"There is a total of : {len(list(tracked_commands_json.keys()))} tracked commands."
    )

    # ACL Category Analysis Summary
    logging.info("=" * 80)
    logging.info("ACL CATEGORY ANALYSIS SUMMARY")
    logging.info("=" * 80)

    # Benchmark YAML files analysis
    if benchmark_total_command_count > 0:
        logging.info("BENCHMARK TEST SUITES ANALYSIS (from YAML files):")
        logging.info("-" * 50)

        # Calculate read/write percentages for benchmarks
        benchmark_read_count = sum(benchmark_read_commands.values())
        benchmark_write_count = sum(benchmark_write_commands.values())
        benchmark_rw_count = benchmark_read_count + benchmark_write_count

        if benchmark_rw_count > 0:
            read_percentage = (benchmark_read_count / benchmark_rw_count) * 100
            write_percentage = (benchmark_write_count / benchmark_rw_count) * 100

            logging.info(f"READ/WRITE COMMAND DISTRIBUTION:")
            logging.info(
                f"  Read commands:  {benchmark_read_count:6d} ({read_percentage:5.1f}%)"
            )
            logging.info(
                f"  Write commands: {benchmark_write_count:6d} ({write_percentage:5.1f}%)"
            )
            logging.info(f"  Total R/W:      {benchmark_rw_count:6d} (100.0%)")
        else:
            logging.info("No read/write commands detected in benchmark ACL categories")

        # Calculate fast/slow percentages for benchmarks
        benchmark_fast_count = sum(benchmark_fast_commands.values())
        benchmark_slow_count = sum(benchmark_slow_commands.values())
        benchmark_fs_count = benchmark_fast_count + benchmark_slow_count

        if benchmark_fs_count > 0:
            fast_percentage = (benchmark_fast_count / benchmark_fs_count) * 100
            slow_percentage = (benchmark_slow_count / benchmark_fs_count) * 100

            logging.info(f"")
            logging.info(f"FAST/SLOW COMMAND DISTRIBUTION:")
            logging.info(
                f"  Fast commands: {benchmark_fast_count:6d} ({fast_percentage:5.1f}%)"
            )
            logging.info(
                f"  Slow commands: {benchmark_slow_count:6d} ({slow_percentage:5.1f}%)"
            )
            logging.info(f"  Total F/S:     {benchmark_fs_count:6d} (100.0%)")
        else:
            logging.info("No fast/slow commands detected in benchmark ACL categories")

        # Group breakdown for benchmarks
        if benchmark_group_total:
            logging.info("")
            logging.info("READ/WRITE BREAKDOWN BY COMMAND GROUP:")

            # Calculate total calls across all groups
            total_all_calls = sum(benchmark_group_total.values())

            # Create list of groups with their total calls for sorting
            group_data = []
            for group, total_group in benchmark_group_total.items():
                read_count = benchmark_group_read.get(group, 0)
                write_count = benchmark_group_write.get(group, 0)
                group_data.append((group, read_count, write_count, total_group))

            # Sort by total calls (descending)
            group_data.sort(key=lambda x: x[3], reverse=True)

            total_read_all = 0
            total_write_all = 0

            for group, read_count, write_count, total_group in group_data:
                group_pct = (total_group / total_all_calls) * 100
                read_pct = (read_count / total_group) * 100 if total_group > 0 else 0
                write_pct = (write_count / total_group) * 100 if total_group > 0 else 0

                read_formatted = format_number_with_suffix(read_count)
                write_formatted = format_number_with_suffix(write_count)

                logging.info(
                    f"  {group.upper():>12} ({group_pct:4.1f}%): {read_formatted:>8} read ({read_pct:5.1f}%), {write_formatted:>8} write ({write_pct:5.1f}%)"
                )

                total_read_all += read_count
                total_write_all += write_count

            # Add total row
            if group_data:
                total_read_pct = (total_read_all / total_all_calls) * 100
                total_write_pct = (total_write_all / total_all_calls) * 100
                total_read_formatted = format_number_with_suffix(total_read_all)
                total_write_formatted = format_number_with_suffix(total_write_all)

                logging.info(
                    f"  {'TOTAL':>12} (100.0%): {total_read_formatted:>8} read ({total_read_pct:5.1f}%), {total_write_formatted:>8} write ({total_write_pct:5.1f}%)"
                )
    else:
        logging.info(
            "BENCHMARK TEST SUITES ANALYSIS: No commands with ACL categories found"
        )

    # CommandStats CSV analysis
    if csv_total_command_count > 0:
        logging.info("")
        logging.info("COMMANDSTATS CSV ANALYSIS (actual Redis usage):")
        logging.info("-" * 50)

        # Calculate read/write percentages for CSV data
        csv_read_count = sum(csv_read_commands.values())
        csv_write_count = sum(csv_write_commands.values())
        csv_rw_count = csv_read_count + csv_write_count

        if csv_rw_count > 0:
            read_percentage = (csv_read_count / csv_rw_count) * 100
            write_percentage = (csv_write_count / csv_rw_count) * 100

            logging.info(f"READ/WRITE COMMAND DISTRIBUTION:")
            logging.info(
                f"  Read commands:  {csv_read_count:8d} ({read_percentage:5.1f}%)"
            )
            logging.info(
                f"  Write commands: {csv_write_count:8d} ({write_percentage:5.1f}%)"
            )
            logging.info(f"  Total R/W:      {csv_rw_count:8d} (100.0%)")
        else:
            logging.info("No read/write commands detected in CSV ACL categories")

        # Calculate fast/slow percentages for CSV data
        csv_fast_count = sum(csv_fast_commands.values())
        csv_slow_count = sum(csv_slow_commands.values())
        csv_fs_count = csv_fast_count + csv_slow_count

        if csv_fs_count > 0:
            fast_percentage = (csv_fast_count / csv_fs_count) * 100
            slow_percentage = (csv_slow_count / csv_fs_count) * 100

            logging.info(f"")
            logging.info(f"FAST/SLOW COMMAND DISTRIBUTION:")
            logging.info(
                f"  Fast commands: {csv_fast_count:8d} ({fast_percentage:5.1f}%)"
            )
            logging.info(
                f"  Slow commands: {csv_slow_count:8d} ({slow_percentage:5.1f}%)"
            )
            logging.info(f"  Total F/S:     {csv_fs_count:8d} (100.0%)")
        else:
            logging.info("No fast/slow commands detected in CSV ACL categories")

        # Group breakdown for CSV data
        if csv_group_total:
            logging.info("")
            logging.info("READ/WRITE BREAKDOWN BY COMMAND GROUP:")

            # Calculate total calls across all groups
            total_all_calls = sum(csv_group_total.values())

            # Create list of groups with their total calls for sorting
            group_data = []
            for group, total_group in csv_group_total.items():
                read_count = csv_group_read.get(group, 0)
                write_count = csv_group_write.get(group, 0)
                group_data.append((group, read_count, write_count, total_group))

            # Sort by total calls (descending)
            group_data.sort(key=lambda x: x[3], reverse=True)

            total_read_all = 0
            total_write_all = 0

            for group, read_count, write_count, total_group in group_data:
                group_pct = (total_group / total_all_calls) * 100
                read_pct = (read_count / total_group) * 100 if total_group > 0 else 0
                write_pct = (write_count / total_group) * 100 if total_group > 0 else 0

                read_formatted = format_number_with_suffix(read_count)
                write_formatted = format_number_with_suffix(write_count)

                logging.info(
                    f"  {group.upper():>12} ({group_pct:4.1f}%): {read_formatted:>8} read ({read_pct:5.1f}%), {write_formatted:>8} write ({write_pct:5.1f}%)"
                )

                total_read_all += read_count
                total_write_all += write_count

            # Add total row
            if group_data:
                total_read_pct = (total_read_all / total_all_calls) * 100
                total_write_pct = (total_write_all / total_all_calls) * 100
                total_read_formatted = format_number_with_suffix(total_read_all)
                total_write_formatted = format_number_with_suffix(total_write_all)

                logging.info(
                    f"  {'TOTAL':>12} (100.0%): {total_read_formatted:>8} read ({total_read_pct:5.1f}%), {total_write_formatted:>8} write ({total_write_pct:5.1f}%)"
                )

        # Validate parsing accuracy by comparing with provided percentages
        if csv_provided_percentages and csv_original_counts:
            logging.info("")
            logging.info("PARSING VALIDATION:")
            logging.info("-" * 30)

            # Calculate total from original counts
            total_original = sum(csv_original_counts.values())
            total_provided_percentage = sum(csv_provided_percentages.values())

            logging.info(f"Total original count: {total_original:,}")
            logging.info(
                f"Sum of provided percentages: {total_provided_percentage:.6f}%"
            )

            # Check if our billion parsing matches original counts
            parsing_errors = 0
            for cmd in csv_original_counts:
                if cmd in priority:  # priority contains our parsed values
                    parsed_value = priority[cmd]
                    original_value = csv_original_counts[cmd]
                    if parsed_value != original_value:
                        parsing_errors += 1
                        logging.warning(
                            f"Parsing mismatch for {cmd}: parsed={parsed_value:,} vs original={original_value:,}"
                        )

            if parsing_errors == 0:
                logging.info("✓ All billion/million/thousand parsing is accurate")
            else:
                logging.warning(f"✗ Found {parsing_errors} parsing errors")

            # Validate percentage calculation
            if abs(total_provided_percentage - 100.0) < 0.001:
                logging.info("✓ Provided percentages sum to 100%")
            else:
                logging.warning(
                    f"✗ Provided percentages sum to {total_provided_percentage:.6f}% (not 100%)"
                )
    else:
        logging.info("")
        logging.info(
            "COMMANDSTATS CSV ANALYSIS: No CSV file provided or no commands found"
        )

    logging.info("=" * 80)
    # Save pipeline count to CSV
    csv_filename = "memtier_pipeline_count.csv"
    with open(csv_filename, "w", newline="") as csvfile:
        fieldnames = ["pipeline", "count"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for pipeline_size in sorted(pipelines.keys()):
            writer.writerow(
                {"pipeline": pipeline_size, "count": pipelines[pipeline_size]}
            )

    logging.info(f"Pipeline count data saved to {csv_filename}")

    csv_filename = "memtier_connection_count.csv"
    with open(csv_filename, "w", newline="") as csvfile:
        fieldnames = ["connections", "count"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Sort connections dictionary by keys before writing
        for connection_count in sorted(connections.keys()):
            writer.writerow(
                {
                    "connections": connection_count,
                    "count": connections[connection_count],
                }
            )

    logging.info(f"Sorted connection count data saved to {csv_filename}")

    csv_filename = "memtier_data_size_histogram.csv"
    with open(csv_filename, "w", newline="") as csvfile:
        fieldnames = ["data_size", "count"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Sort connections dictionary by keys before writing
        for data_size in sorted(data_sizes.keys()):
            writer.writerow(
                {
                    "data_size": data_size,
                    "count": data_sizes[data_size],
                }
            )

    logging.info(f"Sorted data size count data saved to {csv_filename}")

    csv_filename = "memtier_groups_histogram.csv"
    with open(csv_filename, "w", newline="") as csvfile:
        fieldnames = ["group", "count"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Sort connections dictionary by keys before writing
        for group in sorted(tracked_groups_hist.keys()):
            writer.writerow(
                {
                    "group": group,
                    "count": tracked_groups_hist[group],
                }
            )

    logging.info(f"Sorted command groups count data saved to {csv_filename}")

    # Save ACL category data to CSV files

    # Benchmark data CSV files
    csv_filename = "benchmark_acl_read_write_commands.csv"
    with open(csv_filename, "w", newline="") as csvfile:
        fieldnames = ["command", "type", "count"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for command, count in sorted(benchmark_read_commands.items()):
            writer.writerow({"command": command, "type": "read", "count": count})
        for command, count in sorted(benchmark_write_commands.items()):
            writer.writerow({"command": command, "type": "write", "count": count})

    logging.info(f"Benchmark ACL read/write commands data saved to {csv_filename}")

    csv_filename = "benchmark_acl_fast_slow_commands.csv"
    with open(csv_filename, "w", newline="") as csvfile:
        fieldnames = ["command", "type", "count"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for command, count in sorted(benchmark_fast_commands.items()):
            writer.writerow({"command": command, "type": "fast", "count": count})
        for command, count in sorted(benchmark_slow_commands.items()):
            writer.writerow({"command": command, "type": "slow", "count": count})

    logging.info(f"Benchmark ACL fast/slow commands data saved to {csv_filename}")

    # CommandStats CSV data files
    csv_filename = "commandstats_acl_read_write_commands.csv"
    with open(csv_filename, "w", newline="") as csvfile:
        fieldnames = ["command", "type", "count"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for command, count in sorted(csv_read_commands.items()):
            writer.writerow({"command": command, "type": "read", "count": count})
        for command, count in sorted(csv_write_commands.items()):
            writer.writerow({"command": command, "type": "write", "count": count})

    logging.info(f"CommandStats ACL read/write commands data saved to {csv_filename}")

    csv_filename = "commandstats_acl_fast_slow_commands.csv"
    with open(csv_filename, "w", newline="") as csvfile:
        fieldnames = ["command", "type", "count"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for command, count in sorted(csv_fast_commands.items()):
            writer.writerow({"command": command, "type": "fast", "count": count})
        for command, count in sorted(csv_slow_commands.items()):
            writer.writerow({"command": command, "type": "slow", "count": count})

    logging.info(f"CommandStats ACL fast/slow commands data saved to {csv_filename}")

    # Save group breakdown data to CSV files

    # Benchmark group breakdown
    csv_filename = "benchmark_group_read_write_breakdown.csv"
    with open(csv_filename, "w", newline="") as csvfile:
        fieldnames = [
            "group",
            "read_count",
            "write_count",
            "total_count",
            "read_percentage",
            "write_percentage",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        all_groups = set(benchmark_group_read.keys()) | set(
            benchmark_group_write.keys()
        )
        for group in sorted(all_groups):
            read_count = benchmark_group_read.get(group, 0)
            write_count = benchmark_group_write.get(group, 0)
            total_count = read_count + write_count
            read_pct = (read_count / total_count * 100) if total_count > 0 else 0
            write_pct = (write_count / total_count * 100) if total_count > 0 else 0

            writer.writerow(
                {
                    "group": group,
                    "read_count": read_count,
                    "write_count": write_count,
                    "total_count": total_count,
                    "read_percentage": round(read_pct, 2),
                    "write_percentage": round(write_pct, 2),
                }
            )

    logging.info(f"Benchmark group read/write breakdown saved to {csv_filename}")

    # CommandStats group breakdown
    csv_filename = "commandstats_group_read_write_breakdown.csv"
    with open(csv_filename, "w", newline="") as csvfile:
        fieldnames = [
            "group",
            "read_count",
            "write_count",
            "total_count",
            "read_percentage",
            "write_percentage",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        all_groups = set(csv_group_read.keys()) | set(csv_group_write.keys())
        for group in sorted(all_groups):
            read_count = csv_group_read.get(group, 0)
            write_count = csv_group_write.get(group, 0)
            total_count = read_count + write_count
            read_pct = (read_count / total_count * 100) if total_count > 0 else 0
            write_pct = (write_count / total_count * 100) if total_count > 0 else 0

            writer.writerow(
                {
                    "group": group,
                    "read_count": read_count,
                    "write_count": write_count,
                    "total_count": total_count,
                    "read_percentage": round(read_pct, 2),
                    "write_percentage": round(write_pct, 2),
                }
            )

    logging.info(f"CommandStats group read/write breakdown saved to {csv_filename}")
