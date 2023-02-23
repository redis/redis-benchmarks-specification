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
    override_enabled = args.override_tests
    fail_on_required_diff = args.fail_on_required_diff
    overall_result = True
    test_names = []
    defaults_filename = args.defaults_filename

    for test_file in testsuite_spec_files:
        if defaults_filename in test_file:
            continue
        benchmark_config = {}
        requires_override = False
        test_result = True
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

                test_names.append(test_name)
                group = ""
                is_memtier = False
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
                    arguments = benchmark_config["clientconfig"]["arguments"]
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

                        group = command_json["group"]
                        if group not in tested_groups:
                            tested_groups.append(group)
                        if group not in tracked_groups:
                            tracked_groups.append(group)

                if tested_commands != origin_tested_commands:
                    requires_override = True
                    benchmark_config["tested-commands"] = tested_commands
                    logging.warn(
                        "there is a difference between specified test-commands in the yaml (name={}) and the ones we've detected {}!={}".format(
                            test_name, origin_tested_commands, tested_commands
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

                if tested_groups != origin_tested_groups:
                    requires_override = True
                    benchmark_config["tested-groups"] = tested_groups
                    logging.warn(
                        "there is a difference between specified test-groups in the yaml (name={}) and the ones we've detected {}!={}".format(
                            test_name, origin_tested_groups, tested_groups
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

        if requires_override and override_enabled:
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
                count = int(row[1])
                usecs = None
                if len(row) > 2:
                    usecs = int(row[2])
                    total_usecs += usecs
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

                if module is False or include_modules:
                    priority[cmd.lower()] = count
                    if type(usecs) == int:
                        priority_usecs[cmd.lower()] = usecs

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
            logging.info("\t\tTotal missing for Top 30: {}".format(len(top_30_missing)))

        logging.info("Top 50 fully tracked?: {}".format(len(top_50_missing) == 0))
        if len(top_50_missing) > 0:
            logging.info("\t\tTotal missing for Top 50: {}".format(len(top_50_missing)))

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
