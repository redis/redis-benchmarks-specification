import json
import logging
import os

import redis
import oyaml as yaml

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
    tracked_groups = []
    override_enabled = args.override_tests
    fail_on_required_diff = args.fail_on_required_diff
    overall_result = True
    for test_file in testsuite_spec_files:
        benchmark_config = {}
        requires_override = False
        test_result = True
        with open(test_file, "r") as stream:

            try:
                benchmark_config = yaml.safe_load(stream)
                test_name = benchmark_config["name"]
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

    logging.info("Total commands: {}".format(total_commands))
    total_tracked_commands = len(tracked_commands_json.keys())
    logging.info("Total tracked commands: {}".format(total_tracked_commands))

    total_groups = len(groups_json.keys())
    logging.info("Total groups: {}".format(total_groups))
    total_tracked_groups = len(tracked_groups)
    logging.info("Total tracked groups: {}".format(total_tracked_groups))

    if overall_result is False and fail_on_required_diff:
        logging.error(
            "Failing given there were changes required to be made and --fail-on-required-diff was enabled"
        )
        exit(1)

    if args.commandstats_csv != "":
        logging.info(
            "Reading commandstats csv {} to determine commands/test coverage".format(
                args.commandstats_csv
            )
        )
        from csv import reader

        rows = []

        # open file in read mode
        with open(
            args.commandstats_csv, "r", encoding="utf8", errors="ignore"
        ) as read_obj:
            # pass the file object to reader() to get the reader object
            csv_reader = reader(x.replace("\0", "") for x in read_obj)
            # Iterate over each row in the csv using reader object
            for row in csv_reader:
                if len(row) == 0:
                    continue
                # row variable is a list that represents a row in csv
                cmdstat = row[0]
                cmdstat = cmdstat.replace("cmdstat_", "")
                count = int(row[1])
                if count == 0:
                    continue
                tracked = False
                module = False
                cmd = cmdstat.upper()
                group = "n/a"
                deprecated = False
                if "." in cmdstat:
                    module = True
                if cmd in commands_json:
                    command_json = commands_json[cmd]
                    group = command_json["group"]
                    if "deprecated_since" in command_json:
                        deprecated = True

                if cmdstat in tracked_commands_json:
                    tracked = True
                if module is False:
                    row = [cmdstat, group, count, tracked, deprecated]
                    rows.append(row)

        if args.summary_csv != "":
            header = ["command", "group", "count", "tracked", "deprecated"]
            import csv

            with open(args.summary_csv, "w", encoding="UTF8", newline="") as f:
                writer = csv.writer(f)

                # write the header
                writer.writerow(header)
                for row in rows:
                    # write the data
                    writer.writerow(row)
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
