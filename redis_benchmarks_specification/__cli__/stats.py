import json
import logging
import os

import redis
import oyaml as yaml

from redis_benchmarks_specification.__common__.runner import get_benchmark_specs
from redisbench_admin.utils.benchmark_config import (
    get_final_benchmark_config,
)

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
    groups_json = {}
    with open(commands_json_file,"r") as groups_json_file_fd:
        commands_json = json.load(groups_json_file_fd)
    groups_json_file = os.path.abspath(args.groups_json_file)
    logging.info("Reading groups.json file from {}".format(groups_json_file))
    with open(groups_json_file,"r") as groups_json_file_fd:
        groups_json = json.load(groups_json_file_fd)
    testsuites_folder = os.path.abspath(args.test_suites_folder)
    logging.info("Using test-suites folder dir {}".format(testsuites_folder))
    testsuite_spec_files = get_benchmark_specs(testsuites_folder)
    logging.info(
        "There are a total of {} test-suites in folder {}".format(
            len(testsuite_spec_files), testsuites_folder
        )
    )
    for test_file in testsuite_spec_files:
        benchmark_config = {}
        requires_override = False
        override_enabled = args.override_tests
        with open(test_file, "r") as stream:
            try:
                benchmark_config = yaml.safe_load(stream)
                test_name = benchmark_config["name"]
                is_memtier = False
                if "memtier" in test_name:
                    is_memtier = True
                have_tested_groups = False
                tested_groups = []
                if "tested-groups" in benchmark_config:
                    have_tested_groups = True
                origin_tested_commands = []
                tested_commands = []
                have_tested_commands = False
                if "tested-commands" in benchmark_config:
                    have_tested_commands = True
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
                        command_p = command_part.split(" ",1)[0]
                        command = command_p.replace(" ","")
                        command = command.replace("=","")
                        command = command.replace("\"","")
                        command = command.replace("'","")
                        if "-key-pattern" in command:
                            continue
                        command = command.lower()
                        if command not in tested_commands:
                            tested_commands.append(command)

                if tested_commands != origin_tested_commands:
                    requires_override = True
                    benchmark_config["tested-commands"] = tested_commands
                    logging.warn("there is a difference between specified test-commands in the yaml (name={}) and the ones we've detected {}!={}".format(test_name,origin_tested_commands,tested_commands))


            except Exception as e:
                logging.error(
                    "while loading file {} and error was returned: {}".format(
                        test_file, e.__str__()
                    )
                )
                pass

        if requires_override and override_enabled:
            logging.info("Saving a new version of the file {} with the overrided data".format(test_file))
            with open(test_file, 'w') as file:
                documents = yaml.dump(benchmark_config, file, sort_keys=False)

    conn = redis.StrictRedis(
        host=args.redis_host,
        port=args.redis_port,
        password=args.redis_pass,
        username=args.redis_user,
        decode_responses=False,
    )
