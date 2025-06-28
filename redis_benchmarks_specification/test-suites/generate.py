import yaml
import json
import pathlib

expected_format = {
    "name": "HSet_d_400_c_10_t_10_pipeline_10_key-max=1000000",
    "precommand": "",
    "check": {"keyspacelen": 0},
    "command": '--command="HSET myhash2 __key__ __data__" --command-key-pattern=G --key-minimum=1 --key-maximum 1000000 d 400 -c 10 -t 10 --hide-histogram --pipeline=10 --test-time=100',
    "kpis": {
        "tls": {"ops": 1003694, "latency": 1, "kbs": 89188},
        "default": {"ops": 932625, "latency": 1.1, "kbs": 82873},
    },
    "tested-commands": ["hset"],
    "tested-groups": ["hash"],
    "comparison": {"max_variance_percent": 2.0},
}
defaults_filename = "default.yml"
prefix = "memtier_benchmark-"
test_glob = "memtier_*.yml"
files = pathlib.Path().glob(test_glob)
files = [str(x) for x in files]

for file in files:
    if defaults_filename in file:
        files.remove(file)


print(len(files))

rdb_counter = 0
init_commands_counter = 0
can_convert_counter = 0

group_tests = {}
counter_groups = {}
final_enterprise_json = []
for yml_filename in files:
    with open(yml_filename, "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        tested_commands = benchmark_config["tested-commands"]
        name = benchmark_config["name"]
        tested_groups = benchmark_config["tested-groups"]
        contains_db_config = False
        contains_init_commands = False
        contains_rdb = False
        dbconfig = {}
        precommand = ""
        if "dbconfig" in benchmark_config:
            dbconfig = benchmark_config["dbconfig"]
            contains_db_config = True
            if "init_commands" in benchmark_config["dbconfig"]:
                contains_init_commands = True
            if "dataset" in benchmark_config["dbconfig"]:
                contains_rdb = True

        if contains_rdb:
            rdb_counter = rdb_counter + 1
        elif contains_init_commands:
            init_commands_counter = init_commands_counter + 1
        else:
            can_convert_counter = can_convert_counter + 1

        if (
            contains_db_config
            and contains_rdb is False
            and contains_init_commands is False
        ):
            keyspace_check = False
            keyspace_value = 0

            if "check" in dbconfig:
                if "keyspacelen" in dbconfig["check"]:
                    keyspace_check = True
                    keyspace_value = dbconfig["check"]["keyspacelen"]
            if "preload_tool" in dbconfig:
                precommand = dbconfig["preload_tool"]["arguments"]
            # Handle both clientconfig and clientconfigs formats
            if "clientconfigs" in benchmark_config:
                # For multiple configs, use the first one for generation
                command = benchmark_config["clientconfigs"][0]["arguments"]
            else:
                # Legacy single clientconfig format
                command = benchmark_config["clientconfig"]["arguments"]
            check_dict = {"keyspacelen": keyspace_value}

            test_definition = {
                "name": name,
                "precommand": f"{precommand}",
                "check": check_dict,
                "command": f"{command}",
                "kpis": {},
                "tested-commands": tested_commands,
                "tested-groups": tested_groups,
            }
            for tested_group in tested_groups:
                if tested_group not in group_tests:
                    group_tests[tested_group] = []
                group_tests[tested_group].append(test_definition)
                if tested_group not in counter_groups:
                    counter_groups[tested_group] = 0
                counter_groups[tested_group] = counter_groups[tested_group] + 1
            final_enterprise_json.append(test_definition)

print(f"RDB tests {rdb_counter}")
print(f"INIT command tests {init_commands_counter}")
print(f"Other tests {can_convert_counter}")
print(f"Final Enterprise tests {len(final_enterprise_json)}")

print(counter_groups)

for tested_group, tests in group_tests.items():
    with open(f"jsons/{tested_group}.json", "w") as json_fd:
        json.dump(tests, json_fd)
