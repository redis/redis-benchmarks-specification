tests = [
    {
        "name": "memtier_benchmark-100Kkeys-string-setget50c-20KiB",
        "precommand": "--data-size 20000 --ratio 1:0 --key-pattern P:P -c 25 -t 2 --hide-histogram --key-minimum=1 --key-maximum=100000 -n allkeys",
        "check": {"keyspacelen": 100000},
        "command": "--data-size 20000  --ratio 1:10 --key-pattern R:R -c 25 -t 2 --hide-histogram --key-minimum=1 --key-maximum=100000  --test-time 180",
        "kpis": {},
        "tested-commands": ["setget-20k"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-100Kkeys-string-setget50c-20KiB-pipeline-10",
        "precommand": "--data-size 20000 --ratio 1:0 --key-pattern P:P -c 25 -t 2 --hide-histogram --key-minimum=1 --key-maximum=100000 -n allkeys",
        "check": {"keyspacelen": 100000},
        "command": "--pipeline 10 --data-size 20000 --ratio 1:10 --key-pattern R:R -c 25 -t 2 --key-minimum=1 --key-maximum=100000  --hide-histogram --test-time 180",
        "kpis": {},
        "tested-commands": ["setget-20k"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-100Kkeys-string-setget200c-20KiB",
        "precommand": "--data-size 20000 --ratio 1:0 --key-pattern P:P -c 50 -t 4 --hide-histogram --key-minimum=1 --key-maximum=100000 -n allkeys",
        "check": {"keyspacelen": 100000},
        "command": "--data-size 20000  --ratio 1:10 --key-pattern R:R -c 50 -t 4 --hide-histogram --key-minimum=1 --key-maximum=100000 --test-time 180",
        "kpis": {},
        "tested-commands": ["setget-20k"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-100Kkeys-string-setget200c-20KiB-pipeline-10",
        "precommand": "--data-size 20000 --ratio 1:0 --key-pattern P:P -c 50 -t 4 --hide-histogram --key-minimum=1 --key-maximum=100000 -n allkeys",
        "check": {"keyspacelen": 100000},
        "command": "--pipeline 10 --data-size 20000 --ratio 1:10 --key-pattern R:R -c 50 -t 4 --key-minimum=1 --key-maximum=100000 --hide-histogram --test-time 180",
        "kpis": {},
        "tested-commands": ["setget-20k"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-100Kkeys-load-string50c-with-20KiB-values",
        "precommand": "",
        "check": {"keyspacelen": 0},
        "command": "--data-size 20000 --ratio 1:0 --key-pattern P:P --key-minimum=1 --key-maximum=100000 --test-time 180 -c 25 -t 2 --hide-histogram",
        "kpis": {},
        "tested-commands": ["set-20k"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-100Kkeys-load-string50c-with-20KiB-values-pipeline-10",
        "precommand": "",
        "check": {"keyspacelen": 0},
        "command": "--pipeline 10 --data-size 20000 --ratio 1:0 --key-pattern P:P --key-minimum=1 --key-maximum=100000 --test-time 180 -c 25 -t 2 --hide-histogram",
        "kpis": {},
        "tested-commands": ["set-20k"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-100Kkeys-load-string200c-with-20KiB-values",
        "precommand": "",
        "check": {"keyspacelen": 0},
        "command": "--data-size 20000 --ratio 1:0 --key-pattern P:P --key-minimum=1 --key-maximum=100000 --test-time 180 -c 50 -t 4 --hide-histogram",
        "kpis": {},
        "tested-commands": ["set-20k"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-100Kkeys-load-string200c-with-20KiB-values-pipeline-10",
        "precommand": "",
        "check": {"keyspacelen": 0},
        "command": "--pipeline 10 --data-size 20000 --ratio 1:0 --key-pattern P:P --key-minimum=1 --key-maximum=100000 --test-time 180 -c 50 -t 4 --hide-histogram",
        "kpis": {},
        "tested-commands": ["set-20k"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-string-setget50c-100B",
        "precommand": "--data-size 100 --ratio 1:0 --key-pattern P:P -c 25 -t 2 --hide-histogram --key-minimum=1 --key-maximum=1000000 -n allkeys",
        "check": {"keyspacelen": 1000000},
        "command": "--data-size 100 --ratio 1:10 --key-pattern R:R -c 25 -t 2 --hide-histogram --test-time 180",
        "kpis": {},
        "tested-commands": ["setget"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-string-setget50c-1KiB",
        "precommand": "--data-size 1000 --ratio 1:0 --key-pattern P:P -c 25 -t 2 --hide-histogram --key-minimum=1 --key-maximum=1000000 -n allkeys",
        "check": {"keyspacelen": 1000000},
        "command": "--data-size 1000  --ratio 1:10 --key-pattern R:R -c 25 -t 2 --hide-histogram --test-time 180",
        "kpis": {},
        "tested-commands": ["setget"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-string-setget50c-100B-pipeline-10",
        "precommand": "--data-size 100 --ratio 1:0 --key-pattern P:P -c 25 -t 2 --hide-histogram --key-minimum=1 --key-maximum=1000000 -n allkeys",
        "check": {"keyspacelen": 1000000},
        "command": "--pipeline 10 --data-size 100 --ratio 1:10 --key-pattern R:R -c 25 -t 2 --hide-histogram --test-time 180",
        "kpis": {},
        "tested-commands": ["setget"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-string-setget50c-1KiB-pipeline-10",
        "precommand": "--data-size 1000 --ratio 1:0 --key-pattern P:P -c 25 -t 2 --hide-histogram --key-minimum=1 --key-maximum=1000000 -n allkeys",
        "check": {"keyspacelen": 1000000},
        "command": "--pipeline 10 --data-size 1000 --ratio 1:10 --key-pattern R:R -c 25 -t 2 --hide-histogram --test-time 180",
        "kpis": {},
        "tested-commands": ["setget"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-string-setget200c-100B",
        "precommand": "--data-size 100 --ratio 1:0 --key-pattern P:P -c 50 -t 4 --hide-histogram --key-minimum=1 --key-maximum=1000000 -n allkeys",
        "check": {"keyspacelen": 1000000},
        "command": "--data-size 100 --ratio 1:10 --key-pattern R:R -c 50 -t 4 --hide-histogram --test-time 180",
        "kpis": {},
        "tested-commands": ["setget"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-string-setget200c-1KiB",
        "precommand": "--data-size 1000 --ratio 1:0 --key-pattern P:P -c 50 -t 4 --hide-histogram --key-minimum=1 --key-maximum=1000000 -n allkeys",
        "check": {"keyspacelen": 1000000},
        "command": "--data-size 1000  --ratio 1:10 --key-pattern R:R -c 50 -t 4 --hide-histogram --test-time 180",
        "kpis": {},
        "tested-commands": ["setget"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-string-setget200c-100B-pipeline-10",
        "precommand": "--data-size 100 --ratio 1:0 --key-pattern P:P -c 50 -t 4 --hide-histogram --key-minimum=1 --key-maximum=1000000 -n allkeys",
        "check": {"keyspacelen": 1000000},
        "command": "--pipeline 10 --data-size 100 --ratio 1:10 --key-pattern R:R -c 50 -t 4 --hide-histogram --test-time 180",
        "kpis": {},
        "tested-commands": ["setget"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-string-setget200c-1KiB-pipeline-10",
        "precommand": "--data-size 1000 --ratio 1:0 --key-pattern P:P -c 50 -t 4 --hide-histogram --key-minimum=1 --key-maximum=1000000 -n allkeys",
        "check": {"keyspacelen": 1000000},
        "command": "--pipeline 10 --data-size 1000 --ratio 1:10 --key-pattern R:R -c 50 -t 4 --hide-histogram --test-time 180",
        "kpis": {},
        "tested-commands": ["setget"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-load-string50c-with-100B-values",
        "precommand": "",
        "check": {"keyspacelen": 0},
        "command": "--data-size 100 --ratio 1:0 --key-pattern P:P --key-minimum=1 --key-maximum 1000000 --test-time 180 -c 25 -t 2 --hide-histogram",
        "kpis": {},
        "tested-commands": ["set"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-load-string50c-with-1KiB-values",
        "precommand": "",
        "check": {"keyspacelen": 0},
        "command": "--data-size 1000 --ratio 1:0 --key-pattern P:P --key-minimum=1 --key-maximum 1000000 --test-time 180 -c 25 -t 2 --hide-histogram",
        "kpis": {},
        "tested-commands": ["set"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-load-string50c-with-100B-values-pipeline-10",
        "precommand": "",
        "check": {"keyspacelen": 0},
        "command": "--pipeline 10 --data-size 100 --ratio 1:0 --key-pattern P:P --key-minimum=1 --key-maximum 1000000 --test-time 180 -c 25 -t 2 --hide-histogram",
        "kpis": {},
        "tested-commands": ["set"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-load-string50c-with-1KiB-values-pipeline-10",
        "precommand": "",
        "check": {"keyspacelen": 0},
        "command": "--pipeline 10 --data-size 1000 --ratio 1:0 --key-pattern P:P --key-minimum=1 --key-maximum 1000000 --test-time 180 -c 25 -t 2 --hide-histogram",
        "kpis": {},
        "tested-commands": ["set"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-load-string200c-with-100B-values",
        "precommand": "",
        "check": {"keyspacelen": 0},
        "command": "--data-size 100 --ratio 1:0 --key-pattern P:P --key-minimum=1 --key-maximum 1000000 --test-time 180 -c 50 -t 4 --hide-histogram",
        "kpis": {},
        "tested-commands": ["set"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-load-string200c-with-1KiB-values",
        "precommand": "",
        "check": {"keyspacelen": 0},
        "command": "--data-size 1000 --ratio 1:0 --key-pattern P:P --key-minimum=1 --key-maximum 1000000 --test-time 180 -c 50 -t 4 --hide-histogram",
        "kpis": {},
        "tested-commands": ["set"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-load-string200c-with-100B-values-pipeline-10",
        "precommand": "",
        "check": {"keyspacelen": 0},
        "command": "--pipeline 10 --data-size 100 --ratio 1:0 --key-pattern P:P --key-minimum=1 --key-maximum 1000000 --test-time 180 -c 50 -t 4 --hide-histogram",
        "kpis": {},
        "tested-commands": ["set"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-load-string200c-with-1KiB-values-pipeline-10",
        "precommand": "",
        "check": {"keyspacelen": 0},
        "command": "--pipeline 10 --data-size 1000 --ratio 1:0 --key-pattern P:P --key-minimum=1 --key-maximum 1000000 --test-time 180 -c 50 -t 4 --hide-histogram",
        "kpis": {},
        "tested-commands": ["set"],
        "tested-groups": ["string"],
    },
    {
        "name": "memtier_benchmark-1Mkeys-string-mget-1KiB",
        "precommand": "--data-size 1000 --key-minimum=1 --key-maximum 1000000 -n allkeys --ratio=1:0  --key-pattern P:P --hide-histogram -t 2 -c 100",
        "check": {"keyspacelen": 1000000},
        "command": ' --command="MGET __key__ memtier-1 memtier-2 memtier-3 memtier-4 memtier-5 memtier-6 memtier-7 memtier-8 memtier-9" --command-key-pattern=R -c 50 -t 2 --hide-histogram --test-time 180',
        "kpis": {},
        "tested-commands": ["mget"],
        "tested-groups": ["string"],
    },
]

print(len(tests))
re_filenames = [x["name"] for x in tests]
re_test_specs = {}
for x in tests:
    re_test_specs[x["name"]] = x

import yaml
import json
import pathlib


defaults_filename = "default.yml"
prefix = "memtier_benchmark-"
test_glob = "memtier_*.yml"
files = pathlib.Path().glob(test_glob)
files = [str(x) for x in files]

base_yaml = yaml.safe_load(open("memtier_benchmark-1Mkeys-string-get-1KiB.yml"))
del base_yaml["description"]
# del base_yaml["clientconfig"]["resources"]
# del base_yaml["build-variants"]
# del base_yaml["priority"]
# del base_yaml["redis-topologies"]
# del base_yaml["tested-commands"]
# del base_yaml["version"]
# del base_yaml["tested-groups"]
#


#
# for file in files:
#     if defaults_filename in file:
#         files.remove(file)

for re_file in re_filenames:
    re_spec = re_test_specs[re_file]
    precommand = ""
    if "precommand" in re_spec:
        precommand = re_spec["precommand"]

    command = ""
    if "command" in re_spec:
        command = re_spec["command"]
    if "dbconfig" in base_yaml:
        del base_yaml["dbconfig"]
    if precommand != "":
        base_yaml["dbconfig"] = {}
        base_yaml["dbconfig"]["preload_tool"] = {}
        base_yaml["dbconfig"]["preload_tool"][
            "run_image"
        ] = "redislabs/memtier_benchmark:edge"
        base_yaml["dbconfig"]["preload_tool"]["tool"] = "memtier_benchmark"
        base_yaml["dbconfig"]["preload_tool"]["arguments"] = f"{precommand}"

    base_yaml["clientconfig"]["arguments"] = command
    base_yaml["name"] = re_file
    with open(f"re-string/{re_file}.yml", "w") as outfile:
        yaml.dump(base_yaml, outfile)
