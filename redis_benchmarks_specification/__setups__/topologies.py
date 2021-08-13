import os
import yaml


def get_topologies(usecase_filename):
    full_filename = os.path.abspath(usecase_filename)
    topologies_map = {}
    with open(full_filename) as stream:
        build_config = yaml.safe_load(stream)
        spec = build_config["spec"]
        setups = spec["setups"]
        for topology in setups:
            topology_name = topology["name"]
            topologies_map[topology_name] = topology
        # print(build_config)

    return topologies_map
