#  Apache 2 License
#
#  Copyright (c) 2021., Redis Labs
#  All rights reserved.
#
import os
import yaml


def get_build_config(usecase_filename):
    full_filename = os.path.abspath(usecase_filename)
    with open(full_filename) as stream:
        build_config = yaml.safe_load(stream)
        # print(build_config)
        id = build_config["id"]
        return build_config, id


def get_build_config_metadata(build_config):
    build_config_metadata = {}
    if "metadata" in build_config:
        build_config_metadata = build_config["metadata"]
    return build_config_metadata
