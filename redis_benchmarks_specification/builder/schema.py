#  Apache 2 License
#
#  Copyright (c) 2021., Redis Labs
#  All rights reserved.
#
import os
import yaml


def get_build_config(usecase_filename):
    stream = os.path.dirname(os.path.abspath(usecase_filename))
    build_config = yaml.safe_load(stream)
    print(build_config)
    id = build_config["id"]
    return build_config, id
