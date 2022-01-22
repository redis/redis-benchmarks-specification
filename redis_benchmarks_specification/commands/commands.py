#  BSD 3-Clause License
#
#  Copyright (c) 2022., Redis Performance Group <performance at redis dot com>
#  All rights reserved.
#


def generate_command_groups(commands_json):
    groups = {}
    for command_name, command_description in commands_json.items():
        group_name = command_description["group"]
        if group_name not in groups:
            groups[group_name] = []
        groups[group_name].append(command_name)
    return groups
