import json

from redis_benchmarks_specification.commands.commands import generate_command_groups


def test_generate_command_groups():
    with open("./redis_benchmarks_specification/setups/topologies/topologies.yml","r") as json_fd:
        commands_json = json.load(json_fd)
        command_groups = generate_command_groups(commands_json)
        assert "server" in command_groups.keys()