import os
import argparse
from ruamel.yaml import YAML
import collections

# Command groups mapping
COMMAND_GROUPS = {
    "string": ["set", "get", "append", "getbit", "setrange", "bitcount", "mget"],
    "hash": [
        "hset",
        "hget",
        "hincrby",
        "hmset",
        "hdel",
        "hscan",
        "hexists",
        "hkeys",
        "hvals",
        "hmget",
        "hsetnx",
        "hgetall",
    ],
    "list": ["lpush", "rpop", "lpop", "lrem", "lrange", "lindex", "lpos", "linsert"],
    "set": [
        "sadd",
        "smembers",
        "sismember",
        "sunion",
        "sdiff",
        "sinter",
        "smismember",
        "sscan",
    ],
    "sorted_set": [
        "zadd",
        "zrange",
        "zrevrange",
        "zrangebyscore",
        "zrevrangebyscore",
        "zincrby",
        "zrem",
        "zscore",
        "zrank",
        "zunion",
        "zunionstore",
        "zrevrank",
        "zscan",
        "zcard",
    ],
    "stream": ["xadd", "xread"],
    "geospatial": ["geosearch", "geopos", "geohash", "geodist"],
    "key_management": [
        "expire",
        "pexpire",
        "ttl",
        "expireat",
        "touch",
        "del",
        "exists",
    ],
    "pubsub": ["ping", "hello"],
    "scripting": ["eval", "evalsha"],
    "transaction": ["multi", "exec"],
    "hyperloglog": ["pfadd"],
    "server_management": ["hello"],
}


def parse_arguments(arguments):
    """
    Parses the memtier benchmark arguments to extract relevant parameters.
    Specifically extracts the --command argument.

    Args:
        arguments (str): The arguments string from the YAML file.

    Returns:
        dict: A dictionary containing extracted parameters.
    """
    params = {}
    command = None

    for arg in arguments.split():
        if arg.startswith("--command="):
            command = arg.split("=", 1)[1]
        elif arg == "--command":
            command = arguments.split()[arguments.split().index(arg) + 1]

    return command


def categorize_command(command):
    """
    Categorize a Redis command into a command group.

    Args:
        command (str): The Redis command.

    Returns:
        str: The command group.
    """
    for group, commands in COMMAND_GROUPS.items():
        if command in commands:
            return group
    return "unknown"


def summarize_yaml_file(yaml_file_path, command_summary, command_group_summary):
    """
    Processes a single YAML file to extract the tested commands and groups.

    Args:
        yaml_file_path (str): Path to the YAML file.
        command_summary (dict): Dictionary to store the command summary.
        command_group_summary (dict): Dictionary to store the command group summary.
    """
    yaml = YAML()
    yaml.preserve_quotes = True

    try:
        with open(yaml_file_path, "r") as file:
            config = yaml.load(file)
    except Exception as e:
        print(f"Error reading {yaml_file_path}: {e}")
        return

    # Extract tested commands from 'tested-commands'
    tested_commands = config.get("tested-commands", [])
    for command in tested_commands:
        command_summary["tested_commands"][command] += 1
        command_group = categorize_command(command)
        command_group_summary[command_group] += 1

    # Extract command from client configuration - handle both formats
    arguments = ""
    if "clientconfigs" in config:
        # Multiple client configs - use first one for summary
        if config["clientconfigs"] and "arguments" in config["clientconfigs"][0]:
            arguments = config["clientconfigs"][0]["arguments"]
    elif "clientconfig" in config:
        # Single client config
        arguments = config.get("clientconfig", {}).get("arguments", "")

    if arguments:
        command = parse_arguments(arguments)
        if command:
            command_summary["client_arguments_commands"][command] += 1
            command_group = categorize_command(command)
            command_group_summary[command_group] += 1


def summarize_directory(directory):
    """
    Summarizes the commands and command groups across all YAML files in a directory.

    Args:
        directory (str): Path to the directory containing YAML files.
    """
    command_summary = {
        "tested_commands": collections.Counter(),
        "client_arguments_commands": collections.Counter(),
    }
    command_group_summary = collections.Counter()

    # Iterate over all YAML files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".yml") or filename.endswith(".yaml"):
            yaml_file_path = os.path.join(directory, filename)
            summarize_yaml_file(yaml_file_path, command_summary, command_group_summary)

    # Print summary
    print("\nTested Commands Summary:")
    for command, count in command_summary["tested_commands"].items():
        print(f"{command}: {count} occurrences")

    print("\nClient Arguments Commands Summary:")
    for command, count in command_summary["client_arguments_commands"].items():
        print(f"{command}: {count} occurrences")

    print("\nCommand Group Summary:")
    for group, count in command_group_summary.items():
        print(f"{group.capitalize()}: {count} occurrences")


def main():
    parser = argparse.ArgumentParser(
        description="Summarize commands and command groups from YAML benchmark files."
    )
    parser.add_argument(
        "--directory",
        type=str,
        default="../redis_benchmarks_specification/test-suites/",
        help="Path to the directory containing YAML test files.",
    )

    args = parser.parse_args()
    directory = args.directory

    if not os.path.isdir(directory):
        print(f"Directory {directory} does not exist.")
        return

    summarize_directory(directory)


if __name__ == "__main__":
    main()
