import os
import math
import shlex
import sys
import shutil
import argparse
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import PlainScalarString


def parse_arguments(arguments):
    """
    Parses the memtier benchmark arguments to extract relevant parameters.

    Args:
        arguments (str): The arguments string from the YAML file.

    Returns:
        dict: A dictionary containing extracted parameters.
    """
    args_list = shlex.split(arguments)
    params = {}
    for i, arg in enumerate(args_list):
        if arg == "--data-size" and i + 1 < len(args_list):
            try:
                params["data_size"] = int(args_list[i + 1])
            except ValueError:
                pass
        elif arg.startswith("--data-size="):
            try:
                params["data_size"] = int(arg.split("=", 1)[1])
            except ValueError:
                pass
        elif arg == "--key-maximum" and i + 1 < len(args_list):
            try:
                params["key_maximum"] = int(args_list[i + 1])
            except ValueError:
                pass
        elif arg.startswith("--key-maximum="):
            try:
                params["key_maximum"] = int(arg.split("=", 1)[1])
            except ValueError:
                pass
        elif arg == "--command" and i + 1 < len(args_list):
            params["command"] = args_list[i + 1]
        elif arg.startswith("--command="):
            params["command"] = arg.split("=", 1)[1]
    return params


def calculate_expected_memory(params):
    """
    Calculates the expected memory usage based on extracted parameters.

    Args:
        params (dict): Extracted parameters from the arguments.

    Returns:
        float: Expected memory usage in gigabytes (GB).
    """
    # Assumptions
    key_size = 20  # bytes
    overhead_per_key = 50  # bytes
    buffer_factor = 1.1  # 10% buffer

    data_size = params.get("data_size", 3)  # bytes
    key_maximum = params.get("key_maximum", 1000000)  # default to 1 million

    # Calculate number of data fields based on '__data__' placeholders
    command = params.get("command", "")
    num_fields = command.count("__data__")

    if num_fields > 1:
        total_data_size = data_size * num_fields
    else:
        total_data_size = data_size

    # Total size per key in bytes
    total_size_per_key = key_size + total_data_size + overhead_per_key

    # Total memory in bytes with buffer
    total_memory_bytes = total_size_per_key * key_maximum * buffer_factor

    # Convert to gigabytes
    total_memory_gb = total_memory_bytes / (1024**3)

    return total_memory_gb


def remove_test_file(yaml_file_path, removed_dir):
    """
    Removes the YAML test file by moving it to a 'removed_tests' directory.

    Args:
        yaml_file_path (str): Path to the YAML file.
        removed_dir (str): Directory where removed files are stored.
    """
    if not os.path.exists(removed_dir):
        os.makedirs(removed_dir)
    basename = os.path.basename(yaml_file_path)
    destination = os.path.join(removed_dir, basename)
    shutil.move(yaml_file_path, destination)
    print(f"Removed {yaml_file_path} (moved to {destination})")


def process_yaml_file(yaml_file_path, removed_dir):
    """
    Processes a single YAML file to determine if it should be removed.

    Args:
        yaml_file_path (str): Path to the YAML file.
        removed_dir (str): Directory where removed files are stored.
    """
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=0)
    yaml.width = 4096  # Prevent line wrapping

    try:
        with open(yaml_file_path, "r") as file:
            config = yaml.load(file)
    except Exception as e:
        print(f"Error reading {yaml_file_path}: {e}", file=sys.stderr)
        return

    # Check for necessary fields - handle both clientconfig and clientconfigs
    try:
        if "clientconfigs" in config:
            # Multiple client configs - use first one for memory calculation
            arguments = config["clientconfigs"][0]["arguments"]
        elif "clientconfig" in config:
            # Single client config
            arguments = config["clientconfig"]["arguments"]
        else:
            print(f"Skipping {yaml_file_path}: Missing client configuration.")
            return
    except (KeyError, IndexError):
        print(f"Skipping {yaml_file_path}: Invalid client configuration format.")
        return

    # Convert arguments to string
    if not isinstance(arguments, str):
        arguments_str = str(arguments)
    else:
        arguments_str = arguments

    # Parse arguments
    params = parse_arguments(arguments_str)

    # Calculate expected memory
    expected_memory_gb = calculate_expected_memory(params)

    # print(f"File: {yaml_file_path} | Expected Memory: {expected_memory_gb:.2f}GB")

    # If memory exceeds 20GB, remove the test
    if expected_memory_gb > 25:
        print(f"Removed {yaml_file_path}: Memory usage: {expected_memory_gb}.")
        remove_test_file(yaml_file_path, removed_dir)


#  else:
#    print(f"Retained {yaml_file_path}: Memory usage within limits.")


def main():
    parser = argparse.ArgumentParser(
        description="Remove YAML test files exceeding 20GB memory requirement."
    )
    parser.add_argument(
        "--directory",
        type=str,
        default="../redis_benchmarks_specification/test-suites/",
        help="Path to the directory containing YAML test files.",
    )
    parser.add_argument(
        "--removed-dir",
        type=str,
        default="removed_tests",
        help="Directory to move removed YAML files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run without removing any files.",
    )

    args = parser.parse_args()

    directory = args.directory
    removed_dir = os.path.join(directory, args.removed_dir)

    if not os.path.isdir(directory):
        print(f"Directory {directory} does not exist.", file=sys.stderr)
        sys.exit(1)

    # Iterate over all YAML files
    for filename in os.listdir(directory):
        if filename.endswith(".yml") or filename.endswith(".yaml"):
            yaml_file_path = os.path.join(directory, filename)
            if args.dry_run:
                # Perform calculations without removing files
                yaml = YAML()
                yaml.preserve_quotes = True
                yaml.indent(mapping=2, sequence=4, offset=0)
                yaml.width = 4096  # Prevent line wrapping
                try:
                    with open(yaml_file_path, "r") as file:
                        config = yaml.load(file)
                except Exception as e:
                    print(f"Error reading {yaml_file_path}: {e}", file=sys.stderr)
                    continue

                try:
                    if "clientconfigs" in config:
                        # Multiple client configs - use first one for memory calculation
                        arguments = config["clientconfigs"][0]["arguments"]
                    elif "clientconfig" in config:
                        # Single client config
                        arguments = config["clientconfig"]["arguments"]
                    else:
                        print(
                            f"Skipping {yaml_file_path}: Missing client configuration."
                        )
                        continue
                except (KeyError, IndexError):
                    print(
                        f"Skipping {yaml_file_path}: Invalid client configuration format."
                    )
                    continue

                if not isinstance(arguments, str):
                    arguments_str = str(arguments)
                else:
                    arguments_str = arguments

                params = parse_arguments(arguments_str)
                expected_memory_gb = calculate_expected_memory(params)

            #  print(f"File: {yaml_file_path} | Expected Memory: {expected_memory_gb:.2f}GB")
            # if expected_memory_gb > 25:
            #     print(f"Would remove {yaml_file_path}: Exceeds 20GB.")
            # else:
            #     print(f"Would retain {yaml_file_path}: Within memory limits.")
            else:
                # Actual removal based on memory calculation
                process_yaml_file(yaml_file_path, removed_dir)


if __name__ == "__main__":
    main()
