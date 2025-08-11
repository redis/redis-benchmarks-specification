import redis
import argparse
import logging
import numpy as np
import os
import re
import ruamel.yaml
from ruamel.yaml.scalarstring import DoubleQuotedScalarString


def calculate_rate_limit(p50_value):
    if p50_value < 1000:
        return 100
    elif p50_value < 10000:
        return 1000
    else:
        return 10000


def create_new_test_config(
    original_config_path, new_config_path, test_name, new_test_name, p50_value
):
    # Check if the original configuration file exists
    if not os.path.exists(original_config_path):
        return False  # Indicate failure

    # Load the original test configuration with ruamel.yaml
    yaml = ruamel.yaml.YAML()
    yaml.preserve_quotes = True  # Preserve quotes in scalar values
    with open(original_config_path, "r") as file:
        config = yaml.load(file)

    # Calculate the total desired rate limit
    total_rate_limit = calculate_rate_limit(p50_value)

    # Calculate per-connection rate limit
    # Extract the original arguments
    original_arguments = config["clientconfig"]["arguments"]

    # Convert to string if necessary
    if not isinstance(original_arguments, str):
        original_arguments_str = str(original_arguments)
    else:
        original_arguments_str = original_arguments

    # Print the original arguments for debugging
    # print(f"Processing arguments for '{test_name}': {original_arguments_str}")

    # Use regex to extract clients (-c or --clients) and threads (-t or --threads)
    clients_per_thread = 50  # Default value
    threads = 4  # Default value

    clients_match = re.search(
        r"(?:-c|--clients)(?:[=\s]+)(\d+)", original_arguments_str
    )
    if clients_match:
        clients_per_thread = int(clients_match.group(1))

    threads_match = re.search(
        r"(?:-t|--threads)(?:[=\s]+)(\d+)", original_arguments_str
    )
    if threads_match:
        threads = int(threads_match.group(1))

    # Calculate total number of connections
    total_connections = clients_per_thread * threads

    # Calculate per-connection rate limit
    per_connection_rate_limit = max(1, int(total_rate_limit / total_connections))

    # Remove existing rate limit arguments using regex
    new_arguments = re.sub(
        r"--rate(?:-limit(?:ing)?)?(?:\s+\S+)?", "", original_arguments_str
    )

    # Append the new '--rate-limiting' argument and its value
    new_arguments = (
        f"{new_arguments.strip()} --rate-limiting {per_connection_rate_limit}"
    )

    # Update the test name to reflect the new test
    config["name"] = new_test_name
    config["description"] += f" Rate limited to {total_rate_limit} ops/sec."

    # Update the arguments in the config
    config["clientconfig"]["arguments"] = DoubleQuotedScalarString(new_arguments)

    # Ensure the destination directory exists
    os.makedirs(os.path.dirname(new_config_path), exist_ok=True)

    # Save the new test configuration
    with open(new_config_path, "w") as file:
        yaml.dump(config, file)

    print(
        f"Created new test configuration for '{test_name}' with total rate limit {total_rate_limit} ops/sec and per-connection rate limit {per_connection_rate_limit} ops/sec."
    )
    return True  # Indicate success


def main():
    parser = argparse.ArgumentParser(
        description="Create latency benchmarks",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--redistimeseries_host", type=str, default="benchmarks.redislabs.com"
    )
    parser.add_argument("--redistimeseries_port", type=int, default=12011)
    parser.add_argument("--redistimeseries_pass", type=str, default=None)
    parser.add_argument("--redistimeseries_user", type=str, default=None)

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    logging.info(
        "Checking connection to RedisTimeSeries with user: {}, host: {}, port: {}".format(
            args.redistimeseries_user,
            args.redistimeseries_host,
            args.redistimeseries_port,
        )
    )
    rts = redis.Redis(
        host=args.redistimeseries_host,
        port=args.redistimeseries_port,
        password=args.redistimeseries_pass,
        username=args.redistimeseries_user,
        decode_responses=True,
    )
    try:
        rts.ping()
    except redis.exceptions.ConnectionError as e:
        logging.error(f"Failed to connect to RedisTimeSeries: {e}")
        return

    # Key for test cases
    testcases_key = "ci.benchmarks.redis/ci/redis/redis:testcases"

    # Retrieve test cases
    testcases = rts.smembers(testcases_key)
    # Decode bytes to strings
    testcases = [testcase for testcase in testcases]

    failed_files = []  # List to collect test cases with missing config files

    # Iterate over each test case
    for test_name in testcases:
        # Construct the time series key
        ts_key = (
            f"ci.benchmarks.redis/by.branch/ci/redis/redis/{test_name}/"
            "gcc:15.2.0-amd64-debian-bookworm-default/"
            "intel64-ubuntu22.04-redis-icx1/oss-standalone/unstable/"
            "ALL_STATS.Totals.Ops/sec"
        )
        try:
            # Execute the TS.REVRANGE command
            # "-" and "+" denote the minimal and maximal timestamps
            result = rts.execute_command("TS.REVRANGE", ts_key, "-", "+")

            # Check if result is not empty
            if result:
                # Extract values and convert to floats
                values = [float(value) for timestamp, value in result]
                # Compute the median (p50)
                p50_value = np.median(values)

                # Output the results
                print(f"Results for test case '{test_name}': p50 rate = {p50_value}")
                rate = calculate_rate_limit(p50_value)

                original_config_path = f"../redis_benchmarks_specification/test-suites/{test_name}.yml"  # Original test config file
                new_test_name = f"latency-rate-limited-{rate}_qps-{test_name}"
                new_config_path = f"../redis_benchmarks_specification/test-suites/{new_test_name}.yml"  # New test config file
                success = create_new_test_config(
                    original_config_path,
                    new_config_path,
                    test_name,
                    new_test_name,
                    p50_value,
                )
                if not success:
                    failed_files.append(test_name)
            else:
                print(f"No data available for test case '{test_name}'.")
                failed_files.append(test_name)

        except redis.exceptions.ResponseError as e:
            print(f"Error retrieving data for test case '{test_name}': {e}")
            failed_files.append(test_name)
        except Exception as e:
            print(f"An error occurred while processing test case '{test_name}': {e}")
            failed_files.append(test_name)

    # At the end, print out the list of failed files if any
    if failed_files:
        print("\nThe following test cases had missing configuration files or errors:")
        for test_name in failed_files:
            print(f"- {test_name}")


if __name__ == "__main__":
    main()
