"""
Parca Agent integration for the self-contained coordinator.

This module provides functions to check if parca-agent snap is available
and to update its external labels with benchmark metadata for profiling correlation.
"""

import logging
import shutil
import subprocess
from typing import Dict


def check_parca_agent_available() -> bool:
    """
    Check if snap and parca-agent are available on the system.

    Returns True only if:
      1. snap command exists
      2. parca-agent snap is installed
      3. parca-agent service is running
    """
    # Step 1: Check if snap command exists
    if shutil.which("snap") is None:
        logging.info("snap command not found - parca-agent integration disabled")
        return False

    # Step 2: Check if parca-agent snap is installed
    try:
        result = subprocess.run(
            ["snap", "list", "parca-agent"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            logging.info(
                "parca-agent snap not installed - parca-agent integration disabled"
            )
            return False
    except subprocess.TimeoutExpired:
        logging.warning("Timeout checking parca-agent snap installation")
        return False
    except Exception as e:
        logging.warning(f"Failed to check parca-agent snap: {e}")
        return False

    # Step 3: Check if parca-agent service is running
    try:
        result = subprocess.run(
            ["snap", "services", "parca-agent"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if "active" in result.stdout:
            logging.info(
                "parca-agent snap is available and running - integration enabled"
            )
            return True
        else:
            logging.info(
                "parca-agent snap is installed but not running - integration disabled"
            )
            return False
    except subprocess.TimeoutExpired:
        logging.warning("Timeout checking parca-agent service status")
        return False
    except Exception as e:
        logging.warning(f"Failed to check parca-agent service status: {e}")
        return False


def sanitize_label_value(value: str, max_length: int = 64) -> str:
    """
    Sanitize a label value for use in parca-agent external labels.

    - Replaces '=' and ';' with '_' (these are delimiters in the label format)
    - Replaces ':' with '-' (common in build variants)
    - Truncates to max_length
    - Returns 'unknown' for empty/None values

    Note: parca-agent uses semicolon (;) as the delimiter between key=value pairs,
    so we must escape semicolons in values.
    """
    if not value:
        return "unknown"

    # Convert to string if needed
    value = str(value)

    # Replace problematic characters
    # Note: semicolon is the delimiter for parca-agent --metadata-external-labels
    value = value.replace("=", "_")
    value = value.replace(";", "_")
    value = value.replace(":", "-")
    value = value.replace("'", "")
    value = value.replace('"', "")

    # Truncate if too long
    if len(value) > max_length:
        value = value[:max_length]

    return value


def build_labels_string(labels: Dict[str, str]) -> str:
    """
    Build the labels string for the snap set command.

    Format: key1=value1;key2=value2;...

    Note: parca-agent uses semicolon (;) as the delimiter between key=value pairs,
    not comma. See: --metadata-external-labels=KEY=VALUE;...
    """
    parts = []
    for key, value in labels.items():
        if value is not None:
            sanitized_value = sanitize_label_value(value)
            parts.append(f"{key}={sanitized_value}")
    return ";".join(parts)


def update_parca_agent_labels(labels: Dict[str, str], timeout: int = 30) -> bool:
    """
    Update parca-agent external labels and restart the agent.

    Args:
        labels: Dictionary of label key-value pairs
        timeout: Timeout in seconds for each subprocess call

    Returns:
        True if successful, False otherwise
    """
    labels_string = build_labels_string(labels)

    # Set the external labels
    try:
        logging.info(f"Setting parca-agent external labels: {labels_string}")
        result = subprocess.run(
            [
                "sudo",
                "snap",
                "set",
                "parca-agent",
                f"metadata-external-labels={labels_string}",
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            logging.warning(f"Failed to set parca-agent labels: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        logging.warning("Timeout setting parca-agent labels")
        return False
    except Exception as e:
        logging.warning(f"Failed to set parca-agent labels: {e}")
        return False

    # Restart parca-agent to apply the new labels
    try:
        logging.info("Restarting parca-agent to apply new labels")
        result = subprocess.run(
            ["sudo", "snap", "restart", "parca-agent"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            logging.warning(f"Failed to restart parca-agent: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        logging.warning("Timeout restarting parca-agent")
        return False
    except Exception as e:
        logging.warning(f"Failed to restart parca-agent: {e}")
        return False

    logging.info("Successfully updated parca-agent labels")
    return True


def extract_test_labels_from_benchmark_config(benchmark_config: dict) -> Dict[str, str]:
    """
    Extract test-level labels from a benchmark configuration YAML.

    Extracts:
        - test_name: from 'name' field
        - topology: from 'redis-topologies' (first one)
        - client_tool: from 'clientconfig.tool'
        - tested_commands: from 'tested-commands' (joined with '+')
        - tested_groups: from 'tested-groups' (joined with '+')
        - dataset_name: from 'dbconfig.dataset_name' (if present)
    """
    labels = {}

    # Test name
    if "name" in benchmark_config:
        labels["test_name"] = benchmark_config["name"]

    # Topology (take first one if list)
    if "redis-topologies" in benchmark_config:
        topologies = benchmark_config["redis-topologies"]
        if isinstance(topologies, list) and len(topologies) > 0:
            labels["topology"] = topologies[0]
        elif isinstance(topologies, str):
            labels["topology"] = topologies

    # Client tool
    if "clientconfig" in benchmark_config:
        clientconfig = benchmark_config["clientconfig"]
        if isinstance(clientconfig, dict) and "tool" in clientconfig:
            labels["client_tool"] = clientconfig["tool"]

    # Tested commands (join multiple with '+')
    if "tested-commands" in benchmark_config:
        commands = benchmark_config["tested-commands"]
        if isinstance(commands, list):
            labels["tested_commands"] = "+".join(str(cmd) for cmd in commands)
        elif isinstance(commands, str):
            labels["tested_commands"] = commands

    # Tested groups (join multiple with '+')
    if "tested-groups" in benchmark_config:
        groups = benchmark_config["tested-groups"]
        if isinstance(groups, list):
            labels["tested_groups"] = "+".join(str(grp) for grp in groups)
        elif isinstance(groups, str):
            labels["tested_groups"] = groups

    # Dataset name (from dbconfig)
    if "dbconfig" in benchmark_config:
        dbconfig = benchmark_config["dbconfig"]
        if isinstance(dbconfig, dict) and "dataset_name" in dbconfig:
            labels["dataset_name"] = dbconfig["dataset_name"]
        elif isinstance(dbconfig, list):
            for item in dbconfig:
                if isinstance(item, dict) and "dataset_name" in item:
                    labels["dataset_name"] = item["dataset_name"]
                    break

    return labels
