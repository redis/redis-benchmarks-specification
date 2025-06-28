import math
import logging


def extract_redis_dbconfig_parameters(benchmark_config, dbconfig_keyname):
    redis_configuration_parameters = {}
    modules_configuration_parameters_map = {}
    dataset_load_timeout_secs = 120
    dataset_name = None
    dbconfig_present = False
    if dbconfig_keyname in benchmark_config:
        dbconfig_present = True
        if type(benchmark_config[dbconfig_keyname]) == list:
            for k in benchmark_config[dbconfig_keyname]:
                if "configuration-parameters" in k:
                    cp = k["configuration-parameters"]
                    for item in cp:
                        for k, v in item.items():
                            redis_configuration_parameters[k] = v
                if "dataset_load_timeout_secs" in k:
                    dataset_load_timeout_secs = k["dataset_load_timeout_secs"]
                if "dataset_name" in k:
                    dataset_name = k["dataset_name"]
        if type(benchmark_config[dbconfig_keyname]) == dict:
            if "configuration-parameters" in benchmark_config[dbconfig_keyname]:
                cp = benchmark_config[dbconfig_keyname]["configuration-parameters"]
                for k, v in cp.items():
                    redis_configuration_parameters[k] = v
            if "dataset_load_timeout_secs" in benchmark_config[dbconfig_keyname]:
                dataset_load_timeout_secs = benchmark_config[dbconfig_keyname][
                    "dataset_load_timeout_secs"
                ]
            if "dataset_name" in benchmark_config[dbconfig_keyname]:
                dataset_name = benchmark_config[dbconfig_keyname]["dataset_name"]

    return (
        dbconfig_present,
        dataset_name,
        redis_configuration_parameters,
        dataset_load_timeout_secs,
        modules_configuration_parameters_map,
    )


def extract_redis_configuration_from_topology(topologies_map, topology_spec_name):
    redis_arguments = ""
    topology_spec = topologies_map[topology_spec_name]
    if "redis_arguments" in topology_spec:
        redis_arguments = topology_spec["redis_arguments"]
        logging.info(
            f"extracted redis_arguments: {redis_arguments} from topology: {topology_spec_name}"
        )
    return redis_arguments


def extract_client_cpu_limit(benchmark_config):
    # Handle both clientconfig (single) and clientconfigs (multiple) formats
    if "clientconfigs" in benchmark_config:
        # For multiple configs, return the sum of all CPU limits
        total_cpu_limit = 0
        for client_config in benchmark_config["clientconfigs"]:
            cpu_limit = client_config["resources"]["requests"]["cpus"]
            total_cpu_limit += float(cpu_limit)
        return math.ceil(total_cpu_limit)
    else:
        # Legacy single clientconfig format
        db_cpu_limit = benchmark_config["clientconfig"]["resources"]["requests"]["cpus"]
        ceil_db_cpu_limit = math.ceil(float(db_cpu_limit))
        return ceil_db_cpu_limit


def extract_build_variant_variations(benchmark_config, keyname="build-variants"):
    result = None
    if keyname in benchmark_config:
        result = benchmark_config[keyname]
    return result


def extract_client_container_image(benchmark_config, keyname="clientconfig"):
    client_container_image = None
    if keyname in benchmark_config:
        if "run_image" in benchmark_config[keyname]:
            client_container_image = benchmark_config[keyname]["run_image"]
    return client_container_image


def extract_client_container_images(benchmark_config):
    """Extract container images for both single and multiple client configs"""
    if "clientconfigs" in benchmark_config:
        # Multiple client configs - return list of images
        images = []
        for client_config in benchmark_config["clientconfigs"]:
            if "run_image" in client_config:
                images.append(client_config["run_image"])
            else:
                images.append(None)
        return images
    elif "clientconfig" in benchmark_config:
        # Single client config - return list with one image for consistency
        if "run_image" in benchmark_config["clientconfig"]:
            return [benchmark_config["clientconfig"]["run_image"]]
        else:
            return [None]
    return []


def extract_client_tool(benchmark_config, keyname="clientconfig"):
    client_tool = None
    if keyname in benchmark_config:
        if "tool" in benchmark_config[keyname]:
            client_tool = benchmark_config[keyname]["tool"]
    return client_tool


def extract_client_tools(benchmark_config):
    """Extract tools for both single and multiple client configs"""
    if "clientconfigs" in benchmark_config:
        # Multiple client configs - return list of tools
        tools = []
        for client_config in benchmark_config["clientconfigs"]:
            if "tool" in client_config:
                tools.append(client_config["tool"])
            else:
                tools.append(None)
        return tools
    elif "clientconfig" in benchmark_config:
        # Single client config - return list with one tool for consistency
        if "tool" in benchmark_config["clientconfig"]:
            return [benchmark_config["clientconfig"]["tool"]]
        else:
            return [None]
    return []


def extract_client_configs(benchmark_config):
    """Extract client configurations as a list for both single and multiple formats"""
    if "clientconfigs" in benchmark_config:
        # Multiple client configs
        return benchmark_config["clientconfigs"]
    elif "clientconfig" in benchmark_config:
        # Single client config - return as list for consistency
        return [benchmark_config["clientconfig"]]
    return []
