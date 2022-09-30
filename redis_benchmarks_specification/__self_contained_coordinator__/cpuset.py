import math


def generate_cpuset_cpus(ceil_db_cpu_limit, current_cpu_pos):
    previous_cpu_pos = current_cpu_pos
    current_cpu_pos = current_cpu_pos + int(ceil_db_cpu_limit)
    db_cpuset_cpus = ",".join(
        [str(x) for x in range(previous_cpu_pos, current_cpu_pos)]
    )
    return db_cpuset_cpus, current_cpu_pos


def extract_db_cpu_limit(topologies_map, topology_spec_name):
    topology_spec = topologies_map[topology_spec_name]
    db_cpu_limit = topology_spec["resources"]["requests"]["cpus"]
    ceil_db_cpu_limit = math.ceil(float(db_cpu_limit))
    return ceil_db_cpu_limit
