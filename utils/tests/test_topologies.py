from redis_benchmarks_specification.__common__.spec import (
    extract_redis_configuration_from_topology,
)
from redis_benchmarks_specification.__setups__.topologies import get_topologies


def test_get_topologies():
    topologies_map = get_topologies(
        "./redis_benchmarks_specification/setups/topologies/topologies.yml"
    )
    assert "oss-standalone" in topologies_map
    assert topologies_map["oss-standalone"]["resources"]["requests"]["cpus"] == "1"


def test_extract_redis_configuration_from_topology():
    topologies_map = get_topologies(
        "./redis_benchmarks_specification/setups/topologies/topologies.yml"
    )
    assert "oss-standalone-04-io-threads" in topologies_map.keys()
    res = extract_redis_configuration_from_topology(
        topologies_map, "oss-standalone-04-io-threads"
    )
    assert res != ""
    assert "--io-threads 4 --io-threads-do-reads yes" in res
