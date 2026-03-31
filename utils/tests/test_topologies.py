from redis_benchmarks_specification.__common__.spec import (
    extract_redis_configuration_from_topology,
    extract_replica_count,
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


def test_extract_replica_count():
    topologies_map = get_topologies(
        "./redis_benchmarks_specification/setups/topologies/topologies.yml"
    )
    assert extract_replica_count(topologies_map, "oss-standalone") == 0
    assert extract_replica_count(topologies_map, "oss-standalone-01-replicas") == 1
    assert extract_replica_count(topologies_map, "oss-standalone-02-replicas") == 2


def test_extract_replica_count_with_io_threads():
    topologies_map = get_topologies(
        "./redis_benchmarks_specification/setups/topologies/topologies.yml"
    )
    assert (
        extract_replica_count(
            topologies_map, "oss-standalone-01-replicas-08-io-threads"
        )
        == 1
    )
