from redis_benchmarks_specification.__common__.spec import (
    extract_redis_configuration_from_topology,
    extract_replica_count,
    extract_primary_count,
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


def test_extract_primary_count():
    topologies_map = get_topologies(
        "./redis_benchmarks_specification/setups/topologies/topologies.yml"
    )
    assert extract_primary_count(topologies_map, "oss-standalone") == 1
    assert extract_primary_count(topologies_map, "oss-cluster-3-primaries") == 3
    assert extract_primary_count(topologies_map, "oss-cluster-5-primaries") == 5
    assert extract_primary_count(topologies_map, "oss-cluster-9-primaries") == 9
    assert extract_primary_count(topologies_map, "oss-cluster-15-primaries") == 15
    assert extract_primary_count(topologies_map, "oss-cluster-30-primaries") == 30


def test_extract_primary_count_defaults_to_one():
    """If redis_topology or primaries key is missing, default to 1."""
    topologies_map = {
        "custom-topo": {"type": "oss-standalone", "resources": {}},
    }
    assert extract_primary_count(topologies_map, "custom-topo") == 1


def test_cluster_topologies_have_type_oss_cluster():
    """All oss-cluster-* topologies must have type 'oss-cluster'."""
    topologies_map = get_topologies(
        "./redis_benchmarks_specification/setups/topologies/topologies.yml"
    )
    for name, spec in topologies_map.items():
        if name.startswith("oss-cluster-"):
            assert (
                spec["type"] == "oss-cluster"
            ), f"Topology {name} should have type 'oss-cluster', got '{spec['type']}'"
            assert extract_primary_count(topologies_map, name) >= 3
            assert extract_replica_count(topologies_map, name) == 0
