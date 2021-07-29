from redis_benchmarks_specification.__setups__.topologies import get_topologies


def test_get_topologies():
    topologies_map = get_topologies(
        "./redis_benchmarks_specification/setups/topologies/topologies.yml"
    )
    assert "oss-standalone" in topologies_map
    assert topologies_map["oss-standalone"]["resources"]["requests"]["cpus"] == "1"
