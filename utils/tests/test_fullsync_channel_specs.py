import glob

import yaml

from redis_benchmarks_specification.__common__.timeseries import jsonpath_field_chain

SPECS = sorted(
    glob.glob(
        "./redis_benchmarks_specification/test-suites/"
        "memtier_benchmark-20Mkeys-fullsync-raw-1KiB-random-rdb-channel-*.yml"
    )
)


def test_channel_specs_present():
    assert len(SPECS) == 2


def test_channel_specs_declare_replication_sync_v2_metric():
    """The channel specs must export ReplicationFullSyncSecondsV2 under
    "ALL STATS".Totals and preload before the replica starts. The coordinator
    side of this contract (the injector writing that exact key) is pinned by
    test_replication_sync_spec_wiring_matches_injector in #576, which globs all
    specs, so it covers these files once #576 is merged. A merge-order slip
    otherwise exports an empty series with no exception."""
    for path in SPECS:
        with open(path, "r") as yml_file:
            cfg = yaml.safe_load(yml_file)
        assert cfg["dbconfig"].get("preload_before_replica") is True, path
        assert cfg["dbconfig"]["configuration-parameters"]["repl-rdb-channel"] == "yes"
        metrics = cfg["exporter"]["redistimeseries"]["metrics"]
        chains = [jsonpath_field_chain(m) for m in metrics]
        v2 = [
            c
            for c in chains
            if c
            and c[0] == "ALL STATS"
            and "Totals" in c
            and c[c.index("Totals") + 1] == "ReplicationFullSyncSecondsV2"
        ]
        assert v2, f"{path} does not export ReplicationFullSyncSecondsV2"
