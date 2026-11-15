"""Untimed setup and fail-closed checks for single-command RDB benchmarks."""

import logging


class RdbChecks:
    """Opt-in standalone SAVE/load checks; keep sampled bytes out of logs."""

    def __init__(self, config, connections, setup_type, override_test_time):
        if setup_type != "oss-standalone" or len(connections) != 1:
            raise ValueError("RDB checks require one standalone primary")
        if override_test_time:
            raise ValueError("RDB single-command tests forbid a test-time override")
        self.conn = connections[0]
        self.operation = config["operation"]
        self.count = config["keyspacelen"]
        self.size = config["value_size"]
        self.keys = config["sample_keys"]
        if self.operation not in ("save", "load"):
            raise ValueError("RDB operation must be save or load")
        if self.count <= 0 or self.size <= 0 or not self.keys:
            raise ValueError("RDB checks require a positive dataset and sample keys")

    def _dataset(self):
        if self.conn.dbsize() != self.count:
            raise ValueError("RDB dataset key count mismatch")
        values = [self.conn.get(key) for key in self.keys]
        if any(value is None or len(value) != self.size for value in values):
            raise ValueError("RDB sampled value size mismatch")
        return values

    def prepare(self):
        self.values = self._dataset()
        if self.operation == "load":
            if not self.conn.save():
                raise ValueError("RDB load setup SAVE failed")
            self.conn.execute_command("FLUSHALL", "SYNC")
            if self.conn.dbsize() != 0:
                raise ValueError("RDB load must start from an empty database")
        self.conn.config_resetstat()
        logging.info("RDB %s setup checks passed", self.operation)

    def verify(self):
        # Validate command counts before issuing any post-measurement commands.
        command = "save" if self.operation == "save" else "debug"
        stats = self.conn.info("commandstats").get("cmdstat_" + command, {})
        if (
            stats.get("calls") != 1
            or stats.get("failed_calls") != 0
            or stats.get("rejected_calls") != 0
        ):
            raise ValueError("RDB benchmark requires exactly one successful command")
        if self._dataset() != self.values:
            raise ValueError("RDB sampled values changed")
        if self.operation == "load":
            info = self.conn.info("persistence")
            if (
                info.get("rdb_last_load_keys_loaded") != self.count
                or info.get("rdb_last_load_keys_expired") != 0
            ):
                raise ValueError("RDB load counters do not match the dataset")
        logging.info("RDB %s completion checks passed", self.operation)
