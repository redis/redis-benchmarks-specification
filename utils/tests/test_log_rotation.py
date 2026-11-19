#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
"""Tests for the bounded, compressed --logname handler.

Offline; no Redis, no Docker. The regression being guarded: ``--logname`` used to attach an
unbounded ``FileHandler``, and a long-lived coordinator grew its log to 55 GB and filled the disk.
"""

import argparse
import glob
import gzip
import logging
import os
import sys
import uuid

import pytest

from redis_benchmarks_specification.__common__.log_rotation import (
    DEFAULT_LOG_BACKUP_COUNT,
    DEFAULT_LOG_MAX_BYTES,
    add_log_rotation_args,
    build_log_handler,
    configure_logging,
)

LINE = "x" * 99  # 100 bytes with the newline


@pytest.fixture
def logger():
    """A private, non-propagating logger so tests never touch the root logger."""
    lg = logging.getLogger("rotation-test-" + uuid.uuid4().hex)
    lg.setLevel(logging.INFO)
    lg.propagate = False
    yield lg
    for h in list(lg.handlers):
        h.close()
        lg.removeHandler(h)


def _emit(lg, n):
    for i in range(n):
        lg.info("%05d %s", i, LINE)


def _read_all(path):
    """Concatenate backups (oldest first) and the active file, decompressing .gz."""
    out = []
    backups = sorted(
        (p for p in glob.glob(path + ".*")),
        key=lambda p: int(p.split(path + ".")[1].split(".")[0]),
        reverse=True,
    )
    for p in backups:
        opener = gzip.open if p.endswith(".gz") else open
        with opener(p, "rt") as f:
            out.append(f.read())
    with open(path) as f:
        out.append(f.read())
    return "".join(out)


def test_rolls_over_and_compresses(tmp_path, logger):
    path = str(tmp_path / "coord.log")
    logger.addHandler(build_log_handler(path, max_bytes=2000, backup_count=3))
    _emit(logger, 100)  # ~11 KB -> several rollovers

    names = sorted(os.listdir(tmp_path))
    assert "coord.log" in names
    backups = [n for n in names if n != "coord.log"]
    assert backups, "no rollover happened"
    assert all(n.endswith(".gz") for n in backups), backups
    assert not any(
        n.endswith(".tmp") for n in names
    ), "left a partial compression behind"
    # Every backup must be a valid gzip stream, not a truncated file.
    for n in backups:
        with gzip.open(tmp_path / n, "rt") as f:
            assert "x" * 99 in f.read()


def test_backup_count_bounds_disk_and_drops_oldest(tmp_path, logger):
    path = str(tmp_path / "coord.log")
    logger.addHandler(build_log_handler(path, max_bytes=2000, backup_count=2))
    _emit(logger, 500)  # ~55 KB through a 2000-byte window

    names = os.listdir(tmp_path)
    assert len(names) <= 3, names  # active + at most 2 backups, however much is logged
    kept = _read_all(path)
    assert "00499" in kept  # newest survives
    assert "00000" not in kept  # oldest was dropped, not retained forever
    # Total on disk stays within a small multiple of max_bytes, whatever was logged.
    assert sum(os.path.getsize(tmp_path / n) for n in names) < 3 * 2000 + 2000


def test_no_lines_lost_across_rotation_when_backups_suffice(tmp_path, logger):
    path = str(tmp_path / "coord.log")
    logger.addHandler(build_log_handler(path, max_bytes=2000, backup_count=50))
    _emit(logger, 100)
    text = _read_all(path)
    assert [l[:5] for l in text.splitlines()] == ["%05d" % i for i in range(100)]


def test_can_disable_compression(tmp_path, logger):
    path = str(tmp_path / "coord.log")
    logger.addHandler(
        build_log_handler(path, max_bytes=2000, backup_count=3, compress=False)
    )
    _emit(logger, 100)
    backups = [n for n in os.listdir(tmp_path) if n != "coord.log"]
    assert backups and not any(n.endswith(".gz") for n in backups), backups


def test_zero_max_bytes_restores_unbounded_file(tmp_path, logger):
    path = str(tmp_path / "coord.log")
    logger.addHandler(build_log_handler(path, max_bytes=0))
    _emit(logger, 100)
    assert os.listdir(tmp_path) == [
        "coord.log"
    ]  # never rotates; an external logrotate owns it
    assert os.path.getsize(path) > 10_000


def test_backup_count_zero_is_rejected_not_silently_unbounded():
    # RotatingFileHandler(backupCount=0) never rolls over, which would look like rotation but
    # behave like the original bug.
    with pytest.raises(ValueError):
        build_log_handler(
            "/nonexistent/never-opened.log", max_bytes=1000, backup_count=0
        )


def _parser():
    p = argparse.ArgumentParser()
    p.add_argument("--logname", type=str, default=None)
    add_log_rotation_args(p)
    return p


def test_defaults_are_bounded():
    args = _parser().parse_args([])
    assert args.log_max_bytes == DEFAULT_LOG_MAX_BYTES > 0
    assert args.log_backup_count == DEFAULT_LOG_BACKUP_COUNT >= 1
    assert args.no_log_compress is False


@pytest.mark.parametrize(
    "argv",
    [
        ["--log-backup-count", "0"],
        ["--log-max-bytes", "-1"],
        ["--log-max-bytes", "abc"],
    ],
)
def test_bad_values_are_rejected_at_parse_time(argv):
    with pytest.raises(SystemExit):
        _parser().parse_args(argv)


def test_configure_logging_end_to_end(tmp_path):
    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level
    try:
        for h in list(root.handlers):
            root.removeHandler(
                h
            )  # basicConfig is a no-op when the root already has handlers
        path = str(tmp_path / "daemon.log")
        args = _parser().parse_args(
            ["--logname", path, "--log-max-bytes", "2000", "--log-backup-count", "2"]
        )
        configure_logging(args, "%(levelname)s %(message)s", "%H:%M:%S", logging.INFO)
        for i in range(200):
            logging.info("%05d %s", i, LINE)
        for h in root.handlers:
            h.flush()
        names = os.listdir(tmp_path)
        assert len(names) <= 3, names
        assert any(n.endswith(".gz") for n in names)
        with open(path) as f:
            assert f.readline().startswith("INFO ")  # formatter applied
    finally:
        for h in list(root.handlers):
            h.close()
            root.removeHandler(h)
        for h in saved_handlers:
            root.addHandler(h)
        root.setLevel(saved_level)


def test_configure_logging_without_logname_logs_to_stderr_only(tmp_path):
    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level
    try:
        for h in list(root.handlers):
            root.removeHandler(h)
        args = _parser().parse_args([])
        configure_logging(args, "%(message)s", "%H:%M:%S", logging.INFO)
        assert not any(isinstance(h, logging.FileHandler) for h in root.handlers)
        assert os.listdir(tmp_path) == []
    finally:
        for h in list(root.handlers):
            h.close()
            root.removeHandler(h)
        for h in saved_handlers:
            root.addHandler(h)
        root.setLevel(saved_level)


def test_every_daemon_exposes_the_flags(monkeypatch, capsys):
    from redis_benchmarks_specification.__runner__.args import create_client_runner_args
    from redis_benchmarks_specification.__self_contained_coordinator__.args import (
        create_self_contained_coordinator_args,
    )

    for factory in (create_client_runner_args, create_self_contained_coordinator_args):
        ns = factory("t").parse_args([])
        assert ns.log_max_bytes == DEFAULT_LOG_MAX_BYTES
        assert ns.log_backup_count == DEFAULT_LOG_BACKUP_COUNT
        assert ns.no_log_compress is False

    # The builder builds its parser inline in main(), so check its --help output.
    from redis_benchmarks_specification.__builder__ import builder

    monkeypatch.setattr(sys, "argv", ["redis-benchmarks-spec-builder", "--help"])
    with pytest.raises(SystemExit) as exc:
        builder.main()
    assert exc.value.code == 0
    out = capsys.readouterr().out
    for flag in ("--log-max-bytes", "--log-backup-count", "--no-log-compress"):
        assert flag in out
