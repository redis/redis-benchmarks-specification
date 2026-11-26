#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
"""Bounded, compressed file logging for the long-running daemons.

``--logname`` used to attach a plain ``logging.FileHandler`` (via ``basicConfig(filename=...)``),
which never rotates: a coordinator that logs every benchmark result grows its file without limit.
On a fleet runner that file reached 55 GB and, together with preserved failed-test directories,
filled the root filesystem, after which every benchmark failed with ``No space left on device``.

The daemons now log through a ``RotatingFileHandler`` whose rolled-over files are gzip-compressed:

* the active file is rolled once it reaches ``--log-max-bytes`` (default 100 MiB);
* at most ``--log-backup-count`` compressed backups are kept (default 5), oldest dropped first;
* ``--no-log-compress`` keeps backups uncompressed; ``--log-max-bytes 0`` restores the old
  unbounded behaviour (use it when an external ``logrotate`` owns the file).

With the defaults the worst case is roughly ``max-bytes + backup-count * (max-bytes / ~10)`` on disk.
"""

import argparse
import gzip
import logging
import logging.handlers
import os
import shutil
import sys
import time

DEFAULT_LOG_MAX_BYTES = 100 * 1024 * 1024
DEFAULT_LOG_BACKUP_COUNT = 5


def _non_negative_int(value):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError("expected an integer, got {!r}".format(value))
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0, got {}".format(parsed))
    return parsed


def _positive_int(value):
    parsed = _non_negative_int(value)
    if parsed < 1:
        # RotatingFileHandler treats backupCount=0 as "never roll over", which would silently
        # reintroduce an unbounded file while appearing to have rotation configured.
        raise argparse.ArgumentTypeError("must be >= 1, got {}".format(parsed))
    return parsed


def add_log_rotation_args(parser):
    """Add the rotation flags next to ``--logname`` on a daemon's parser."""
    parser.add_argument(
        "--log-max-bytes",
        type=_non_negative_int,
        default=DEFAULT_LOG_MAX_BYTES,
        help="Roll the --logname file over once it reaches this many bytes. "
        "0 disables rotation (unbounded file, the pre-rotation behaviour).",
    )
    parser.add_argument(
        "--log-backup-count",
        type=_positive_int,
        default=DEFAULT_LOG_BACKUP_COUNT,
        help="Number of rolled-over --logname files to keep; the oldest is deleted.",
    )
    parser.add_argument(
        "--no-log-compress",
        default=False,
        action="store_true",
        help="Keep rolled-over --logname files uncompressed instead of gzip-compressing them.",
    )


def _gzip_namer(name):
    return name + ".gz"


def _gzip_rotator(source, dest):
    # Compress into a temporary name and rename, so a crash mid-compression never leaves a
    # truncated ".gz" that looks like a valid backup.
    tmp = dest + ".tmp"
    try:
        with open(source, "rb") as f_in, gzip.open(tmp, "wb", compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.replace(tmp, dest)
    except Exception as e:
        # Typically ENOSPC. Drop the partial temp file, then still move the source into the
        # backup slot uncompressed (a rename needs no free space). Raising instead would leave
        # the full active file in place, so every later record would retry the rollover and
        # write another temp file: the disk-filling failure this module exists to prevent.
        # The slot keeps its ".gz" name so --log-backup-count still bounds it; zcat -f reads it.
        try:
            os.remove(tmp)
        except OSError:
            pass
        sys.stderr.write(
            "log rotation: compressing {} failed ({}); kept it uncompressed as {}\n".format(
                source, e, dest
            )
        )
        os.replace(source, dest)
        return
    os.remove(source)


def _set_aside_oversized_log(logname, max_bytes):
    """Move an existing log that is already over ``max_bytes`` out of the rotation.

    A deployment upgrading from the unbounded handler can have a multi-GB ``--logname`` file.
    RotatingFileHandler would gzip all of it on the first record, holding the handler lock
    (stalling every logging thread) and needing free space for the compressed copy. Renaming it
    aside is O(1) and needs no space; it is left for the operator to archive or delete.
    """
    try:
        size = os.path.getsize(logname)
    except OSError:
        return None
    if size <= max_bytes:
        return None
    aside = "{}.pre-rotation-{}".format(
        logname, time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    )
    os.replace(logname, aside)
    sys.stderr.write(
        "log rotation: {} was {} bytes, over --log-max-bytes {}; moved it to {} "
        "(not rotated or deleted)\n".format(logname, size, max_bytes, aside)
    )
    return aside


def build_log_handler(
    logname,
    max_bytes=DEFAULT_LOG_MAX_BYTES,
    backup_count=DEFAULT_LOG_BACKUP_COUNT,
    compress=True,
):
    """Return the file handler for ``logname``.

    ``max_bytes == 0`` returns a plain, never-rotating ``FileHandler`` (append mode).
    """
    if max_bytes <= 0:
        return logging.FileHandler(logname, mode="a")
    if backup_count < 1:
        raise ValueError("backup_count must be >= 1 when rotation is enabled")
    _set_aside_oversized_log(logname, max_bytes)
    handler = logging.handlers.RotatingFileHandler(
        logname, mode="a", maxBytes=max_bytes, backupCount=backup_count
    )
    if compress:
        handler.namer = _gzip_namer
        handler.rotator = _gzip_rotator
    return handler


def configure_logging(args, log_format, log_datefmt, log_level):
    """Configure root logging for a daemon from its parsed ``args``."""
    if args.logname is None:
        logging.basicConfig(format=log_format, level=log_level, datefmt=log_datefmt)
        return
    print("Writting log to {}".format(args.logname))
    handler = build_log_handler(
        args.logname,
        max_bytes=args.log_max_bytes,
        backup_count=args.log_backup_count,
        compress=not args.no_log_compress,
    )
    handler.setFormatter(logging.Formatter(log_format, log_datefmt))
    logging.basicConfig(handlers=[handler], level=log_level)
