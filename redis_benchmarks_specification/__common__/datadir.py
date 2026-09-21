"""Selection and verification of the directory benchmark data is written to.

Every entrypoint used to derive its temp dirs from ``Path.home()``. The
coordinator bind-mounts one of those dirs into the server container and passes
the mount point as ``dbdir``, which becomes redis' ``--dir`` -- so the home
directory's filesystem silently decided where benchmark data landed. On a
typical runner that is the root volume, shared with the OS, container images
and build artifacts.

That matters for any storage-sensitive suite. A run can be limited by the root
volume's provisioned throughput and still look repeatable, because a steady
storage ceiling produces a low coefficient of variation.

``--datadir`` makes the choice explicit. The guarantee that makes it useful is
that it *fails closed*: if the requested path is unusable we abort rather than
fall back to the home directory. A silent fallback is worse than an error,
because the run still produces a plausible-looking datapoint attributed to
storage it never touched.
"""

import logging
import os
import subprocess
from pathlib import Path


class DatadirError(Exception):
    """Raised when a requested ``--datadir`` cannot be used as-is."""


def add_datadir_arguments(parser):
    """Register the datadir flags on an entrypoint's parser."""
    parser.add_argument(
        "--datadir",
        type=str,
        default=None,
        help="Parent directory for benchmark temp dirs, including the redis data "
        "dir. Defaults to the home directory. If the path is unusable the process "
        "aborts -- it never falls back to the home directory.",
    )
    parser.add_argument(
        "--datadir-require-mountpoint",
        default=False,
        action="store_true",
        help="Require --datadir to be a mountpoint. Use when the point of the run "
        "is which storage backs the data dir, so a path that quietly lives on the "
        "root filesystem is treated as a failure.",
    )
    return parser


def _backing_device(path):
    """Return the device backing ``path``, or "" if it cannot be determined."""
    try:
        out = subprocess.run(
            ["findmnt", "-no", "SOURCE", "--target", str(path)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return out.stdout.strip()
    except Exception:
        return ""


def _is_mountpoint(path):
    return os.path.ismount(str(path))


def _is_instance_store(device):
    """True when ``device`` is physically attached EC2 instance storage.

    A device name proves nothing: EBS is exposed as /dev/nvme* too. The
    authoritative signal is the by-id symlink, which carries an
    ``Instance_Storage`` marker only for physically attached disks.
    """
    if not device:
        return False
    base = os.path.basename(os.path.realpath(device))
    by_id = "/dev/disk/by-id"
    if not os.path.isdir(by_id):
        return False
    try:
        for name in os.listdir(by_id):
            if "Instance_Storage" not in name:
                continue
            target = os.path.realpath(os.path.join(by_id, name))
            if os.path.basename(target) == base:
                return True
    except OSError:
        return False
    return False


def resolve_datadir(args):
    """Return the directory to use as the parent for benchmark temp dirs.

    Falls back to the home directory only when ``--datadir`` was not requested.
    """
    requested = getattr(args, "datadir", None)
    require_mountpoint = getattr(args, "datadir_require_mountpoint", False)

    if requested is None or str(requested).strip() == "":
        if require_mountpoint:
            raise DatadirError(
                "--datadir-require-mountpoint was passed without --datadir. "
                "Specify the data directory whose mount you want enforced."
            )
        home = str(Path.home())
        logging.info("Using home directory for benchmark data: {}".format(home))
        return home

    datadir = os.path.abspath(os.path.expanduser(str(requested)))

    if not os.path.exists(datadir):
        raise DatadirError(
            "--datadir {} does not exist. Refusing to fall back to the home "
            "directory: a run that silently lands on the root disk still "
            "produces a datapoint, attributed to storage it never used.".format(datadir)
        )

    if not os.path.isdir(datadir):
        raise DatadirError("--datadir {} is not a directory.".format(datadir))

    if not os.access(datadir, os.W_OK | os.X_OK):
        raise DatadirError(
            "--datadir {} is not writable by uid {}.".format(datadir, os.geteuid())
        )

    if require_mountpoint and not _is_mountpoint(datadir):
        raise DatadirError(
            "--datadir {} is not a mountpoint and --datadir-require-mountpoint "
            "was passed. It resolves to device {}, which is whatever backs the "
            "parent filesystem -- most likely the root volume.".format(
                datadir, _backing_device(datadir) or "unknown"
            )
        )

    logging.info(
        "Using {} for benchmark data (device {})".format(
            datadir, _backing_device(datadir) or "unknown"
        )
    )
    return datadir


def collect_storage_metadata(datadir):
    """Describe the storage backing ``datadir``.

    Returned so a result can record which storage produced it. Two backends that
    publish under one identity merge into a single baseline, which hides exactly
    the difference a storage comparison exists to measure.
    """

    def _run(cmd):
        try:
            return subprocess.run(
                cmd, capture_output=True, text=True, timeout=30
            ).stdout.strip()
        except Exception:
            return ""

    datadir = os.path.abspath(str(datadir))
    device = _backing_device(datadir)
    realdev = os.path.realpath(device) if device else ""
    base = os.path.basename(realdev) if realdev else ""

    metadata = {
        "datadir": datadir,
        "device": device,
        "device_resolved": realdev,
        "is_mountpoint": _is_mountpoint(datadir),
        "mountpoint": _run(["findmnt", "-no", "TARGET", "--target", datadir]),
        "fstype": _run(["findmnt", "-no", "FSTYPE", "--target", datadir]),
        "mount_options": _run(["findmnt", "-no", "OPTIONS", "--target", datadir]),
        "physically_attached": _is_instance_store(realdev or device),
    }

    if base:
        metadata["model"] = _run(["lsblk", "-ndo", "MODEL", realdev])
        metadata["size"] = _run(["lsblk", "-ndo", "SIZE", realdev])
        rotational = ""
        try:
            with open("/sys/block/{}/queue/rotational".format(base)) as fh:
                rotational = fh.read().strip()
        except OSError:
            pass
        metadata["rotational"] = rotational

    # md devices hide their members, and the members are what the run actually hit.
    if base.startswith("md"):
        detail = _run(["mdadm", "--detail", realdev])
        metadata["raid_members"] = [
            token
            for token in detail.split()
            if token.startswith("/dev/") and token != realdev
        ]

    return metadata


def storage_backend_label(metadata):
    """Short, groupable label for the storage a result came from."""
    if not metadata.get("is_mountpoint"):
        return "unknown"
    if metadata.get("physically_attached"):
        return "instance-store"
    return "network-attached"
