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
that it *fails closed*: when the requested path is unusable we abort instead of
falling back to the home directory. A silent fallback is worse than an error,
because the run still produces a plausible-looking datapoint attributed to
storage it never touched.

Validation applies only to an explicitly requested path. The home-directory
default is returned unchecked, exactly as before, so adding this flag cannot
turn a working deployment into a startup failure.
"""

import logging
import os
import stat
import subprocess
import tempfile
from pathlib import Path

# Allowlist rather than denylist: a datadir on an unrecognised filesystem is
# not proven to be durable local storage, and for a flag whose contract is
# fail-closed an unknown answer must abort rather than pass. Memory
# (tmpfs/ramfs), container layers (overlay, fuse.fuse-overlayfs), VM passthrough
# (9p, virtiofs) and network filesystems all fail this by omission.
DURABLE_FSTYPES = ("ext2", "ext3", "ext4", "xfs", "btrfs", "zfs", "f2fs")


class DatadirError(Exception):
    """Raised when a requested ``--datadir`` cannot be used as-is."""


def add_datadir_arguments(parser, holds_server_data=True):
    """Register the datadir flags on an entrypoint's parser.

    ``holds_server_data`` is False for entrypoints that never start a server
    (the client runner), so the help text does not promise that setting the
    flag there relocates the redis data dir.
    """
    what = (
        "including the redis data dir"
        if holds_server_data
        else "client output only; this entrypoint starts no server"
    )
    parser.add_argument(
        "--datadir",
        type=str,
        default=None,
        help="Parent directory for benchmark temp dirs ({}). Defaults to the "
        "home directory. An explicitly requested path that is unusable aborts "
        "the process -- it never falls back to the home directory.".format(what),
    )
    parser.add_argument(
        "--datadir-require-separate-filesystem",
        default=False,
        action="store_true",
        help="Require --datadir to live on a different filesystem than '/', and "
        "to be durable storage. Use when the point of the run is which storage "
        "backs the data dir, so a path that quietly sits on the root volume (or "
        "on tmpfs) is treated as a failure.",
    )
    return parser


def _run(cmd):
    """Run argv, returning stdout, or "" when the tool is missing or fails."""
    try:
        return subprocess.run(
            cmd, capture_output=True, text=True, timeout=5
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as e:
        logging.warning("{} failed: {}".format(cmd[0], e))
        return ""


def _findmnt(field, path):
    return _run(["findmnt", "-no", field, "--target", str(path)])


def backing_device(path):
    """Device backing ``path``, or "" if it cannot be determined.

    findmnt prints ``SOURCE[subpath]`` for bind mounts and btrfs subvolumes;
    the suffix is stripped so the value is a real device node.
    """
    source = _findmnt("SOURCE", path)
    return source.split("[", 1)[0] if source else ""


def _same_filesystem_as_root(path):
    try:
        return os.stat(path).st_dev == os.stat("/").st_dev
    except OSError as e:
        raise DatadirError(
            "cannot stat {} or '/' to compare filesystems: {}".format(path, e)
        )


def _probe_writable(datadir):
    """Actually create and remove a dir.

    os.access() is advisory: it consults the real uid, ignores SELinux and
    quota, and as root it passes on a 0500 directory. Since this runs once at
    startup, do the real operation instead.
    """
    try:
        probe = tempfile.mkdtemp(dir=datadir)
        os.rmdir(probe)
    except OSError as e:
        raise DatadirError("--datadir {} is not usable: {}".format(datadir, e))


def datadir_is_explicit(args):
    """True when --datadir was actually passed, as opposed to defaulted."""
    return getattr(args, "datadir", None) is not None


def resolve_datadir(args):
    """Return the directory to use as the parent for benchmark temp dirs.

    Falls back to the home directory only when ``--datadir`` was not passed at
    all. A passed-but-empty value is an error, not a fallback: it is what an
    unset variable in a wrapper script renders to, and it would otherwise put
    the run on the root volume while looking deliberate.
    """
    requested = getattr(args, "datadir", None)
    require_separate = getattr(args, "datadir_require_separate_filesystem", False)

    if requested is None:
        if require_separate:
            raise DatadirError(
                "--datadir-require-separate-filesystem was passed without "
                "--datadir. Specify the data directory whose storage you want "
                "enforced."
            )
        return str(Path.home())

    if str(requested).strip() == "":
        raise DatadirError(
            "--datadir was passed an empty value (an unset variable in a "
            "wrapper script renders this way). Refusing to fall back to the "
            "home directory."
        )

    # realpath, not abspath: a datadir is very often a symlink to the mount
    # ('/data' -> '/mnt/nvme0'), and every check below needs the real path.
    datadir = os.path.realpath(os.path.expanduser(str(requested)))

    if not os.path.exists(datadir):
        raise DatadirError(
            "--datadir {} does not exist. Refusing to fall back to the home "
            "directory: a run that silently lands on the root disk still "
            "produces a datapoint, attributed to storage it never used.".format(datadir)
        )

    if not os.path.isdir(datadir):
        raise DatadirError("--datadir {} is not a directory.".format(datadir))

    _probe_writable(datadir)

    # The server binary is executed from a bind mount under the datadir, so a
    # noexec data volume fails deep inside docker, per test, after work has
    # been claimed. Catch it here instead.
    options = _findmnt("OPTIONS", datadir)
    fstype = _findmnt("FSTYPE", datadir)
    if "noexec" in options.split(","):
        raise DatadirError(
            "--datadir {} is on a noexec mount. The server binary is executed "
            "from a bind mount under it, so benchmarks would fail per test.".format(
                datadir
            )
        )

    if require_separate:
        if fstype and fstype not in DURABLE_FSTYPES:
            raise DatadirError(
                "--datadir {} is on {}, which is not durable local storage. A "
                "benchmark pointed there does not measure the storage under "
                "test.".format(datadir, fstype)
            )
        if backing_device(datadir) and backing_device(datadir) == backing_device("/"):
            raise DatadirError(
                "--datadir {} is backed by the same device as '/' ({}). A "
                "separate subvolume or partition on the same device has its "
                "own st_dev but shares the throughput budget.".format(
                    datadir, backing_device(datadir)
                )
            )
        if not fstype:
            # Without findmnt the durability check cannot run at all. For a flag
            # whose whole contract is fail-closed, degrading to the st_dev test
            # alone would silently weaken the guarantee.
            raise DatadirError(
                "cannot determine the filesystem backing --datadir {} (is "
                "findmnt installed?). Refusing to assert a storage guarantee "
                "that was not verified.".format(datadir)
            )
        if _same_filesystem_as_root(datadir):
            raise DatadirError(
                "--datadir {} shares a filesystem with '/' (device {}), so it "
                "is on the root volume.".format(
                    datadir, backing_device(datadir) or "unknown"
                )
            )

    logging.info(
        "Using {} for benchmark data (device {}, fstype {})".format(
            datadir,
            backing_device(datadir) or "unknown",
            fstype or "unknown",
        )
    )
    return datadir


def private_run_root(datadir):
    """Return a 0700, self-owned directory under ``datadir`` for per-run temp dirs.

    Client output dirs are chmod 0777 so non-root client images can write their
    results. Under $HOME that was safe because $HOME is 0700. A datadir can be
    any operator-chosen mount -- often 1777 like /tmp -- so interpose a private,
    uid-scoped parent to keep the barrier the 0777 dir relies on.

    Every step here refuses to follow a symlink and refuses a directory this
    process does not own. A naive makedirs()/stat()/chmod() on a world-writable
    datadir lets a local uid pre-create this path as a symlink and have us --
    typically root -- chmod the target, or hand us a directory they still own
    and can rewrite under us.
    """
    root = os.path.join(datadir, "redis-benchmarks-{}".format(os.geteuid()))
    try:
        os.mkdir(root, 0o700)
    except FileExistsError:
        pass
    except OSError as e:
        raise DatadirError("cannot create {}: {}".format(root, e))

    # O_NOFOLLOW fails on the final component if it is a symlink, so every
    # check and repair below acts on the directory itself, not on a link target.
    try:
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError as e:
        raise DatadirError(
            "{} is not a real directory (a symlink here would redirect every "
            "benchmark write): {}".format(root, e)
        )
    try:
        st = os.fstat(fd)
        if st.st_uid != os.geteuid():
            raise DatadirError(
                "{} is owned by uid {}, not {}. Refusing to reuse a directory "
                "another user can rewrite under us.".format(
                    root, st.st_uid, os.geteuid()
                )
            )
        if stat.S_IMODE(st.st_mode) != 0o700:
            os.fchmod(fd, 0o700)
    finally:
        os.close(fd)
    return root
