import argparse
import ast
import os
import stat
import tempfile

import pytest

from redis_benchmarks_specification.__common__ import datadir as datadir_mod
from redis_benchmarks_specification.__common__.datadir import (
    DatadirError,
    add_datadir_arguments,
    backing_device,
    datadir_is_explicit,
    private_run_root,
    resolve_datadir,
)
from redis_benchmarks_specification.__runner__.args import create_client_runner_args
from redis_benchmarks_specification.__self_contained_coordinator__.args import (
    create_self_contained_coordinator_args,
)

PKG_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "redis_benchmarks_specification",
)


def _args(**kwargs):
    ns = argparse.Namespace(datadir=None, datadir_require_separate_filesystem=False)
    for k, v in kwargs.items():
        setattr(ns, k, v)
    return ns


def _a_separate_writable_fs():
    """A writable mount that is NOT the root filesystem, or None."""
    root_dev = os.stat("/").st_dev
    for candidate in ("/dev/shm", "/run/shm", "/tmp"):
        if not os.path.isdir(candidate):
            continue
        if os.stat(candidate).st_dev == root_dev:
            continue
        if os.access(candidate, os.W_OK | os.X_OK):
            return candidate
    return None


# --------------------------------------------------------------------------
# default (no --datadir) must be byte-identical to the previous behaviour
# --------------------------------------------------------------------------


def test_defaults_to_home_when_not_requested():
    assert resolve_datadir(_args()) == os.path.expanduser("~")


def test_missing_attributes_do_not_break_callers():
    assert resolve_datadir(argparse.Namespace()) == os.path.expanduser("~")


def test_default_deployment_layout_is_byte_identical():
    """The composition the entrypoints actually use, not resolve_datadir alone.

    Each entrypoint does `private_run_root(d) if datadir_is_explicit(args) else d`.
    Interposing the private parent unconditionally would move every existing
    runner's temp dirs from $HOME to $HOME/redis-benchmarks-<uid> and add a new
    startup failure mode on a read-only or full $HOME -- under a claim that
    nothing changes for deployments that never set the flag.
    """
    args = _args()
    d = resolve_datadir(args)
    home = private_run_root(d) if datadir_is_explicit(args) else d
    assert home == os.path.expanduser("~")
    assert not datadir_is_explicit(args)


def test_an_explicit_datadir_does_get_the_private_parent():
    with tempfile.TemporaryDirectory() as d:
        os.chmod(d, 0o1777)
        args = _args(datadir=d)
        assert datadir_is_explicit(args)
        root = private_run_root(resolve_datadir(args))
        assert root.startswith(os.path.realpath(d))
        assert stat.S_IMODE(os.stat(root).st_mode) == 0o700


def test_private_run_root_refuses_a_planted_symlink():
    """The attack the 1777 datadir in the docstring invites.

    makedirs/stat/chmod all follow symlinks, so a local uid could pre-create
    this predictable name pointing anywhere and have us -- often root -- chmod
    the target to 0700.
    """
    with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as victim:
        os.chmod(d, 0o1777)
        os.chmod(victim, 0o755)
        os.symlink(victim, os.path.join(d, "redis-benchmarks-{}".format(os.geteuid())))
        with pytest.raises(DatadirError, match="not a real directory"):
            private_run_root(d)
        assert stat.S_IMODE(os.stat(victim).st_mode) == 0o755


def test_require_separate_filesystem_rejects_an_unknown_fstype():
    """Allowlist, not denylist: an unrecognised filesystem must abort."""
    if (
        not os.path.isdir("/dev/shm")
        or os.stat("/dev/shm").st_dev == os.stat("/").st_dev
    ):
        pytest.skip("no tmpfs available")
    with pytest.raises(DatadirError, match="not durable local storage"):
        resolve_datadir(
            _args(datadir="/dev/shm", datadir_require_separate_filesystem=True)
        )


# --------------------------------------------------------------------------
# fail-closed
# --------------------------------------------------------------------------


def test_empty_string_is_an_error_not_a_fallback():
    """An unset variable in a wrapper script renders as "".

    Falling back to $HOME here is the precise failure --datadir exists to
    prevent, so it must raise.
    """
    for blank in ("", "   ", "\t"):
        with pytest.raises(DatadirError, match="empty value"):
            resolve_datadir(_args(datadir=blank))


def test_missing_datadir_aborts_rather_than_falling_back():
    with tempfile.TemporaryDirectory() as d:
        missing = os.path.join(d, "does-not-exist")
        with pytest.raises(DatadirError) as excinfo:
            resolve_datadir(_args(datadir=missing))
        assert "Refusing to fall back" in str(excinfo.value)


def test_file_instead_of_directory_aborts():
    with tempfile.NamedTemporaryFile() as f:
        with pytest.raises(DatadirError, match="not a directory"):
            resolve_datadir(_args(datadir=f.name))


def test_unwritable_datadir_aborts_via_a_real_probe():
    """os.access() is advisory and passes as root; the probe is not."""
    with tempfile.TemporaryDirectory() as d:
        sub = os.path.join(d, "ro")
        os.mkdir(sub)
        os.chmod(sub, 0o500)
        try:
            if os.geteuid() == 0:
                # root bypasses DAC, so the probe legitimately succeeds
                assert resolve_datadir(_args(datadir=sub)) == os.path.realpath(sub)
            else:
                with pytest.raises(DatadirError, match="not usable"):
                    resolve_datadir(_args(datadir=sub))
        finally:
            os.chmod(sub, 0o700)


# --------------------------------------------------------------------------
# path normalisation
# --------------------------------------------------------------------------


def test_relative_path_is_absolutised():
    with tempfile.TemporaryDirectory() as d:
        cwd = os.getcwd()
        try:
            os.chdir(d)
            os.mkdir("sub")
            got = resolve_datadir(_args(datadir="sub"))
            assert os.path.isabs(got)
            assert got == os.path.realpath(os.path.join(d, "sub"))
        finally:
            os.chdir(cwd)


def test_symlink_to_a_directory_is_resolved():
    """'/data' -> '/mnt/nvme0' is how operators usually wire a data volume.

    abspath() would leave the symlink unresolved and every device check below
    would then describe the wrong filesystem.
    """
    with tempfile.TemporaryDirectory() as d:
        target = os.path.join(d, "target")
        link = os.path.join(d, "link")
        os.mkdir(target)
        os.symlink(target, link)
        assert resolve_datadir(_args(datadir=link)) == os.path.realpath(target)


def test_user_expansion():
    assert resolve_datadir(_args(datadir="~")) == os.path.realpath(
        os.path.expanduser("~")
    )


# --------------------------------------------------------------------------
# --datadir-require-separate-filesystem
# --------------------------------------------------------------------------


def test_require_separate_filesystem_rejects_the_root_filesystem():
    """os.path.ismount('/') is True, so the old predicate ACCEPTED '/'.

    Which check rejects it depends on whether '/' is writable by the test user,
    so pin the rejection rather than the message.
    """
    with pytest.raises(DatadirError):
        resolve_datadir(_args(datadir="/", datadir_require_separate_filesystem=True))


def test_require_separate_filesystem_rejects_a_plain_subdirectory_of_root():
    with tempfile.TemporaryDirectory() as d:
        if os.stat(d).st_dev != os.stat("/").st_dev:
            pytest.skip("temp dir is not on the root filesystem here")
        # Either gate may catch it first: same st_dev, or same backing device.
        with pytest.raises(
            DatadirError, match="same device as|shares a filesystem with"
        ):
            resolve_datadir(_args(datadir=d, datadir_require_separate_filesystem=True))


def test_require_separate_filesystem_rejects_tmpfs():
    """A RAM disk passes a mountpoint test and measures nothing about storage."""
    if (
        not os.path.isdir("/dev/shm")
        or os.stat("/dev/shm").st_dev == os.stat("/").st_dev
    ):
        pytest.skip("no tmpfs available")
    with pytest.raises(DatadirError, match="not durable local storage"):
        resolve_datadir(
            _args(datadir="/dev/shm", datadir_require_separate_filesystem=True)
        )


def test_require_separate_filesystem_accepts_a_subdirectory_of_the_right_mount():
    """A subdir of the correct mount is on the correct device by construction."""
    mount = _a_separate_writable_fs()
    if mount is None or mount == "/dev/shm":
        pytest.skip("no separate durable writable filesystem available")
    with tempfile.TemporaryDirectory(dir=mount) as sub:
        assert resolve_datadir(
            _args(datadir=sub, datadir_require_separate_filesystem=True)
        ) == os.path.realpath(sub)


def test_require_separate_filesystem_without_datadir_is_an_error():
    with pytest.raises(DatadirError, match="without --datadir"):
        resolve_datadir(_args(datadir_require_separate_filesystem=True))


# --------------------------------------------------------------------------
# private run root -- restores the 0700 barrier the 0777 client dir relies on
# --------------------------------------------------------------------------


def test_private_run_root_is_0700_even_under_a_world_writable_datadir():
    with tempfile.TemporaryDirectory() as d:
        os.chmod(d, 0o1777)
        root = private_run_root(d)
        assert root.startswith(d)
        assert stat.S_IMODE(os.stat(root).st_mode) == 0o700


def test_private_run_root_is_idempotent_and_repairs_permissions():
    with tempfile.TemporaryDirectory() as d:
        first = private_run_root(d)
        os.chmod(first, 0o777)
        second = private_run_root(d)
        assert first == second
        assert stat.S_IMODE(os.stat(second).st_mode) == 0o700


# --------------------------------------------------------------------------
# wiring
# --------------------------------------------------------------------------


def test_arguments_are_registered_and_default_to_unset():
    parser = argparse.ArgumentParser()
    add_datadir_arguments(parser)
    parsed = parser.parse_args([])
    assert parsed.datadir is None
    assert parsed.datadir_require_separate_filesystem is False

    parsed = parser.parse_args(
        ["--datadir", "/mnt/nvme", "--datadir-require-separate-filesystem"]
    )
    assert parsed.datadir == "/mnt/nvme"
    assert parsed.datadir_require_separate_filesystem is True


def test_every_entrypoint_parser_exposes_the_flags():
    for factory in (
        create_client_runner_args,
        create_self_contained_coordinator_args,
    ):
        parsed = factory("x").parse_args([])
        assert hasattr(parsed, "datadir"), factory
        assert hasattr(parsed, "datadir_require_separate_filesystem"), factory


def test_no_module_still_derives_its_temp_dir_from_home():
    """Reachability guard: the value must not just be computed, it must be used.

    Mirrors the AST guard test_builder.py already uses for
    override_deployment_regexp. Without this, a future edit can reintroduce a
    home-derived temp dir and every other test stays green.
    """
    offenders = []
    for dirpath, _, filenames in os.walk(PKG_ROOT):
        for fn in filenames:
            if not fn.endswith(".py"):
                continue
            path = os.path.join(dirpath, fn)
            rel = os.path.relpath(path, PKG_ROOT)
            if rel == os.path.join("__common__", "datadir.py"):
                continue
            src = open(path).read()
            for node in ast.walk(ast.parse(src)):
                if not isinstance(node, ast.Call):
                    continue
                f = node.func
                # Path.home()
                if isinstance(f, ast.Attribute) and f.attr == "home":
                    offenders.append(rel)
                # os.path.expanduser("~")
                if (
                    isinstance(f, ast.Attribute)
                    and f.attr == "expanduser"
                    and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and node.args[0].value == "~"
                ):
                    offenders.append(rel)
    assert offenders == [], "still derive temp dirs from home: {}".format(
        sorted(set(offenders))
    )


def test_backing_device_strips_the_bind_mount_subpath():
    """findmnt prints SOURCE[subpath]; the suffix is not part of the device."""
    dev = backing_device("/")
    assert "[" not in dev


# --------------------------------------------------------------------------
# Stubbed findmnt: these pin behaviour that real mounts cannot exercise on an
# ordinary CI box without root. Mutation testing showed the noexec check and
# the accept-direction of --datadir-require-separate-filesystem were both
# deletable with the suite still green.
# --------------------------------------------------------------------------


@pytest.fixture
def fake_findmnt(monkeypatch):
    table = {}

    def _fake(field, path):
        return table.get(field, "")

    monkeypatch.setattr(datadir_mod, "_findmnt", _fake)
    return table


def test_noexec_datadir_is_rejected(fake_findmnt, tmp_path):
    """The server binary is executed from a bind mount under the datadir."""
    fake_findmnt["OPTIONS"] = "rw,noexec,nosuid,relatime"
    fake_findmnt["FSTYPE"] = "ext4"
    with pytest.raises(DatadirError, match="noexec"):
        resolve_datadir(_args(datadir=str(tmp_path)))


def test_noexec_substring_does_not_false_positive(fake_findmnt, tmp_path):
    """'noexec' must match an option, not a substring of one."""
    fake_findmnt["OPTIONS"] = "rw,relatime,x-noexec-lookalike"
    fake_findmnt["FSTYPE"] = "ext4"
    assert resolve_datadir(_args(datadir=str(tmp_path))) == os.path.realpath(tmp_path)


def test_require_separate_filesystem_accepts_a_durable_separate_device(
    fake_findmnt, tmp_path, monkeypatch
):
    """The ACCEPT direction. Without this, 'reject everything' passes the suite."""
    fake_findmnt["OPTIONS"] = "rw,relatime"
    fake_findmnt["FSTYPE"] = "ext4"
    monkeypatch.setattr(
        datadir_mod,
        "backing_device",
        lambda p: "/dev/nvme9n1" if str(p) != "/" else "/dev/root",
    )
    monkeypatch.setattr(datadir_mod, "_same_filesystem_as_root", lambda p: False)
    assert resolve_datadir(
        _args(datadir=str(tmp_path), datadir_require_separate_filesystem=True)
    ) == os.path.realpath(tmp_path)


def test_require_separate_filesystem_rejects_the_same_backing_device(
    fake_findmnt, tmp_path, monkeypatch
):
    """A btrfs subvolume or second partition has its own st_dev but shares the disk."""
    fake_findmnt["OPTIONS"] = "rw,relatime"
    fake_findmnt["FSTYPE"] = "btrfs"
    monkeypatch.setattr(datadir_mod, "backing_device", lambda p: "/dev/nvme0n1p2")
    monkeypatch.setattr(datadir_mod, "_same_filesystem_as_root", lambda p: False)
    with pytest.raises(DatadirError, match="same device"):
        resolve_datadir(
            _args(datadir=str(tmp_path), datadir_require_separate_filesystem=True)
        )


def test_require_separate_filesystem_fails_closed_without_findmnt(
    fake_findmnt, tmp_path, monkeypatch
):
    """A missing util-linux must not silently downgrade the guarantee."""
    monkeypatch.setattr(datadir_mod, "_same_filesystem_as_root", lambda p: False)
    monkeypatch.setattr(datadir_mod, "backing_device", lambda p: "")
    with pytest.raises(DatadirError, match="cannot determine the filesystem"):
        resolve_datadir(
            _args(datadir=str(tmp_path), datadir_require_separate_filesystem=True)
        )
