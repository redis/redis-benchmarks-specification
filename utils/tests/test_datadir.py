import argparse
import os
import tempfile

import pytest

from redis_benchmarks_specification.__common__.datadir import (
    DatadirError,
    add_datadir_arguments,
    collect_storage_metadata,
    resolve_datadir,
    storage_backend_label,
)


def _args(**kwargs):
    ns = argparse.Namespace(datadir=None, datadir_require_mountpoint=False)
    for k, v in kwargs.items():
        setattr(ns, k, v)
    return ns


def test_defaults_to_home_when_not_requested():
    assert resolve_datadir(_args()) == os.path.expanduser("~")


def test_empty_string_is_treated_as_unset():
    assert resolve_datadir(_args(datadir="   ")) == os.path.expanduser("~")


def test_missing_attributes_do_not_break_callers():
    """Entrypoints that have not been rebuilt yet must keep working."""
    assert resolve_datadir(argparse.Namespace()) == os.path.expanduser("~")


def test_existing_directory_is_used():
    with tempfile.TemporaryDirectory() as d:
        assert resolve_datadir(_args(datadir=d)) == os.path.abspath(d)


def test_user_expansion():
    assert resolve_datadir(_args(datadir="~")) == os.path.expanduser("~")


def test_relative_path_is_absolutised():
    with tempfile.TemporaryDirectory() as d:
        cwd = os.getcwd()
        try:
            os.chdir(d)
            os.mkdir("sub")
            assert resolve_datadir(_args(datadir="sub")) == os.path.join(
                os.path.realpath(d), "sub"
            ) or resolve_datadir(_args(datadir="sub")) == os.path.join(d, "sub")
        finally:
            os.chdir(cwd)


def test_missing_datadir_aborts_rather_than_falling_back():
    """The whole point: never silently use the home directory instead."""
    with tempfile.TemporaryDirectory() as d:
        missing = os.path.join(d, "does-not-exist")
        with pytest.raises(DatadirError) as excinfo:
            resolve_datadir(_args(datadir=missing))
        assert missing in str(excinfo.value)
        # and it must not have quietly answered with $HOME
        assert os.path.expanduser("~") not in str(excinfo.value).split(missing)[0]


def test_file_instead_of_directory_aborts():
    with tempfile.NamedTemporaryFile() as f:
        with pytest.raises(DatadirError, match="not a directory"):
            resolve_datadir(_args(datadir=f.name))


@pytest.mark.skipif(os.geteuid() == 0, reason="root bypasses write permission checks")
def test_unwritable_datadir_aborts():
    with tempfile.TemporaryDirectory() as d:
        sub = os.path.join(d, "ro")
        os.mkdir(sub)
        os.chmod(sub, 0o500)
        try:
            with pytest.raises(DatadirError, match="not writable"):
                resolve_datadir(_args(datadir=sub))
        finally:
            os.chmod(sub, 0o700)


def test_require_mountpoint_rejects_a_plain_directory():
    with tempfile.TemporaryDirectory() as d:
        with pytest.raises(DatadirError, match="not a mountpoint"):
            resolve_datadir(_args(datadir=d, datadir_require_mountpoint=True))


def test_require_mountpoint_accepts_a_real_mountpoint():
    writable_mount = next(
        (
            p
            for p in ("/dev/shm", "/tmp", "/run/shm")
            if os.path.ismount(p) and os.access(p, os.W_OK | os.X_OK)
        ),
        None,
    )
    if writable_mount is None:
        pytest.skip("no writable mountpoint available in this environment")
    assert (
        resolve_datadir(_args(datadir=writable_mount, datadir_require_mountpoint=True))
        == writable_mount
    )


def test_require_mountpoint_without_datadir_is_an_error():
    with pytest.raises(DatadirError, match="without --datadir"):
        resolve_datadir(_args(datadir_require_mountpoint=True))


def test_arguments_are_registered_and_default_to_unset():
    parser = argparse.ArgumentParser()
    add_datadir_arguments(parser)
    parsed = parser.parse_args([])
    assert parsed.datadir is None
    assert parsed.datadir_require_mountpoint is False

    parsed = parser.parse_args(
        ["--datadir", "/mnt/nvme", "--datadir-require-mountpoint"]
    )
    assert parsed.datadir == "/mnt/nvme"
    assert parsed.datadir_require_mountpoint is True


def test_collect_storage_metadata_describes_the_path():
    with tempfile.TemporaryDirectory() as d:
        md = collect_storage_metadata(d)
        assert md["datadir"] == os.path.abspath(d)
        for key in (
            "device",
            "is_mountpoint",
            "mountpoint",
            "fstype",
            "mount_options",
            "physically_attached",
        ):
            assert key in md
        assert isinstance(md["physically_attached"], bool)
        assert isinstance(md["is_mountpoint"], bool)


def test_storage_backend_label_distinguishes_backends():
    assert storage_backend_label({"is_mountpoint": False}) == "unknown"
    assert (
        storage_backend_label({"is_mountpoint": True, "physically_attached": True})
        == "instance-store"
    )
    assert (
        storage_backend_label({"is_mountpoint": True, "physically_attached": False})
        == "network-attached"
    )
