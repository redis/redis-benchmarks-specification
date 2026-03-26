#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import datetime
import logging
import os
import socket
import subprocess
import redis

from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
    STREAM_GH_EVENTS_COMMIT_BUILDERS_CG,
    STREAM_KEYNAME_NEW_BUILD_EVENTS,
    STREAM_GH_NEW_BUILD_RUNNERS_CG,
    get_arch_specific_stream_name,
)


def _get_caller_identity():
    """Get identity of who is running this CLI command.

    Returns a string like: "fcosta_oliveira@myhost (cli)"
    or "github-actions@runner-abc (gh-actions)" if in CI.
    """
    # Check if running in GitHub Actions
    gh_actor = os.getenv("GITHUB_ACTOR", "")
    gh_run_id = os.getenv("GITHUB_RUN_ID", "")
    if gh_actor:
        source = "gh-actions"
        if gh_run_id:
            source = f"gh-actions/run/{gh_run_id}"
        return f"{gh_actor} ({source})"

    # Local CLI: use git user + hostname
    user = ""
    try:
        result = subprocess.run(
            ["git", "config", "user.name"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            user = result.stdout.strip()
    except Exception:
        pass
    if not user:
        user = os.getenv("USER", "unknown")
    host = socket.gethostname()
    return f"{user}@{host} (cli)"


def _format_idle(idle_ms):
    """Format idle time in milliseconds to human-readable string."""
    if idle_ms < 1000:
        return f"{idle_ms}ms"
    secs = idle_ms / 1000
    if secs < 60:
        return f"{secs:.0f}s"
    mins = secs / 60
    if mins < 60:
        return f"{mins:.0f}m"
    hours = mins / 60
    if hours < 24:
        return f"{hours:.1f}h"
    days = hours / 24
    return f"{days:.1f}d"


def _format_age(timestamp_ms):
    """Format a stream ID timestamp to age string."""
    try:
        ts = int(str(timestamp_ms).split("-")[0])
        dt = datetime.datetime.utcfromtimestamp(ts / 1000)
        age = datetime.datetime.utcnow() - dt
        secs = age.total_seconds()
        if secs < 0:
            return "just now"
        if secs < 60:
            return f"{secs:.0f}s ago"
        mins = secs / 60
        if mins < 60:
            return f"{mins:.0f}m ago"
        hours = mins / 60
        if hours < 24:
            return f"{hours:.1f}h ago"
        days = hours / 24
        return f"{days:.1f}d ago"
    except Exception:
        return "unknown"


def _get_build_info(conn, stream_id):
    """Resolve a benchmark stream ID to git hash/branch/org/repo via XRANGE on build streams."""
    for arch in ["amd64", "arm64"]:
        build_stream = get_arch_specific_stream_name(arch)
        try:
            entries = conn.xrange(build_stream, min=stream_id, max=stream_id)
            if entries:
                _, fields = entries[0]
                info = {}
                for k in [
                    b"git_hash",
                    b"git_branch",
                    b"github_org",
                    b"github_repo",
                    b"tests_regexp",
                    b"deployment_name_regexp",
                    b"run_image",
                    b"server_name",
                    b"pull_request",
                    b"triggered_by",
                ]:
                    if k in fields:
                        v = fields[k]
                        info[k.decode()] = v.decode() if isinstance(v, bytes) else v
                return info
        except Exception:
            pass
    return {}


def _get_commit_info(conn, stream_id):
    """Resolve a commit stream ID to git hash/branch via XRANGE on commit stream."""
    try:
        entries = conn.xrange(
            STREAM_KEYNAME_GH_EVENTS_COMMIT, min=stream_id, max=stream_id
        )
        if entries:
            _, fields = entries[0]
            info = {}
            for k in [
                b"git_hash",
                b"git_branch",
                b"github_org",
                b"github_repo",
                b"pull_request",
                b"tests_regexp",
                b"deployment_name_regexp",
                b"triggered_by",
            ]:
                if k in fields:
                    v = fields[k]
                    info[k.decode()] = v.decode() if isinstance(v, bytes) else v
            return info
    except Exception:
        pass
    return {}


def _short_hash(h):
    return h[:10] if h and len(h) > 10 else (h or "")


def _short_info(info):
    """One-line summary from build/commit info dict."""
    if not info:
        return ""
    org = info.get("github_org", "")
    repo = info.get("github_repo", "")
    branch = info.get("git_branch", "")
    ghash = _short_hash(info.get("git_hash", ""))
    pr = info.get("pull_request", "")

    parts = []
    if org and org != "redis":
        parts.append(f"{org}/{repo}")
    elif repo and repo != "redis":
        parts.append(repo)
    if branch:
        parts.append(branch)
    if ghash:
        parts.append(ghash)
    if pr:
        parts.append(f"PR#{pr}")
    return " ".join(parts)


def _get_queue_progress(conn, stream_id, platform):
    """Get test queue progress for a stream_id on a platform.

    Note: failed is a subset of completed (coordinator pushes to both lists),
    so total = pending + running + completed (not + failed, which would double-count).
    """
    prefix = f"ci.benchmarks.redis/ci/redis/redis:benchmarks:{stream_id}:{platform}"
    pending = conn.llen(f"{prefix}:tests_pending")
    running = conn.llen(f"{prefix}:tests_running")
    completed = conn.llen(f"{prefix}:tests_completed")
    failed = conn.llen(f"{prefix}:tests_failed")
    total = pending + running + completed
    return pending, running, completed, failed, total


def admin_runners_command(conn, args):
    """List all runners with what they're processing, progress, and ETA."""
    print("\n=== RUNNERS ===\n")

    for arch in ["amd64", "arm64"]:
        stream = get_arch_specific_stream_name(arch)
        try:
            conn.xinfo_stream(stream)
        except redis.exceptions.ResponseError:
            continue

        groups = conn.xinfo_groups(stream)
        if not groups:
            continue

        for group in groups:
            group_name = group.get("name", "")
            if isinstance(group_name, bytes):
                group_name = group_name.decode()
            group_pending = group.get("pending", 0)
            group_consumers = group.get("consumers", 0)
            lag = group.get("lag", 0)

            # Extract platform name
            prefix = f"{STREAM_GH_NEW_BUILD_RUNNERS_CG}-"
            platform = group_name[len(prefix) :] if prefix in group_name else group_name

            if group_consumers == 0:
                continue

            consumers = conn.xinfo_consumers(stream, group_name)
            for consumer in consumers:
                c_pending = consumer.get("pending", 0)
                c_idle = consumer.get("idle", 0)

                status = "IDLE"
                if c_pending > 0:
                    status = "WORKING"
                if c_idle > 300000:
                    status = "STALE" if c_pending == 0 else "STUCK"

                idle_str = _format_idle(c_idle)
                queue_depth = c_pending + lag

                print(f"  {platform} ({arch})")
                print(
                    f"    Status: {status}  |  Last active: {idle_str} ago  |  Queue depth: {queue_depth} (pending: {c_pending}, lag: {lag})"
                )

                # Show what the runner is currently processing
                if c_pending > 0:
                    pending_msgs = conn.xpending_range(stream, group_name, "-", "+", 10)
                    for msg in pending_msgs:
                        msg_id = msg.get("message_id", b"")
                        if isinstance(msg_id, bytes):
                            msg_id = msg_id.decode()
                        idle = msg.get("time_since_delivered", 0)

                        # Resolve stream ID to build info
                        build_info = _get_build_info(conn, msg_id)
                        info_str = _short_info(build_info)

                        # Get queue progress on this platform
                        pend, run, done, fail, total = _get_queue_progress(
                            conn, msg_id, platform
                        )
                        if total > 0:
                            pct = int(done / total * 100) if total > 0 else 0
                            progress = f"{done}/{total} ({pct}%)"

                            # ETA for this run
                            eta_str = ""
                            if done > 0 and (pend + run) > 0:
                                try:
                                    ts = int(msg_id.split("-")[0])
                                    elapsed = (
                                        datetime.datetime.utcnow()
                                        - datetime.datetime.utcfromtimestamp(ts / 1000)
                                    ).total_seconds()
                                    avg = elapsed / done
                                    eta = avg * (pend + run)
                                    eta_str = f"  ETA: ~{int(eta / 60)}m"
                                except Exception:
                                    pass

                            # Currently running test name
                            running_key = f"ci.benchmarks.redis/ci/redis/redis:benchmarks:{msg_id}:{platform}:tests_running"
                            running_tests = conn.lrange(running_key, 0, 0)
                            running_name = ""
                            if running_tests:
                                rn = running_tests[0]
                                running_name = (
                                    rn.decode() if isinstance(rn, bytes) else rn
                                )

                            print(
                                f"    Processing: {msg_id}  ({_format_idle(idle)} elapsed)"
                            )
                            print(f"      {info_str}")
                            print(f"      Progress: {progress}{eta_str}")
                            if running_name:
                                print(f"      Running:  {running_name}")
                            if fail > 0:
                                print(f"      Failed:   {fail}")
                        else:
                            print(f"    Queued: {msg_id}  (not started yet)")
                            if info_str:
                                print(f"      {info_str}")
                elif lag > 0:
                    print(f"    {lag} benchmark streams waiting in queue")

                print()


def admin_builders_command(conn, args):
    """List all builders with what they're building."""
    print("\n=== BUILDERS ===\n")

    stream = STREAM_KEYNAME_GH_EVENTS_COMMIT
    try:
        stream_info = conn.xinfo_stream(stream)
        stream_len = stream_info.get("length", 0)
    except redis.exceptions.ResponseError as e:
        print(f"  Cannot read stream {stream}: {e}")
        return

    groups = conn.xinfo_groups(stream)
    if not groups:
        print("  No builder groups found")
        return

    for group in groups:
        group_name = group.get("name", "")
        if isinstance(group_name, bytes):
            group_name = group_name.decode()
        group_pending = group.get("pending", 0)
        group_consumers = group.get("consumers", 0)
        lag = group.get("lag", 0)

        if group_consumers == 0:
            continue

        consumers = conn.xinfo_consumers(stream, group_name)
        for consumer in consumers:
            c_name = consumer.get("name", "")
            if isinstance(c_name, bytes):
                c_name = c_name.decode()
            c_pending = consumer.get("pending", 0)
            c_idle = consumer.get("idle", 0)

            # Skip dead consumers (idle > 30 days)
            if c_idle > 30 * 24 * 60 * 60 * 1000:
                idle_str = _format_idle(c_idle)
                print(f"  {group_name}")
                print(
                    f"    Status: DEAD  |  Last active: {idle_str} ago  |  Stuck pending: {c_pending}"
                )
                print()
                continue

            status = "IDLE"
            if c_pending > 0:
                status = "WORKING"
            if c_idle > 300000:
                status = "STALE" if c_pending == 0 else "STUCK"

            idle_str = _format_idle(c_idle)
            print(f"  {group_name}")
            print(
                f"    Status: {status}  |  Last active: {idle_str} ago  |  Pending: {c_pending}  |  Queue: {lag}"
            )

            # Show recent pending work with commit info
            if c_pending > 0:
                pending_msgs = conn.xpending_range(stream, group_name, "-", "+", 5)
                for msg in pending_msgs:
                    msg_id = msg.get("message_id", b"")
                    if isinstance(msg_id, bytes):
                        msg_id = msg_id.decode()
                    idle = msg.get("time_since_delivered", 0)
                    commit_info = _get_commit_info(conn, msg_id)
                    info_str = _short_info(commit_info)
                    print(f"      {msg_id}  ({_format_idle(idle)} ago)  {info_str}")
                if c_pending > 5:
                    print(f"      ... and {c_pending - 5} more")

            print()


def admin_queues_command(conn, args):
    """List all benchmark runs per platform with git info."""
    print("\n=== BENCHMARK QUEUES ===\n")

    platform_filter = args.platform if args.platform else None

    # Discover platforms from zset keys
    platform_keys = conn.keys("ci.benchmarks.redis/ci/redis/redis:benchmarks:*:zset")
    if not platform_keys:
        print("  No platform data found.")
        return

    platforms = {}
    for pk in platform_keys:
        pk_str = pk.decode() if isinstance(pk, bytes) else pk
        parts = pk_str.split(":")
        platform_name = parts[-2]
        platforms[platform_name] = pk_str

    # Find which stream each runner is currently processing
    runner_active = {}  # stream_id -> set of platforms actively processing it
    for arch in ["amd64", "arm64"]:
        build_stream = get_arch_specific_stream_name(arch)
        try:
            groups = conn.xinfo_groups(build_stream)
        except redis.exceptions.ResponseError:
            continue
        for group in groups:
            group_name = group.get("name", "")
            if isinstance(group_name, bytes):
                group_name = group_name.decode()
            grp_prefix = f"{STREAM_GH_NEW_BUILD_RUNNERS_CG}-"
            if grp_prefix not in group_name:
                continue
            plat = group_name[len(grp_prefix) :]
            pending_msgs = conn.xpending_range(build_stream, group_name, "-", "+", 5)
            for msg in pending_msgs:
                msg_id = msg.get("message_id", b"")
                if isinstance(msg_id, bytes):
                    msg_id = msg_id.decode()
                if msg_id not in runner_active:
                    runner_active[msg_id] = set()
                runner_active[msg_id].add(plat)

    for platform_name in sorted(platforms.keys()):
        if platform_filter and platform_name != platform_filter:
            continue

        zset_key = platforms[platform_name]

        # Get recent benchmark runs (last 7 days)
        now_ms = int(datetime.datetime.utcnow().timestamp() * 1000)
        week_ago_ms = now_ms - (7 * 24 * 60 * 60 * 1000)
        stream_ids = conn.zrangebyscore(zset_key, week_ago_ms, "+inf", withscores=True)

        if not stream_ids:
            continue

        print(f"Platform: {platform_name}  ({len(stream_ids)} runs in last 7d)\n")
        print(
            f"  {'STREAM ID':<25} {'AGE':<8} {'PROGRESS':<14} {'STATUS':<12} {'COMMIT'}"
        )
        print(f"  {'-'*25} {'-'*8} {'-'*14} {'-'*12} {'-'*40}")

        for sid_raw, score in reversed(stream_ids):
            sid = sid_raw.decode() if isinstance(sid_raw, bytes) else sid_raw
            pend, run, done, fail, total = _get_queue_progress(conn, sid, platform_name)
            age = _format_age(sid)

            if total == 0:
                status = "QUEUED"
            elif pend == 0 and run == 0:
                status = "DONE" if fail == 0 else f"DONE ({fail}F)"
            elif run > 0:
                # Is the runner actively processing THIS stream?
                if sid in runner_active and platform_name in runner_active[sid]:
                    status = "RUNNING *"
                else:
                    status = "RUNNING"
            elif pend > 0 and run == 0 and done == 0:
                status = "STALLED"
            else:
                status = "IN_PROGRESS"

            progress = f"{done}/{total}" if total > 0 else "-"
            if fail > 0 and (pend > 0 or run > 0):
                progress += f" ({fail}F)"

            # Resolve git info
            build_info = _get_build_info(conn, sid)
            info_str = _short_info(build_info)

            print(f"  {sid:<25} {age:<8} {progress:<14} {status:<12} {info_str}")

        print()


def admin_status_command(conn, args):
    """Show detailed status for a specific benchmark run."""
    stream_id = args.stream_id
    if not stream_id:
        print("Error: --stream-id is required for status command")
        return

    platform_filter = args.platform if args.platform else None

    print(f"\n=== BENCHMARK STATUS: {stream_id} ===\n")

    # Resolve build info
    build_info = _get_build_info(conn, stream_id)
    if build_info:
        print(
            f"  Commit:  {build_info.get('github_org', '')}/{build_info.get('github_repo', '')} {build_info.get('git_branch', '')} {_short_hash(build_info.get('git_hash', ''))}"
        )
        if build_info.get("pull_request"):
            print(f"  PR:      #{build_info['pull_request']}")
        if build_info.get("tests_regexp", ".*") != ".*":
            print(f"  Filter:  {build_info['tests_regexp']}")
        if build_info.get("deployment_name_regexp", ".*") != ".*":
            print(f"  Topo:    {build_info['deployment_name_regexp']}")
        if build_info.get("triggered_by"):
            print(f"  By:      {build_info['triggered_by']}")
        print()

    # Find which platform(s) this stream is on
    platform_keys = conn.keys("ci.benchmarks.redis/ci/redis/redis:benchmarks:*:zset")
    found_platforms = []
    for pk in platform_keys:
        pk_str = pk.decode() if isinstance(pk, bytes) else pk
        parts = pk_str.split(":")
        platform_name = parts[-2]
        if platform_filter and platform_name != platform_filter:
            continue
        score = conn.zscore(pk_str, stream_id)
        if score is not None:
            found_platforms.append(platform_name)

    if not found_platforms:
        print(f"  Not picked up by any coordinator yet.")
        if build_info:
            print(f"  (Waiting in build stream for a runner to claim it)")
        return

    for platform_name in found_platforms:
        prefix = (
            f"ci.benchmarks.redis/ci/redis/redis:benchmarks:{stream_id}:{platform_name}"
        )

        pending = conn.llen(f"{prefix}:tests_pending")
        running = conn.llen(f"{prefix}:tests_running")
        completed = conn.llen(f"{prefix}:tests_completed")
        failed = conn.llen(f"{prefix}:tests_failed")
        # failed is a subset of completed, so don't double-count
        total = pending + running + completed

        triggered = _format_age(stream_id)
        pct = int(completed / total * 100) if total > 0 else 0

        print(f"  Platform:  {platform_name}")
        print(f"  Triggered: {triggered}")
        print(
            f"  Progress:  {completed}/{total} ({pct}%) completed, {running} running, {pending} pending, {failed} failed"
        )

        # ETA
        if completed > 0 and (pending + running) > 0:
            try:
                ts = int(stream_id.split("-")[0])
                elapsed = (
                    datetime.datetime.utcnow()
                    - datetime.datetime.utcfromtimestamp(ts / 1000)
                ).total_seconds()
                avg_per_test = elapsed / completed
                eta_secs = avg_per_test * (pending + running)
                eta_min = int(eta_secs / 60)
                print(f"  ETA:       ~{eta_min}m  (avg {avg_per_test:.0f}s/test)")
            except Exception:
                pass

        # Show running tests
        if running > 0:
            running_tests = conn.lrange(f"{prefix}:tests_running", 0, -1)
            for t in running_tests:
                t_str = t.decode() if isinstance(t, bytes) else t
                print(f"  Running:   {t_str}")

        # Show failed tests
        if failed > 0:
            print(f"  Failed tests:")
            failed_tests = conn.lrange(f"{prefix}:tests_failed", 0, -1)
            for t in failed_tests:
                t_str = t.decode() if isinstance(t, bytes) else t
                print(f"    - {t_str}")

        # Topology-level tracking
        topo_p = conn.llen(f"{prefix}:topologies_pending")
        topo_r = conn.llen(f"{prefix}:topologies_running")
        topo_d = conn.llen(f"{prefix}:topologies_completed")
        topo_f = conn.llen(f"{prefix}:topologies_failed")
        # failed is subset of completed for topologies too
        topo_total = topo_p + topo_r + topo_d

        if topo_total > 0:
            print(
                f"  Topologies: {topo_d}/{topo_total} completed, {topo_r} running, {topo_p} pending, {topo_f} failed"
            )

            if topo_r > 0:
                topo_running = conn.lrange(f"{prefix}:topologies_running", 0, -1)
                for t in topo_running:
                    t_str = t.decode() if isinstance(t, bytes) else t
                    print(f"    Running: {t_str}")

            if topo_f > 0:
                topo_failed = conn.lrange(f"{prefix}:topologies_failed", 0, -1)
                for t in topo_failed:
                    t_str = t.decode() if isinstance(t, bytes) else t
                    print(f"    Failed:  {t_str}")

        print()


def admin_summary_command(conn, args):
    """Fleet overview: all runners, active work, queue depth."""
    print("\n=== FLEET SUMMARY ===\n")

    runners = []
    total_lag = 0

    for arch in ["amd64", "arm64"]:
        stream = get_arch_specific_stream_name(arch)
        try:
            conn.xinfo_stream(stream)
        except redis.exceptions.ResponseError:
            continue

        groups = conn.xinfo_groups(stream)
        for group in groups:
            group_name = group.get("name", "")
            if isinstance(group_name, bytes):
                group_name = group_name.decode()
            lag = group.get("lag", 0)
            total_lag += lag

            grp_prefix = f"{STREAM_GH_NEW_BUILD_RUNNERS_CG}-"
            if grp_prefix not in group_name:
                continue
            platform = group_name[len(grp_prefix) :]

            consumers = conn.xinfo_consumers(stream, group_name)
            for consumer in consumers:
                c_pending = consumer.get("pending", 0)
                c_idle = consumer.get("idle", 0)

                status = "IDLE"
                if c_pending > 0:
                    status = "WORKING"
                if c_idle > 300000:
                    status = "STALE" if c_pending == 0 else "STUCK"

                # Get active queue progress
                active_info = ""
                if c_pending > 0:
                    pending_msgs = conn.xpending_range(stream, group_name, "-", "+", 1)
                    if pending_msgs:
                        msg_id = pending_msgs[0].get("message_id", b"")
                        if isinstance(msg_id, bytes):
                            msg_id = msg_id.decode()
                        pend, run, done, fail, total = _get_queue_progress(
                            conn, msg_id, platform
                        )
                        if total > 0:
                            build_info = _get_build_info(conn, msg_id)
                            info_str = _short_info(build_info)
                            active_info = f"{done}/{total} done  {info_str}"

                runners.append(
                    {
                        "platform": platform,
                        "arch": arch,
                        "status": status,
                        "idle": c_idle,
                        "pending": c_pending,
                        "lag": lag,
                        "active_info": active_info,
                    }
                )

    working = sum(1 for r in runners if r["status"] == "WORKING")
    idle = sum(1 for r in runners if r["status"] == "IDLE")
    stuck = sum(1 for r in runners if r["status"] == "STUCK")
    stale = sum(1 for r in runners if r["status"] == "STALE")

    print(
        f"  Runners: {len(runners)} total  |  {working} working  |  {idle} idle  |  {stuck} stuck  |  {stale} stale"
    )
    print(f"  Queue:   {total_lag} benchmark streams waiting")
    print()

    # Table
    print(f"  {'PLATFORM':<45} {'ARCH':<6} {'STATUS':<8} {'IDLE':<8} {'ACTIVE WORK'}")
    print(f"  {'-'*45} {'-'*6} {'-'*8} {'-'*8} {'-'*40}")
    for r in sorted(runners, key=lambda x: x["status"]):
        idle_str = _format_idle(r["idle"])
        print(
            f"  {r['platform']:<45} {r['arch']:<6} {r['status']:<8} {idle_str:<8} {r['active_info']}"
        )

    # Builder status
    print()
    print("  Builders:")
    stream = STREAM_KEYNAME_GH_EVENTS_COMMIT
    try:
        groups = conn.xinfo_groups(stream)
        for group in groups:
            group_name = group.get("name", "")
            if isinstance(group_name, bytes):
                group_name = group_name.decode()
            group_pending = group.get("pending", 0)
            consumers = conn.xinfo_consumers(stream, group_name)
            for consumer in consumers:
                c_idle = consumer.get("idle", 0)
                c_pending = consumer.get("pending", 0)
                if c_idle > 30 * 24 * 60 * 60 * 1000:
                    continue  # skip dead
                status = "IDLE"
                if c_pending > 0:
                    status = "WORKING"
                if c_idle > 300000:
                    status = "STALE" if c_pending == 0 else "STUCK"
                idle_str = _format_idle(c_idle)
                print(
                    f"    {group_name:<50} {status:<8} {idle_str:<8} pending: {c_pending}"
                )
    except Exception:
        pass

    print()


def admin_cancel_command(conn, args):
    """Cancel/flush a benchmark run's pending tests."""
    stream_id = args.stream_id
    if not stream_id:
        print("Error: --stream-id is required for cancel command")
        return

    platform_filter = args.platform if args.platform else None
    test_name_filter = args.tests_regexp if args.tests_regexp != ".*" else None

    platform_keys = conn.keys("ci.benchmarks.redis/ci/redis/redis:benchmarks:*:zset")
    for pk in platform_keys:
        pk_str = pk.decode() if isinstance(pk, bytes) else pk
        parts = pk_str.split(":")
        platform_name = parts[-2]
        if platform_filter and platform_name != platform_filter:
            continue
        score = conn.zscore(pk_str, stream_id)
        if score is None:
            continue

        prefix = (
            f"ci.benchmarks.redis/ci/redis/redis:benchmarks:{stream_id}:{platform_name}"
        )
        pending_key = f"{prefix}:tests_pending"
        topo_pending_key = f"{prefix}:topologies_pending"

        caller = _get_caller_identity()
        if test_name_filter:
            removed = conn.lrem(pending_key, 0, test_name_filter)
            print(
                f"Removed {removed} entries matching '{test_name_filter}' from {platform_name} pending queue"
            )
            logging.info(f"Cancel by: {caller}")
        else:
            # Signal the coordinator to break out of its test loop
            reset_key = f"{prefix}:reset_requested"
            conn.set(reset_key, f"canceled by {caller}", ex=3600)
            print(f"  Set reset signal for coordinator (by {caller})")

            pending_count = conn.llen(pending_key)
            topo_count = conn.llen(topo_pending_key)
            conn.delete(pending_key)
            conn.delete(topo_pending_key)
            print(
                f"Flushed {pending_count} pending tests and {topo_count} topology entries for {stream_id} on {platform_name}"
            )


def admin_skip_command(conn, args):
    """Skip all pending work for a runner (ACK pending messages + reset to latest).

    This makes the runner skip its current backlog and only pick up NEW work.
    The runner itself keeps running — it just won't process old queued streams.
    Also flushes all pending/running test queue lists for the skipped streams.
    """
    platform_filter = args.platform
    if not platform_filter:
        print("Error: --platform is required for skip command")
        print(
            "  Example: --tool admin --admin-command skip --platform arm-aws-m8g.metal-24xl"
        )
        return

    for arch in ["amd64", "arm64"]:
        stream = get_arch_specific_stream_name(arch)
        try:
            groups = conn.xinfo_groups(stream)
        except redis.exceptions.ResponseError:
            continue

        for group in groups:
            group_name = group.get("name", "")
            if isinstance(group_name, bytes):
                group_name = group_name.decode()

            grp_prefix = f"{STREAM_GH_NEW_BUILD_RUNNERS_CG}-"
            if grp_prefix not in group_name:
                continue
            platform = group_name[len(grp_prefix) :]

            if platform != platform_filter:
                continue

            caller = _get_caller_identity()
            print(f"\nSkipping all work for runner: {platform} ({arch})")
            print(f"  Requested by: {caller}")

            # 1. Get and ACK all pending messages
            pending_msgs = conn.xpending_range(stream, group_name, "-", "+", 1000)
            if pending_msgs:
                msg_ids = []
                for msg in pending_msgs:
                    msg_id = msg.get("message_id", b"")
                    if isinstance(msg_id, bytes):
                        msg_id = msg_id.decode()
                    msg_ids.append(msg_id)

                    # Signal the coordinator to break out of its test loop
                    # (checked every iteration alongside the HTTP _reset_queue_requested flag)
                    reset_key = f"ci.benchmarks.redis/ci/redis/redis:benchmarks:{msg_id}:{platform}:reset_requested"
                    conn.set(reset_key, f"skipped by {caller}", ex=3600)
                    print(f"  Set reset signal: {reset_key}")

                    # Also flush the test queue lists for this stream on this platform
                    prefix = f"ci.benchmarks.redis/ci/redis/redis:benchmarks:{msg_id}:{platform}"
                    for suffix in [
                        "tests_pending",
                        "tests_running",
                        "topologies_pending",
                        "topologies_running",
                    ]:
                        key = f"{prefix}:{suffix}"
                        count = conn.llen(key)
                        if count > 0:
                            conn.delete(key)
                            print(f"  Cleared {count} entries from {suffix}")

                # ACK all pending messages
                ack_count = conn.xack(
                    stream,
                    group_name,
                    *[m.encode() if isinstance(m, str) else m for m in msg_ids],
                )
                print(f"  ACK'd {ack_count} pending stream messages")

                # Show what was skipped
                for msg_id in msg_ids:
                    build_info = _get_build_info(conn, msg_id)
                    info_str = _short_info(build_info)
                    print(f"    Skipped: {msg_id}  {info_str}")
            else:
                print(f"  No pending messages to skip")

            # 2. Reset consumer group to latest
            try:
                conn.xgroup_setid(stream, group_name, id="$")
                print(f"  Reset consumer group to latest (will only process new work)")
            except Exception as e:
                print(f"  Error resetting group position: {e}")

            print(f"  Done. Runner will pick up only NEW benchmark streams.")
            return

    print(f"No runner found for platform: {platform_filter}")


def admin_reset_command(conn, args):
    """Reset a runner's consumer group — nuclear option.

    Deletes and recreates the consumer group. Use when a runner is completely stuck.
    This is equivalent to restarting the runner process from scratch.
    """
    platform_filter = args.platform
    if not platform_filter:
        print("Error: --platform is required for reset command")
        return

    for arch in ["amd64", "arm64"]:
        stream = get_arch_specific_stream_name(arch)
        try:
            groups = conn.xinfo_groups(stream)
        except redis.exceptions.ResponseError:
            continue

        for group in groups:
            group_name = group.get("name", "")
            if isinstance(group_name, bytes):
                group_name = group_name.decode()

            grp_prefix = f"{STREAM_GH_NEW_BUILD_RUNNERS_CG}-"
            if grp_prefix not in group_name:
                continue
            platform = group_name[len(grp_prefix) :]

            if platform != platform_filter:
                continue

            print(f"\nResetting consumer group for runner: {platform} ({arch})")

            # Show current state before reset
            pending_count = group.get("pending", 0)
            consumers_count = group.get("consumers", 0)
            print(
                f"  Current state: {consumers_count} consumers, {pending_count} pending messages"
            )

            # Destroy and recreate
            try:
                conn.xgroup_destroy(stream, group_name)
                print(f"  Destroyed consumer group: {group_name}")
            except Exception as e:
                print(f"  Error destroying group: {e}")

            try:
                conn.xgroup_create(stream, group_name, id="$", mkstream=True)
                print(f"  Recreated consumer group at latest position")
            except Exception as e:
                print(f"  Error recreating group: {e}")

            print(f"  Done. Runner will rejoin with a clean slate on next iteration.")
            return

    print(f"No runner found for platform: {platform_filter}")


def admin_work_command(conn, args):
    """Show all active/pending work across the fleet — work-centric view.

    For each benchmark stream, shows: who triggered it, fork/branch/hash,
    which platforms are running/pending/done, and progress per platform.
    """
    platform_filter = args.platform if args.platform else None
    if platform_filter:
        print(f"\n=== ACTIVE WORK (platform: {platform_filter}) ===\n")
    else:
        print("\n=== ACTIVE WORK ===\n")

    # 1. Collect all stream IDs from runner pending messages (= actively being processed)
    #    AND undelivered messages (= lag, waiting to be picked up by a runner)
    runner_streams = {}  # stream_id -> {arch, platforms_processing}
    for arch in ["amd64", "arm64"]:
        stream = get_arch_specific_stream_name(arch)
        try:
            groups = conn.xinfo_groups(stream)
        except redis.exceptions.ResponseError:
            continue

        for group in groups:
            group_name = group.get("name", "")
            if isinstance(group_name, bytes):
                group_name = group_name.decode()
            lag = group.get("lag", 0)
            last_delivered = group.get("last-delivered-id", b"0-0")
            if isinstance(last_delivered, bytes):
                last_delivered = last_delivered.decode()

            grp_prefix = f"{STREAM_GH_NEW_BUILD_RUNNERS_CG}-"
            if grp_prefix not in group_name:
                continue
            platform = group_name[len(grp_prefix) :]

            if platform_filter and platform_filter != platform:
                continue

            # Get pending messages (delivered but not ACK'd = runner is working on them)
            pending_msgs = conn.xpending_range(stream, group_name, "-", "+", 20)
            for msg in pending_msgs:
                msg_id = msg.get("message_id", b"")
                if isinstance(msg_id, bytes):
                    msg_id = msg_id.decode()
                c_idle = msg.get("time_since_delivered", 0)

                if msg_id not in runner_streams:
                    runner_streams[msg_id] = {"arch": arch, "platforms": {}}
                runner_streams[msg_id]["platforms"][platform] = {
                    "state": "processing",
                    "idle": c_idle,
                }

            # Get lag messages (not yet delivered = waiting in queue for this runner)
            if lag > 0 and last_delivered != "0-0":
                # Read messages after the last delivered ID (these are the lag)
                # Use exclusive range: (last_delivered to get messages AFTER it
                lag_start = last_delivered
                try:
                    lag_entries = conn.xrange(
                        stream, min=f"({lag_start}", count=min(lag, 20)
                    )
                    for entry_id, entry_fields in lag_entries:
                        entry_id_str = (
                            entry_id.decode()
                            if isinstance(entry_id, bytes)
                            else entry_id
                        )
                        if entry_id_str not in runner_streams:
                            runner_streams[entry_id_str] = {
                                "arch": arch,
                                "platforms": {},
                            }
                        if platform not in runner_streams[entry_id_str]["platforms"]:
                            runner_streams[entry_id_str]["platforms"][platform] = {
                                "state": "pending_queue",
                            }
                except Exception:
                    pass

    # 2. Also collect from active queues (streams that have test lists but may not be in pending)
    platform_keys = conn.keys("ci.benchmarks.redis/ci/redis/redis:benchmarks:*:zset")
    now_ms = int(datetime.datetime.utcnow().timestamp() * 1000)
    day_ago_ms = now_ms - (24 * 60 * 60 * 1000)

    for pk in platform_keys:
        pk_str = pk.decode() if isinstance(pk, bytes) else pk
        parts = pk_str.split(":")
        platform_name = parts[-2]

        if platform_filter and platform_filter != platform_name:
            continue

        stream_ids = conn.zrangebyscore(pk_str, day_ago_ms, "+inf")
        for sid_raw in stream_ids:
            sid = sid_raw.decode() if isinstance(sid_raw, bytes) else sid_raw
            pend, run, done, fail, total = _get_queue_progress(conn, sid, platform_name)
            if total == 0:
                continue  # no test data yet

            if sid not in runner_streams:
                runner_streams[sid] = {"arch": "unknown", "platforms": {}}

            if platform_name not in runner_streams[sid]["platforms"]:
                runner_streams[sid]["platforms"][platform_name] = {"state": "queued"}

            # Update with progress info
            runner_streams[sid]["platforms"][platform_name].update(
                {
                    "pending": pend,
                    "running": run,
                    "completed": done,
                    "failed": fail,
                    "total": total,
                }
            )

            # Determine state
            if pend == 0 and run == 0 and done > 0:
                runner_streams[sid]["platforms"][platform_name]["state"] = "done"
            elif run > 0:
                runner_streams[sid]["platforms"][platform_name]["state"] = "running"
            elif pend > 0:
                runner_streams[sid]["platforms"][platform_name]["state"] = "pending"

    # 2b. Collect pending builds (commits waiting to be built)
    builder_stream = STREAM_KEYNAME_GH_EVENTS_COMMIT
    try:
        builder_groups = conn.xinfo_groups(builder_stream)
        for group in builder_groups:
            group_name = group.get("name", "")
            if isinstance(group_name, bytes):
                group_name = group_name.decode()
            # Skip dead groups
            group_consumers = group.get("consumers", 0)
            if group_consumers == 0:
                continue
            consumers = conn.xinfo_consumers(builder_stream, group_name)
            is_dead = all(
                c.get("idle", 0) > 30 * 24 * 60 * 60 * 1000 for c in consumers
            )
            if is_dead:
                continue

            # Determine which arch this builder handles
            builder_arch = "amd64"
            if "arm64" in group_name or "arm" in group_name:
                builder_arch = "arm64"

            pending_msgs = conn.xpending_range(builder_stream, group_name, "-", "+", 20)
            for msg in pending_msgs:
                msg_id = msg.get("message_id", b"")
                if isinstance(msg_id, bytes):
                    msg_id = msg_id.decode()
                # Only show recent builds (last 7 days)
                try:
                    ts = int(msg_id.split("-")[0])
                    if ts < now_ms - (7 * 24 * 60 * 60 * 1000):
                        continue
                except Exception:
                    continue

                if msg_id not in runner_streams:
                    runner_streams[msg_id] = {"arch": builder_arch, "platforms": {}}
                # Mark as building — use the builder group name as "platform"
                short_builder = group_name.replace(":redis/redis/commits", "")
                if short_builder not in runner_streams[msg_id]["platforms"]:
                    runner_streams[msg_id]["platforms"][short_builder] = {
                        "state": "building",
                    }
    except Exception:
        pass

    # 3. Filter to only active work (not all done)
    active_streams = {}
    for sid, info in runner_streams.items():
        has_active = any(
            p.get("state")
            in (
                "processing",
                "running",
                "pending",
                "queued",
                "pending_queue",
                "building",
            )
            for p in info["platforms"].values()
        )
        if has_active:
            active_streams[sid] = info

    if not active_streams:
        print("  No active work found.")
        return

    # 4. Build table rows (one row per stream x platform)
    rows = []
    for sid in sorted(active_streams.keys(), reverse=True):
        info = active_streams[sid]

        # Resolve build info (try build stream first, then commit stream)
        build_info = _get_build_info(conn, sid)
        commit_info = _get_commit_info(conn, sid)
        combined = {**commit_info, **build_info}

        org = combined.get("github_org", "?")
        repo = combined.get("github_repo", "?")
        branch = combined.get("git_branch", "?")
        ghash = _short_hash(combined.get("git_hash", "?"))
        pr = combined.get("pull_request", "")
        triggered_by = combined.get("triggered_by", "")

        # Build compact source string: org/repo branch hash [PR#N] [by user (source)]
        if org == "?" and repo == "?":
            source = ghash  # only hash available
        elif org == "redis" and repo == "redis":
            source = branch
        else:
            source = f"{org}/{repo} {branch}"
        if ghash and ghash != "?" and ghash not in source:
            source += f" {ghash}"
        if pr:
            source += f" PR#{pr}"

        # Append trigger origin to source
        if triggered_by:
            source += f" by {triggered_by}"

        age = _format_age(sid)

        for platform, pinfo in sorted(info["platforms"].items()):
            state = pinfo.get("state", "?").upper()
            total = pinfo.get("total", 0)
            done = pinfo.get("completed", 0)
            run = pinfo.get("running", 0)
            pend = pinfo.get("pending", 0)
            fail = pinfo.get("failed", 0)

            # Progress string
            if total > 0:
                pct = int(done / total * 100)
                progress = f"{done}/{total} ({pct}%)"
            else:
                progress = "-"

            # Status
            if state == "DONE":
                status = "DONE"
            elif state in ("RUNNING", "PROCESSING"):
                status = "RUNNING"
                if total == 0:
                    status = "QUEUED"
            elif state in ("PENDING", "PENDING_QUEUE"):
                status = "PENDING"
            elif state == "BUILDING":
                status = "BUILDING"
            else:
                status = state

            # ETA
            eta = ""
            if done > 0 and (pend + run) > 0:
                try:
                    ts = int(sid.split("-")[0])
                    elapsed = (
                        datetime.datetime.utcnow()
                        - datetime.datetime.utcfromtimestamp(ts / 1000)
                    ).total_seconds()
                    avg = elapsed / done
                    eta_secs = avg * (pend + run)
                    eta = f"~{int(eta_secs / 60)}m"
                except Exception:
                    pass

            # Fail marker
            fail_str = str(fail) if fail > 0 else ""

            # Currently running test (short name)
            running_test = ""
            if run > 0:
                running_key = f"ci.benchmarks.redis/ci/redis/redis:benchmarks:{sid}:{platform}:tests_running"
                running_tests = conn.lrange(running_key, 0, 0)
                if running_tests:
                    rn = running_tests[0]
                    rn_str = rn.decode() if isinstance(rn, bytes) else rn
                    # Shorten: remove common prefix
                    running_test = rn_str.replace("memtier_benchmark-", "mt-")

            # Short platform name
            short_plat = platform
            short_plat = short_plat.replace("-aws-", "-")
            short_plat = short_plat.replace("-gcp-", "-")
            short_plat = short_plat.replace("-azure-cobalt-", "-az-")
            short_plat = short_plat.replace("Standard_", "")

            rows.append(
                {
                    "age": age,
                    "source": source,
                    "platform": short_plat,
                    "status": status,
                    "progress": progress,
                    "fail": fail_str,
                    "eta": eta,
                    "running": running_test,
                }
            )

    # 5. Print table
    # Header
    print(
        f"  {'AGE':<8} {'STATUS':<9} {'PROGRESS':<12} {'FAIL':>4} {'ETA':<6} {'PLATFORM':<30} {'SOURCE':<55} {'RUNNING TEST'}"
    )
    print(f"  {'-'*8} {'-'*9} {'-'*12} {'-'*4} {'-'*6} {'-'*30} {'-'*55} {'-'*30}")
    for r in rows:
        if r["status"] == "DONE":
            continue  # skip completed work in work view
        print(
            f"  {r['age']:<8} {r['status']:<9} {r['progress']:<12} {r['fail']:>4} {r['eta']:<6} {r['platform']:<30} {r['source']:<55} {r['running']}"
        )
    print()


def admin_command_logic(args, project_name, project_version):
    """Main admin command dispatcher."""
    logging.info(f"Using: {project_name} {project_version}")

    admin_cmd = args.admin_command
    if not admin_cmd:
        print("Error: --admin-command is required.")
        print()
        print("Commands:")
        print("  summary   Fleet overview: all runners, builders, queue depth")
        print(
            "  work      All active/pending work: who triggered, fork/branch, platforms, progress"
        )
        print("  runners   Show each runner, what it's processing, progress + ETA")
        print("  builders  Show each builder, what it's building")
        print("  queues    List benchmark runs per platform with git info")
        print("  status    Detailed status for a specific benchmark run")
        print("  cancel    Flush pending tests for a specific benchmark run")
        print("  skip      Skip all queued work for a runner (ACK + reset to latest)")
        print("  reset     Nuclear reset: destroy + recreate runner consumer group")
        print()
        print("Examples:")
        print("  --tool admin --admin-command summary")
        print("  --tool admin --admin-command runners")
        print("  --tool admin --admin-command queues --platform arm-aws-m8g.metal-24xl")
        print("  --tool admin --admin-command status --stream-id 1774518430559-0")
        print(
            "  --tool admin --admin-command cancel --stream-id <id> [--platform <name>]"
        )
        print("  --tool admin --admin-command skip --platform arm-aws-m8g.metal-24xl")
        print("  --tool admin --admin-command reset --platform arm-aws-m8g.metal-24xl")
        return

    conn = redis.StrictRedis(
        host=args.redis_host,
        port=args.redis_port,
        password=args.redis_pass,
        username=args.redis_user,
        decode_responses=False,
    )
    conn.ping()

    if admin_cmd == "runners":
        admin_runners_command(conn, args)
    elif admin_cmd == "builders":
        admin_builders_command(conn, args)
    elif admin_cmd == "queues":
        admin_queues_command(conn, args)
    elif admin_cmd == "status":
        admin_status_command(conn, args)
    elif admin_cmd == "cancel":
        admin_cancel_command(conn, args)
    elif admin_cmd == "summary":
        admin_summary_command(conn, args)
    elif admin_cmd == "work":
        admin_work_command(conn, args)
    elif admin_cmd == "skip":
        admin_skip_command(conn, args)
    elif admin_cmd == "reset":
        admin_reset_command(conn, args)
    else:
        print(f"Unknown admin command: {admin_cmd}")
        print(
            "Available: summary, runners, builders, queues, status, cancel, skip, reset"
        )
