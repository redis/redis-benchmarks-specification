"""Explicit declaration of the Redis Stream field contract between components.

Why this module exists
----------------------
The trigger CLI, the builder, and the coordinator communicate exclusively through Redis
Streams, using bare string literals as field names on both sides:

    producer:  build_stream_fields["tests_priority_upper_limit"] = ...
    consumer:  if b"priority_upper_limit" in testDetails: ...

Nothing links those two literals, so a rename or a typo on either side does not fail — the
consumer's ``in testDetails`` check is simply False and it silently falls back to a default.
That is precisely how the per-run priority cap came to be inert: the builder writes
``tests_priority_upper_limit`` (``__builder__/builder.py``) while the coordinator read
``priority_upper_limit``, so ``--tests-priority-upper-limit`` had no effect on any run and the
"detected a priority_upper_limit definition on the streamdata" log line never fired.

The declarations below are the single source of truth. ``utils/tests/test_stream_contract.py``
asserts that every field a consumer reads is a field some producer writes, and specifically
flags *near-miss* pairs (one name a prefix/suffix of another), which is the shape this class of
bug takes.

Adding a field
--------------
1. add it to the relevant ``*_STREAM_FIELDS`` set below,
2. write it in the producer,
3. read it in the consumer via :func:`read_stream_field`.

Skipping step 1 makes the contract test fail, which is the point.
"""

import logging

# Fields the trigger CLI writes onto the commits stream (consumed by the builder).
COMMITS_STREAM_FIELDS = frozenset(
    {
        "git_hash",
        "git_branch",
        "git_version",
        "git_timestamp_ms",
        "commit_summary",
        "commit_datetime",
        "ref_label",
        "github_org",
        "github_repo",
        "tests_regexp",
        "tests_groups_regexp",
        "tests_priority_upper_limit",
        "tests_priority_lower_limit",
        "deployment_name_regexp",
        "command_regexp",
        "pull_request",
        "zip_archive_key",
        "zip_archive_len",
        "use_git_timestamp",
        "server_name",
        "run_image",
        "build_image",
        "build_command",
        "build_artifacts",
        "build_arch",
        "arch",
        "target_platform",
        "triggered_by",
        "metadata",
        "compiler",
        "cpp_compiler",
        "build_vars",
        "override_deployment_regexp",
        "executable",
        "docker_air_gap",
        "platform",
        "build_timeout",
        "mnt_point",
        "replayed_from",
        "restore_build_artifacts",
        "id",
    }
)

#: Fields a consumer reads off a stream that **no in-repo producer writes**.
#:
#: This is recorded debt, not an allow-list of good practice. Each entry is either a dead
#: consumer branch or a field expected from an out-of-repo producer; either way the setting
#: cannot currently be exercised through the normal CLI path. The contract test asserts this set
#: matches reality **exactly**, so a new orphan fails CI and removing one requires deleting it
#: here — a ratchet in both directions.
#:
#: - ``executable``: read at ``__self_contained_coordinator__/self_contained_coordinator.py``
#:   (``if b"executable" in testDetails``) but written nowhere in this repository.
#: - ``docker_air_gap``: read off the stream by the coordinator to override the per-run default,
#:   but only ever *written* into the runner heartbeat hash — never into a stream payload. So the
#:   documented per-test override is unreachable via the CLI.
KNOWN_UNPRODUCED_CONSUMED_FIELDS = frozenset(
    {
        "executable",
        "docker_air_gap",
    }
)

# Fields the builder writes onto the builds stream (consumed by the coordinator).
BUILDS_STREAM_FIELDS = COMMITS_STREAM_FIELDS

#: Documented aliases: ``canonical -> additional accepted names``.
#:
#: ``tests_priority_{upper,lower}_limit`` is the canonical name because that is what every
#: producer has always written. The un-prefixed spelling is accepted so that any consumer
#: written against the old (never-functional) read path keeps working, and so that stream
#: entries already in flight — including those produced by coordinators still running an older
#: pinned release — are honoured rather than silently ignored.
FIELD_ALIASES = {
    "tests_priority_upper_limit": ("priority_upper_limit",),
    "tests_priority_lower_limit": ("priority_lower_limit",),
}


def read_stream_field(stream_fields, name, default=None, cast=None):
    """Read ``name`` from a Redis Stream entry, honouring documented aliases.

    Stream entries come back from redis-py with **bytes** keys and bytes values. Callers
    previously hand-rolled ``if b"x" in d: int(d[b"x"].decode())`` at every site, which is where
    the name drift crept in.

    :param stream_fields: the stream entry mapping (bytes keys, as redis-py returns it)
    :param name: canonical field name, as declared in one of the ``*_STREAM_FIELDS`` sets
    :param default: returned when neither the canonical name nor any alias is present
    :param cast: optional callable applied to the decoded value (e.g. ``int``)
    :returns: ``(value, matched_name)`` — ``matched_name`` is ``None`` when the default was used,
        otherwise the field name actually found. Returning the matched name lets callers log
        *which* spelling arrived, which is what makes an alias hit visible in the logs instead of
        silently equivalent.
    """
    for candidate in (name,) + tuple(FIELD_ALIASES.get(name, ())):
        for key in (candidate.encode(), candidate):
            if key in stream_fields:
                raw = stream_fields[key]
                if isinstance(raw, (bytes, bytearray)):
                    try:
                        raw = raw.decode()
                    except UnicodeDecodeError:
                        # Total on the no-cast path, as the docstring promises. This helper is read
                        # inside consumer loops that ACK unconditionally, so raising here would drop
                        # the work silently and never retry it -- strictly worse than a mangled
                        # value the caller can see.
                        #
                        # Decoded lossily rather than treated as absent: a corrupt value is not the
                        # same as a missing one, and folding them together erases exactly the
                        # distinction the matched-name return exists to preserve. Logged so the
                        # corruption is greppable instead of silent.
                        logging.getLogger(__name__).warning(
                            "Stream field %r carried undecodable bytes %r; decoding lossily. "
                            "The producer wrote a non-UTF-8 value.",
                            candidate,
                            bytes(raw),
                        )
                        raw = bytes(raw).decode(errors="replace")
                return (cast(raw) if cast is not None else raw), candidate
    return default, None
