#!/usr/bin/env python3
"""Regenerate the top-level commands.json corpus from a Redis checkout.

The corpus mirrors the command table of a specific Redis release and is the
lookup table __cli__/stats.py uses for group/ACL bookkeeping. It had been
generated against Redis <= 8.0.0; this script refreshes it from a newer tree.

Sources, all read from the checkout passed as the first argument:
  - src/commands/*.json          command metadata (the command table)
  - modules/vector-sets/commands.json   vector-set module commands
  - the existing commands.json   entries on the CARRIED_OVER_COMMANDS
                                 allowlist (external module bundles such as
                                 RedisJSON, not part of the checkout) are
                                 carried over verbatim; anything else that
                                 only exists in the file is treated as
                                 drift and fails the run

Every tested-commands entry of the suites must resolve in the result, which
turns "a suite names a command that does not exist" (e.g. the historical
`setx` typo) into a hard error instead of a silent statistics gap.

Usage:
    python3 utils/regenerate_commands_json.py /path/to/redis [--check]

--check exits non-zero when the committed commands.json differs from a fresh
regeneration, so a CI job can fail on drift instead of silently going stale.
"""

import argparse
import glob
import json
import os
import sys

import yaml

VECTOR_SET_MODULE_FILE = "modules/vector-sets/commands.json"

# Commands that live outside the redis checkout (external module bundles,
# e.g. RedisJSON) and are carried over verbatim from the committed corpus.
# The allowlist is explicit on purpose: a command upstream removes must not
# silently survive regeneration from the stale file being validated.
CARRIED_OVER_COMMANDS = ("JSON.GET", "JSON.SET")

# The command table spells some groups with underscores; the corpus uses the
# canonical groups.json names (see PR #371 for the sorted-set precedent).
GROUP_CANONICAL_NAME = {"sorted_set": "sorted-set"}

# COMMAND INFO reports implicit ACL categories on top of the explicit ones;
# replicate setImplicitACLCategories() from src/server.c. Only the categories
# the stats tool buckets on are emitted.
IMPLICIT_ACL_CATEGORIES = [
    ("write", lambda flags, acl: "WRITE" in flags),
    ("read", lambda flags, acl: "READONLY" in flags and "scripting" not in acl),
    ("admin", lambda flags, acl: "ADMIN" in flags),
    ("dangerous", lambda flags, acl: "ADMIN" in flags),
    ("pubsub", lambda flags, acl: "PUBSUB" in flags),
    ("fast", lambda flags, acl: "FAST" in flags),
    ("blocking", lambda flags, acl: "BLOCKING" in flags),
]

# Canonical category order of ACLDefaultCommandCategories in src/acl.c; the
# corpus lists categories in this order.
ACL_CATEGORY_ORDER = [
    "keyspace", "read", "write", "set", "sortedset", "list", "hash",
    "string", "array", "bitmap", "hyperloglog", "geo", "stream", "pubsub",
    "admin", "fast", "slow", "blocking", "dangerous", "connection",
    "transaction", "scripting", "ratelimit",
]


def derive_acl_categories(metadata):
    """Merge explicit acl_categories with the implicit ones Redis derives."""
    explicit = [category.lower() for category in metadata.get("acl_categories") or []]
    flags = metadata.get("command_flags") or []
    categories = set(explicit)
    categories.update(name for name, predicate in IMPLICIT_ACL_CATEGORIES if predicate(flags, categories))
    if "fast" not in categories:
        categories.add("slow")
    ordered = [name for name in ACL_CATEGORY_ORDER if name in categories]
    ordered.extend(sorted(categories - set(ACL_CATEGORY_ORDER)))
    return ["@" + name for name in ordered]

# Field order of the historical corpus; regenerated entries follow it too.
ENTRY_FIELD_ORDER = [
    "summary",
    "since",
    "group",
    "complexity",
    "acl_categories",
    "arity",
    "arguments",
    "command_flags",
    "hints",
    "history",
    "key_specs",
    "deprecated_since",
    "replaced_by",
    "doc_flags",
    "module",
]


def load_core_command_table(redis_root):
    """Flatten src/commands/*.json into {COMMAND NAME: raw metadata}.

    Subcommand entries declare their parent in a "container" field, so the
    corpus name is "<container> <key>" ("ACL CAT"); standalone commands keep
    their key verbatim ("MSETEX", "RESTORE-ASKING"). Sentinel-only commands
    never run against the redis-server instances this corpus benchmarks.
    """
    table = {}
    for path in sorted(glob.glob(os.path.join(redis_root, "src", "commands", "*.json"))):
        stem = os.path.basename(path)[:-len(".json")]
        if stem == "sentinel" or stem.startswith("sentinel-"):
            continue
        with open(path, encoding="utf-8") as handle:
            key, metadata = next(iter(json.load(handle).items()))
        container = metadata.get("container")
        name = "{} {}".format(container, key) if container else key
        table[name] = metadata
    return table


def key_spec_flag_name(flag):
    """Short flags stay uppercase; multi-word ones become snake_case."""
    if len(flag) <= 2:
        return flag
    return flag.lower()


def convert_key_specs(source_key_specs):
    """Map source key_specs to the corpus shape (flag booleans, typed searches)."""
    converted = []
    for key_spec in source_key_specs:
        entry = {}
        if "notes" in key_spec:
            entry["notes"] = key_spec["notes"]
        begin_search = key_spec.get("begin_search") or {}
        if "index" in begin_search:
            entry["begin_search"] = {
                "type": "index",
                "spec": {"index": begin_search["index"]["pos"]},
            }
        elif "keyword" in begin_search:
            entry["begin_search"] = {
                "type": "keyword",
                "spec": {"keyword": begin_search["keyword"]["keyword"], "startfrom": begin_search["keyword"]["startfrom"]},
            }
        elif "unknown" in begin_search:
            entry["begin_search"] = {"type": "unknown", "spec": {}}
        find_keys = key_spec.get("find_keys") or {}
        if "range" in find_keys:
            entry["find_keys"] = {
                "type": "range",
                "spec": {
                    "lastkey": find_keys["range"]["lastkey"],
                    "keystep": find_keys["range"]["step"],
                    "limit": find_keys["range"]["limit"],
                },
            }
        elif "keynum" in find_keys:
            keynum_spec = {"keynumidx": find_keys["keynum"]["keynumidx"]}
            keynum_spec["firstkey"] = find_keys["keynum"]["firstkey"]
            if "step" in find_keys["keynum"]:
                keynum_spec["keystep"] = find_keys["keynum"]["step"]
            entry["find_keys"] = {"type": "keynum", "spec": keynum_spec}
        elif "unknown" in find_keys:
            entry["find_keys"] = {"type": "unknown", "spec": {}}
        for flag in key_spec.get("flags", []):
            entry[key_spec_flag_name(flag)] = True
        converted.append(entry)
    return converted


def convert_entry(name, metadata, module_name=None):
    """Project one source-table entry onto the corpus entry shape."""
    entry = {}
    if "summary" in metadata:
        entry["summary"] = metadata["summary"]
    if "since" in metadata:
        entry["since"] = metadata["since"]
    entry["group"] = GROUP_CANONICAL_NAME.get(metadata["group"], metadata["group"])
    if "complexity" in metadata:
        entry["complexity"] = metadata["complexity"]
    # COMMAND DOCS reports no categories for module commands; skip deriving.
    if module_name is None:
        entry["acl_categories"] = derive_acl_categories(metadata)
    if "arity" in metadata:
        entry["arity"] = metadata["arity"]
    if "arguments" in metadata:
        entry["arguments"] = metadata["arguments"]
    if "command_flags" in metadata:
        entry["command_flags"] = [flag.lower() for flag in metadata["command_flags"]]
    if "command_tips" in metadata:
        entry["hints"] = [tip.lower() for tip in metadata["command_tips"]]
    if "history" in metadata:
        entry["history"] = metadata["history"]
    if "key_specs" in metadata:
        entry["key_specs"] = convert_key_specs(metadata["key_specs"])
    if "deprecated_since" in metadata:
        entry["deprecated_since"] = metadata["deprecated_since"]
    if "replaced_by" in metadata:
        entry["replaced_by"] = metadata["replaced_by"]
    if "doc_flags" in metadata:
        entry["doc_flags"] = [flag.lower() for flag in metadata["doc_flags"]]
    if module_name:
        entry["module"] = module_name
    return {field: entry[field] for field in ENTRY_FIELD_ORDER if field in entry}


def regenerate(redis_root, existing_corpus):
    corpus = {}
    core_table = load_core_command_table(redis_root)
    for name in sorted(core_table):
        corpus[name] = convert_entry(name, core_table[name])

    vector_sets_path = os.path.join(redis_root, VECTOR_SET_MODULE_FILE)
    if os.path.exists(vector_sets_path):
        with open(vector_sets_path, encoding="utf-8") as handle:
            for name, metadata in sorted(json.load(handle).items()):
                converted = convert_entry(name, metadata, module_name="vectorset")
                converted["group"] = "module"
                corpus[name] = converted

    # External-bundle commands on the allowlist are carried over verbatim;
    # anything else present only in the committed corpus is drift.
    orphans = set(existing_corpus) - set(corpus) - set(CARRIED_OVER_COMMANDS)
    if orphans:
        sys.exit("commands present only in the committed corpus (removed upstream?): {}".format(sorted(orphans)))
    for name in CARRIED_OVER_COMMANDS:
        if name in existing_corpus:
            corpus[name] = existing_corpus[name]
    return {name: corpus[name] for name in sorted(corpus)}


def validate_suite_commands(repo_root, corpus):
    """Fail when a suite declares a tested-command the corpus cannot resolve.

    stats.py looks each suite command up as-is and then upper-cased; a name
    that resolves neither way is silently dropped from coverage statistics.
    Suites may spell subcommands with Redis' pipe separator (client|list)
    while the corpus uses a space (CLIENT LIST), so both spellings resolve.
    """
    suites_dir = os.path.join(repo_root, "redis_benchmarks_specification", "test-suites")
    unresolvable = []
    for path in sorted(glob.glob(os.path.join(suites_dir, "*.yml"))):
        with open(path, encoding="utf-8") as handle:
            spec = yaml.safe_load(handle)
        for command in (spec or {}).get("tested-commands") or []:
            spaced = command.replace("|", " ")
            candidates = {command, spaced, command.upper(), spaced.upper()}
            if not candidates & set(corpus):
                unresolvable.append("{}: {}".format(os.path.basename(path), command))
    if unresolvable:
        sys.exit("tested-commands that resolve nowhere in commands.json:\n  " + "\n  ".join(unresolvable))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("redis_root", help="Path to a Redis source checkout at the target release")
    parser.add_argument("--check", action="store_true", help="Compare against the committed corpus instead of rewriting it")
    args = parser.parse_args()

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    corpus_path = os.path.join(repo_root, "commands.json")
    with open(corpus_path, encoding="utf-8") as handle:
        existing_corpus = json.load(handle)

    regenerated = regenerate(args.redis_root, existing_corpus)

    groups_path = os.path.join(repo_root, "groups.json")
    with open(groups_path, encoding="utf-8") as handle:
        known_groups = set(json.load(handle)) | {"module"}
    unknown_groups = {entry["group"] for entry in regenerated.values()} - known_groups
    if unknown_groups:
        sys.exit("groups {} are not present in groups.json; add them there first".format(sorted(unknown_groups)))

    validate_suite_commands(repo_root, regenerated)

    if args.check:
        with open(corpus_path, encoding="utf-8") as handle:
            drift = json.load(handle) != regenerated
        if drift:
            print("commands.json is stale relative to the checkout at {}".format(args.redis_root))
        else:
            print("commands.json matches the checkout at {}".format(args.redis_root))
        sys.exit(1 if drift else 0)

    payload = json.dumps(regenerated, indent=2)
    with open(corpus_path, "w", encoding="utf-8") as handle:
        handle.write(payload)
    print("Wrote {} commands to {}".format(len(regenerated), corpus_path))


if __name__ == "__main__":
    main()
