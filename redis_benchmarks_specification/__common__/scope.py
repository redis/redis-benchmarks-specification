"""
Diff-driven benchmark scoping for the webhook trigger.

When a pull request is labeled with the trigger label (action:run-benchmark), the
webhook (``__api__/app.py``) uses this module to look at the PR's changed files and
decide which subset of the benchmark suite to run — instead of always running the
full suite. The decision is purely deterministic (no AI):

  * changed source files that map cleanly to a data-type group (t_hash.c -> hash,
    t_zset.c -> sorted-set, ...) -> run only those groups (``tests_groups_regexp``);
  * any broad/infra or unmapped source file (server.c, networking.c, db.c, dict.c,
    *.h, a brand new file, ...), or a PR with no source changes -> run the curated
    cross-group "core" set from core-specs.json (``tests_regexp``) — small, but with
    one representative per group so a cross-cutting regression still surfaces;
  * a very large PR, an unavailable/failed GitHub fetch, or any internal error ->
    no filter -> full suite (today's behavior). Scoping must never break the trigger.

The emitted regexes are ANCHORED (``^(?:...)$``) because the coordinator matches them
with ``re.search`` against each group name / the test-name stem — unanchored, "set"
would also select "sorted-set". The resulting fields are merged into the commit-stream
event, where the builder and coordinator already honor ``tests_groups_regexp`` and
``tests_regexp`` (verified end-to-end). No downstream change is required.

Gated by ``BENCHMARK_PR_DIFF_SCOPING`` (env, default on); restore full-suite-on-label
by setting it to 0 — no code change.
"""

import json
import logging
import os
import re

_COMMON_DIR = os.path.dirname(__file__)
_FILES_TO_GROUPS_PATH = os.path.join(_COMMON_DIR, "files-to-groups.json")
_CORE_SPECS_PATH = os.path.join(_COMMON_DIR, "core-specs.json")


def load_files_to_groups(path=None):
    """Load the file-basename -> [group, ...] map. Values are normalized to lists."""
    path = path or _FILES_TO_GROUPS_PATH
    with open(path) as fh:
        data = json.load(fh)
    exact = data.get("exact", data)
    return {
        base: (groups if isinstance(groups, list) else [groups])
        for base, groups in exact.items()
        if not base.startswith("_")
    }


def load_core_specs(path=None):
    """Load the curated core-set test-name stems (one representative per group)."""
    path = path or _CORE_SPECS_PATH
    with open(path) as fh:
        data = json.load(fh)
    return list(data.get("core_specs", data) if isinstance(data, dict) else data)


# Loaded once at import, guarded: a missing/corrupt data file degrades to "no scope
# data" -> full suite everywhere, instead of raising on the webhook hot path.
try:
    _FILES_TO_GROUPS = load_files_to_groups()
    _CORE_SPECS = load_core_specs()
except Exception as exc:  # noqa: BLE001 — never break import of the webhook
    logging.error("diff-scoping: failed to load scope data, scoping disabled: %s", exc)
    _FILES_TO_GROUPS, _CORE_SPECS = {}, []


def _anchored_alt(items):
    """Build an anchored alternation regex from literal strings: ^(?:a|b|c)$."""
    return "^(?:%s)$" % "|".join(re.escape(s) for s in items)


def scope_fields_from_changed_files(
    changed_files, files_to_groups=None, core_specs=None
):
    """
    Convert a PR's changed file paths into benchmark stream filter fields.

    Returns one of:
      * ``{"tests_groups_regexp": "^(?:hash|sorted-set)$"}`` — every changed *source*
        file maps to a known data-type group;
      * ``{"tests_regexp": "^(?:<core spec>|...)$"}`` — a broad/infra/unmapped source
        file, or no source changes (the curated cross-group core set);
      * ``{}`` — the PR changed nothing, OR the core list is empty / nothing mapped
        with no core list available (caller -> full suite).

    Only ``src/*.c`` / ``src/*.h`` files influence the decision; docs, tests and CI
    changes are ignored so a hash-only PR that also edits a .tcl test still scopes to
    ``hash``.
    """
    if files_to_groups is None:
        files_to_groups = _FILES_TO_GROUPS
    if core_specs is None:
        core_specs = _CORE_SPECS
    if not changed_files:
        return {}
    src_files = [
        f
        for f in changed_files
        if f.startswith("src/") and (f.endswith(".c") or f.endswith(".h"))
    ]
    groups = []
    all_mapped = True
    for f in src_files:
        base = f.rsplit("/", 1)[-1]
        mapped = files_to_groups.get(base)
        if mapped:
            for g in mapped:
                if g not in groups:
                    groups.append(g)
        else:
            all_mapped = False
    if src_files and all_mapped and groups:
        return {"tests_groups_regexp": _anchored_alt(groups)}
    # broad / infra / unmapped source file, or no source files -> curated core set.
    # If the core list is unavailable, fall through to the full suite (safe default).
    if not core_specs:
        return {}
    return {"tests_regexp": _anchored_alt(core_specs)}


def get_pr_changed_files(github_token, gh_org, gh_repo, pr_number, max_files=100):
    """
    Return the list of file paths changed in a PR, or ``None`` if scoping should
    fall back to the full suite — i.e. on any error (auth/network/API) OR when the PR
    is too large to scope meaningfully (> ``max_files``; such a PR is inherently broad
    and the per-file pagination would add many synchronous round-trips on the webhook
    hot path). ``None`` is the safe sentinel: the caller runs the full suite.
    """
    try:
        from github import Github

        # This runs inside the webhook request, which GitHub gives ~10s before it marks
        # delivery failed and retries (-> duplicate triggers). Bound the total work:
        #   - short per-request timeout,
        #   - per_page=100 so a <=max_files PR is a single get_files page,
        #   - lazy=True so get_repo costs no round-trip (get_pull is the first call).
        # Worst case is then 2 sequential API calls.
        gh = (
            Github(github_token, timeout=4, per_page=100)
            if github_token
            else Github(timeout=4, per_page=100)
        )
        pull = gh.get_repo("{}/{}".format(gh_org, gh_repo), lazy=True).get_pull(
            int(pr_number)
        )
        changed = pull.changed_files or 0
        if changed > max_files:
            logging.info(
                "diff-scoping: PR %s/%s#%s changed %d files (> %d) -> full suite",
                gh_org,
                gh_repo,
                pr_number,
                changed,
                max_files,
            )
            return None
        return [f.filename for f in pull.get_files()]
    except Exception as exc:  # noqa: BLE001 — never let scoping break the trigger
        logging.warning(
            "diff-scoping: could not fetch changed files for %s/%s#%s (%s: %s) -> full suite",
            gh_org,
            gh_repo,
            pr_number,
            type(exc).__name__,
            exc,
        )
        return None


def compute_pr_scope_fields(github_token, gh_org, gh_repo, pr_number, max_files=100):
    """
    High-level entry point used by the webhook. Returns ``(fields, message)`` where
    ``fields`` is the dict to merge into the commit-stream event ({} = full suite)
    and ``message`` is a human-readable one-liner for the logs.

    Fully guarded: any failure (including missing scope-data files) returns ({}, ...)
    so the webhook always falls back to the full suite and never 500s.
    """
    try:
        files = get_pr_changed_files(
            github_token, gh_org, gh_repo, pr_number, max_files
        )
        if files is None:
            return {}, "diff-scoping: full suite (diff unavailable or PR too large)"
        fields = scope_fields_from_changed_files(files)
        return fields, "diff-scoping: %d changed files -> %s" % (
            len(files),
            fields if fields else "full suite",
        )
    except Exception as exc:  # noqa: BLE001 — entry-point safety net
        logging.warning("diff-scoping: unexpected error -> full suite: %s", exc)
        return {}, "diff-scoping: full suite (internal error)"
