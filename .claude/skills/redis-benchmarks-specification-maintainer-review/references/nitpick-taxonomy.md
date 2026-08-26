# Cross-cutting nitpick taxonomy — redis-benchmarks-specification, real precedent only

12 evidenced categories, each grounded in a real PR review, issue, or a workflow file that exists *because of* a
real historical bug in this repo. Work through these against the target PR. Categories 1-3 carry outsized weight
(see SKILL.md) because this is a cross-language spec/contract repo, not a general application — its own CI
(`stream-contract.yml`, `validate-spec-fields.yml`) exists specifically to catch categories 1 and 2 automatically,
which is itself strong evidence of how much the real maintainers care about them. Most PRs will only trip a
handful of these, if any — a routine PR adding one more internally-consistent benchmark spec may trip none.

1. **Stream field-name contracts between producer and consumer must match exactly — a mismatch fails silently,
   not loudly.** This is the single best-evidenced category in the whole repo: `.github/workflows/stream-contract.yml`
   exists, in the maintainers' own words in that file, because *"the builder writes `tests_priority_upper_limit`
   while the coordinator read `priority_upper_limit`, so `--tests-priority-upper-limit` had no effect on any run"*
   (issue #448, real, cited by exact field names both sides used). The same class of bug recurs across the
   issue tracker, each with exact file:line evidence: `gh_org`/`gh_repo` written vs `github_org`/`github_repo`
   read (#453); `use_git_timestamp` written as `str(bool)` and read with `bool(str)`, so `"False"` is always
   truthy (#456); `before_sha` computed but only ever used as a `None`-guard, never actually read, so the
   "baseline" benchmark silently re-benchmarks the head commit (#454); `ref` written but never read (#458);
   `--tests-regexp` used as a literal `LREM` value instead of a regex (#457); a `--dockerhub` path that swaps
   `git_version` into the `git_hash` positional (#455); two fields read but never written at all, `executable`
   and `docker_air_gap` (#450); `--tests-regexp` matched against different things (filename stem vs. `name:`
   field) in two components with different anchoring (#449). If a diff touches anything that writes to or reads
   from the commits/builds/results Redis Streams (`__cli__`, `__builder__`, `__runner__`,
   `__self_contained_coordinator__`, `__api__`), check that every field name, type, and truthiness convention
   matches on both ends — grep for the literal string on both the writer and every reader you can find in the
   diff and surrounding code, since this class of bug produces no exception, just a silent fallback to a default.

2. **YAML spec fields and cross-file scope-mapping must reconcile with the SPEC validator's derived model, not
   just look right to a human.** `validate-spec-fields.yml` runs `redis-benchmarks-spec-cli --tool stats
   --fail-on-required-diff` on every PR specifically to catch this. Real precedent: a spec's declared
   `command`/test-commands must match what the validator actually detects in the command string (filipecosta90,
   PR#178, quoting the real validator output: `['get']!=['get', 'bitfield']`); a spec is not mergeable without a
   field the validator needs even if the benchmark itself is correct (filipecosta90, PR#174: *"please include
   `--command-key-pattern` so this can be approved and merged"*). The `files-to-groups.json`/`groups.json`/
   `core-specs.json` scope-mapping saga (PR#373 → PR#442) is the fullest real example: an initial attempt used a
   synthetic `blocking` tested-group that the validator's command-derived model couldn't reconcile (caught by
   Cursor Bugbot and by the CI job), and the accepted fix mapped the source file to the *existing*
   list/sorted-set/stream groups instead, verified locally via the same CLI command CI runs. If a diff adds a
   new spec or changes `tested-groups`/`command`, check whether it's the kind of change this validator would
   actually be able to reconcile — you may not be able to run the validator yourself in a read-only review, but
   you can check whether the diff's own YAML is internally consistent (command string vs. declared fields vs.
   any `files-to-groups.json`/`groups.json` entries touched in the same diff).

3. **Preload/dataset counts and `dataset_name` labels must match what's actually loaded — off-by-one and
   copy-paste mislabeling are the most common real bug in this class.** Three real, recent issues, each caught
   by literally running the preload command and measuring the result: a "10K-element" shared list preload that
   actually populates 9,999 elements because of how the key-pattern's upper bound is exclusive (#509, and caught
   again live in review on a sibling spec — PR#441, same 9999-vs-10000 mechanism); a shared zset preload that
   undercounts cardinality due to per-occurrence key draws (#508); seven `100Kkeys-hash-*` specs that declared
   `dataset_name: 1Mkeys-hash-50-fields-10B-size` — the label of a different, 10x-larger dataset with a
   different preload shape entirely (#510). If a diff adds or copies a preload command, check whether the
   claimed element/key count in the filename, `description`, and `dataset_description` actually matches what the
   preload args would produce (watch especially for `--key-maximum`/`-n allkeys` boundary-inclusivity, and for a
   `dataset_name` that was copy-pasted from a sibling spec with a different scale or shape).

4. **A spec's description must state what code path it actually exercises, not what it was intended to
   exercise.** Real precedent, verified against the actual server-side implementation: a `CLIENT LIST ID <id>`
   spec described as a cheaper variant of the `CLIENT LIST` walk actually resolves via `lookupClientByID()` →
   `raxFind()`, an O(log n) point lookup in a completely different subsystem, not a walk at all (PR#445); a
   feature-store ingest spec's JSON output paths were copied from a similarly-named but wrong section
   (`Hsets`) instead of the section memtier actually emits (`Himports`), verified with `--json-out-file`
   (PR#438). The open issue tracker has several more of this shape: a filter spec where the filter never
   actually excludes anything in practice (#490's cross-contamination via a reused key-index), a suite whose
   name implies one command's cost but that hits a fast/degenerate path instead (#477, #494, #515, #479). When
   reviewing a new or changed spec, ask whether the description's claim about *what's being measured* survives a
   close read of the actual `command`/preload args, not just whether the YAML is well-formed.

5. **A benchmark suite must be server-bound, not load-generator-bound, or the number it produces isn't measuring
   what it claims to.** This category is specific to a benchmark-spec repo and shows up repeatedly in the issue
   tracker: memtier itself pegged near its own thread budget, so the recorded throughput reflects the client's
   ceiling, not the server's (#487 at 81% of memtier's threads, #486 at 91%, with the further inconsistency that
   its ingest sibling uses `-t 10` while the serving suite uses `-t 4`, #485 similarly at 91%). If a diff adds a
   new suite with an unusually small key/value footprint and a high client thread/connection count, or the PR
   description includes memtier's own thread-utilization numbers, check whether the stated conclusion could
   instead be an artifact of the client saturating first.

6. **Long-running / blocking specs that never bound their own growth are a real, found bug class.** PR#442:
   *"The `xreadgroup-block` spec never ACKs... every delivered entry stays in that consumer's PEL for the full
   run — unbounded by the stream's `MAXLEN ~` trim, since trimmed entries leave dangling PEL references behind."*
   For any spec that blocks, streams, or holds pending/unacked state across a sustained run, check whether
   something (an `XACK`, a `NOACK` flag, an eviction/expiry setting) actually bounds the resource that
   accumulates, and whether the PR's own claimed metric (e.g. p50/p99 unblock latency) could be confounded by
   that growth over the run.

7. **Distinguish a genuine CI failure from a known infra flake — don't hold a PR hostage to the latter.** Real
   precedent, stated plainly rather than silently ignored: *"The unrelated `test_spin_docker_cluster_redis` CI
   failure is a known cluster-formation timing flake, not caused by this diff"* (PR#445). If a PR's CI has a
   failure, check whether it's plausibly related to the diff before treating it as blocking — but say so
   explicitly rather than staying silent about a failing check, since silently ignoring a red CI run (without
   at least naming which check and why it's unrelated) reads as sloppy, not confident.

8. **New behavior needs a real test, not a prose claim of manual testing.** This is written, explicit project
   doctrine (`CONTRIBUTING.md`: *"All new behaviour must be covered by tests... Coverage should not decrease"*),
   not just a review-comment pattern — but there's real review precedent behind it too: *"requires test. I can
   add it if required"* (filipecosta90, PR#211). If a diff adds new logic (not just a new declarative YAML spec)
   with no corresponding test under `utils/tests/`, that's worth naming plainly.

9. **Don't manufacture formatting/whitespace nitpicks — `tox.ini` already runs `black --check` in CI.** A real
   2022-era review once asked for a `black` reformat by hand (PR#142); that's now CI's job, not a reviewer's —
   only mention style if it's something tooling can't catch (stray/backup files, dead commented-out code — see
   next item), not indentation or quote-style.

10. **Stray files and genuinely dead/unreachable code are worth naming plainly, once confirmed.** Real precedent
    for confirming before commenting, not guessing: a library-API misuse traced to the actual docs
    (`docker-py`'s `ContainerApiMixin.logs()` doesn't take a `logs` kwarg) with the concrete downstream effect
    named — `teardown` silently skipped, leaking temp folders with server artifacts (mpozniak95, PR#248). The
    issue tracker's own `#450` ("Two stream fields are read but never written... dead branch... unreachable")
    is the same category found by the maintainer's own later audit. If you spot something that looks unused or
    unreachable, say so with the specific reason (grep result, or the exact narrow condition that makes a branch
    dead) rather than a vague "is this needed?"

11. **Resist redundant computation when something already-computed could be reused.** Real precedent: *"Why
    don't use `original_files` here? seems like unnecessary computation"* (ofekshenawa, PR#168) — a plain,
    specific question naming the existing value that could have been reused, not a general performance lecture.

12. **Design/scope questions are asked as genuine open questions, sometimes explicitly deferred rather than
    decided unilaterally — that's authentic, not indecisive.** Real precedent: *"We're using 'tags' to filter
    'testnames'. Is it intended?"* (paulorsousa, PR#353 — confirmed as an actual naming mistake and fixed);
    *"what do you think? Should we automatically add `--cluster-mode`; or require the user to pass it in the
    test suite commands?"* (paulorsousa, PR#373, addressed to a co-maintainer by role, not by name in your
    output — see the security rule against literal @-mentions). If a PR makes a debatable design choice that
    isn't clearly wrong, it's authentic to phrase it as a real question rather than a directive.
