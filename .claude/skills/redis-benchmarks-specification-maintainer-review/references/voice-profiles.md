# Voice profiles — real redis-benchmarks-specification maintainers

Mined from actual GitHub history on `redis/redis-benchmarks-specification`: `gh api .../pulls/<n>/reviews`,
`gh api .../pulls/<n>/comments`, and `gh issue view` across ~400 merged PRs and the open/closed issue list.
Quotes below are real and cited by PR/issue number. Read this alongside `nitpick-taxonomy.md` before writing
anything — the goal is to sound like this project's actual history, not a generic reviewer.

**Be honest about what the record actually shows first:** unlike a project with several independent, long-tenured
human reviewers, the overwhelming majority of PRs on this repo (roughly 90%+ of the ~400 merged) are authored
*and* merged by one person, and in the current era that same person is also the one writing the most detailed
review comments — on their own PRs. That is a real, unusual, well-evidenced pattern here, not an assumption. It
means the "second independent maintainer catches a problem" scenario that a lot of review culture assumes is
comparatively rare in this repo's history — genuinely independent review is concentrated in a few contributors
and in an earlier era (2022–2023). Calibrate for that: on a PR from the primary maintainer, real precedent is
either near-silence or a structured self-audit written in the exact style described below; on a PR from anyone
else, real precedent is a fast, terse, usually short pass.

## fcostaoliveira (primary maintainer, 2024–2026 era — by far the deepest and most consistent voice)

**Voice**: structured, evidence-first, terse relative to how much ground it covers. A real finding is almost
always: one **bolded, specific claim** stating exactly what's wrong, then 1-3 sentences of the mechanism (often
citing the actual function/code path, not just "this looks wrong"), then a concrete suggested fix or an explicit
"up to you." Distinguishes blocking findings from non-blocking asides explicitly ("Separately, not a blocker:
..."). Closes reviews in plain prose, not a labeled verdict block.

**Real findings, quoted**:
- PR#445 — description-vs-reality mismatch, traced to the actual code path: *"`client-list-id`'s description
  doesn't match the code path it exercises. `CLIENT LIST ID <id>` resolves via `lookupClientByID()` →
  `raxFind(server.clients_index, ...)` in `networking.c` — an O(log n) radix-tree point lookup, not a walk of the
  connection list... Worth correcting the description to say what it actually measures."* Same review also
  separates a non-blocking observation (`CLIENT LIST TYPE normal` reads statistically identical to plain
  `CLIENT LIST` here since every connection is type `normal`) from the blocking one, and explicitly clears an
  unrelated flaky CI failure: *"The unrelated `test_spin_docker_cluster_redis` CI failure is a known
  cluster-formation timing flake, not caused by this diff."*
- PR#441 — dataset/preload count correctness: *"`memtier_benchmark-1key-list-10K-elements-lset-quicklist.yml`
  preloads 9999 elements, not 10,000... measured `LLEN` after running the preload args as written comes out to
  9999."* Gives the author both options (fix the count or fix the naming) rather than dictating one.
- PR#442 — unbounded resource growth in a long-running spec: *"The `xreadgroup-block` spec never ACKs. All
  consumer connections read under the same consumer name with no `XACK` and no `NOACK` flag, so every delivered
  entry stays in that consumer's PEL for the full run — unbounded by the stream's `MAXLEN ~` trim... it's a
  plausible confound on the very p50/p99 unblock-latency numbers this spec is measuring."*
- PR#438 — jsonpath/output-schema correctness, verified against real tool output: *"Verified empirically with
  `--json-out-file`: memtier buckets both `HIMPORT PREPARE` and `HIMPORT SET` under a single **`Himports`**
  section... Renamed `Hsets` → `Himports`."*

**What this means for a first-pass bot review, honestly**: this maintainer's deepest reviews describe themselves
as multiple rounds of "N-agent"/"N-lens adversarial review" against real running infrastructure (a fleet client
image, the actual container-timeout backstop code, a live CI run on an exact commit SHA) — that is a real,
evidenced process, but it is *this person's own workflow*, not something a single-pass, read-only comment step
can honestly claim to have done. **Do not claim multi-round review, agent counts, or "verified on the fleet" —
you did not do that.** What's genuinely transferable is the *standard* this process enforces: cite the exact
file/function/line when you can find one in the diff, state a finding as a specific claim rather than a vague
suspicion, separate blocking from non-blocking explicitly, and don't hold a PR hostage to an unrelated flaky CI
job.

## filipecosta90 (2022–2023 era — earlier, terser voice)

Filipe's account handle in the repo's early history; `filipecosta90` and the later `fcostaoliveira` account are
very likely the same maintainer across two GitHub identities — every merged PR across both eras traces back to
one continuously-owned project, and the throughline (thorough, technically precise, prone to citing the exact
mechanism) is consistent even as the volume of self-written prose grew a lot over time. Treat this as a plausible
inference from the record, not a confirmed fact you should assert as certain in a review.

**Voice**: short, direct, transactional. Approvals are almost always a one-line thanks: *"LGTM. Thank you
@slice4e"*, *"LGTM. Thank you @paulorsousa!"*, *"Thank you @slice4e! Approving"*. Blocking requests are equally
short and specific:
- PR#211 — *"requires test. I can add it if required."*
- PR#142 — *"need to reformat using black."* (Note: `black --check` has been wired into `tox.ini`/CI since; this
  class of comment is now CI's job, not a reviewer's — see the nitpick taxonomy's formatting rule.)
- PR#178 — pasted the actual validator output rather than describing it: *"there is a difference between
  specified test-commands in the yaml (name=memtier_benchmark-1key-100MB-string-bitfield) and the ones we've
  detected ['get']!=['get', 'bitfield']!"*
- PR#174 — ties a specific missing field to mergeability: *"@slice4e please include `--command-key-pattern` so
  this can be approved and merged :)"*
- PR#243 — flags a structural problem plainly: *"@ofekshenawa notice that we have duplicate test names."*
- PR#216 — asks rather than assumes when something disappears in a diff: *"@markovamaria why was this removed?"*
- Issue #235 (as a maintainer answering an external bug report) — one precise diagnostic question instead of
  guessing: *"@odidev do you start with an empty DB before the test? Can you retry with
  `--flushall_on_every_test_start` option enabled?"*

**What this era shows**: silence/one-line approval is the default even then; a real, specific, often
CI-output-quoting objection is what triggers `CHANGES_REQUESTED`, never a style preference alone.

## paulorsousa (recurring contributor and occasional reviewer, both eras)

**Voice**: raises design/scope questions as genuine open questions, not directives, often pulling in the other
maintainer by name rather than deciding unilaterally:
- PR#353 — *"We're using 'tags' to filter 'testnames'. Is it intended?"* (a real naming/scope confusion the
  author confirmed and fixed: *"it was a bad naming."*)
- PR#373 — *"@filipecosta90 what do you think? Should we automatically add `--cluster-mode`; or require the user
  to pass it in the test suite commands?"*

Also a repeat PR author across the project's whole history (streams, cluster topology, HyperLogLog, IO-thread
topologies) — when reviewing his own or others' work the register stays the same: short, specific, deferential
where genuinely uncertain.

## Other real, occasional voices (each sparse individually — don't over-index on any one, but don't invent
generic "reviewer-speak" that contradicts them either)

- **markovamaria** — welcomes contributors by name before getting technical: *"Hi @mpozniak95, thank you for
  PR!"* (PR#248, `CHANGES_REQUESTED` after a real bug — see nitpick taxonomy item on library-API misuse). Also a
  frequent bug reporter herself (issues #244, #222, #217, #209, #223, #240) with clear, specific titles.
- **mpozniak95** — a contributor who found and precisely documented a real bug rather than just describing a
  symptom: PR#248's `self_contained_coordinator.py` inline comment cites the exact `docker-py` API mismatch
  (`ContainerApiMixin.logs()` doesn't accept a `logs` kwarg per the library's own docs) and traces the actual
  consequence (`teardown` silently skipped, leaking temp folders with server artifacts) — a model for how to
  write up a finding you're confident about: name the API, cite the doc, state the downstream effect.
- **ofekshenawa** — flags unnecessary work plainly: PR#168 — *"Why don't use `original_files` here? seems like
  unnecessary computation"* (author's reply: *"indeed. addressed in the latest commit"*).
- **slice4e** — informal, decisive on small things, comfortable disagreeing lightly with another reviewer: PR#175
  — *"Maria, you are right that there are 2 'Installation' sections. I would agree that we should probably keep
  1 of them. I would say the second one."*
- **zuiderkwast** — an outside/occasional reviewer whose entire review body on PR#198 was *"Nice."* Real
  precedent that a one-word, low-effort human approval is authentic here too — don't assume every real review in
  this project's history is substantive; plenty aren't, and that's fine.

## Tone notes that apply across all of the above

- Thank contributors by @-handle in prose is a real, constant habit of the humans in this history — but per the
  security rules for this skill, **the bot must never literally @-mention a GitHub handle**. Say "thanks for
  this" or "thanks for the fix" without the `@`, or refer to the person descriptively ("the PR author") — don't
  drop the warmth, just drop the notification-triggering syntax.
- Nobody in this history writes a labeled "Verdict:" line, a bolded summary, or a TL;DR section. Every real
  review ends on a plain sentence of prose.
- Silence, or a one-line thanks, is the single most common real outcome in this project's history — for routine
  PRs (version bumps, a new benchmark spec that's internally consistent, a CI tweak), replicate that rather than
  manufacturing a comment.
