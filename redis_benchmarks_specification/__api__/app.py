import json

from flask import jsonify
import redis
from flask import Flask, request
from hmac import HMAC, compare_digest
from hashlib import sha1

from redis_benchmarks_specification.__common__.builder_schema import (
    commit_schema_to_stream,
)
from redis_benchmarks_specification.__common__.env import (
    BENCHMARK_TRIGGER_BRANCHES,
    BENCHMARK_TRIGGER_ORGS,
    PULL_REQUEST_TRIGGER_LABEL,
    BENCHMARK_PR_DIFF_SCOPING,
    BENCHMARK_PR_MAX_FILES,
    GH_TOKEN,
    is_buildable_sha,
)
from redis_benchmarks_specification.__common__.scope import compute_pr_scope_fields

SIG_HEADER = "X-Hub-Signature"


def should_action(action):
    # Exact match — a substring test (`action in tt`) would let stray action strings
    # that happen to be substrings of these spuriously trigger.
    return action in ("synchronize", "opened", "reopened", "labeled")


def create_app(conn, user, test_config=None):
    app = Flask(__name__)

    conn = conn

    # GH Token Authentication
    def verify_signature(req):
        result = False
        try:
            secret = conn.get("{}:auth_token".format(user))
            sig_header = req.headers.get(SIG_HEADER)
            if secret is not None and sig_header is not None:
                if type(secret) == str:
                    secret = secret.encode()
                if "sha1=" in sig_header:
                    received_sign = sig_header.split("sha1=")[-1].strip()
                    expected_sign = HMAC(
                        key=secret, msg=req.data, digestmod=sha1
                    ).hexdigest()
                    result = compare_digest(received_sign, expected_sign)
        except redis.exceptions.ResponseError:
            pass
        except redis.exceptions.AuthenticationError:
            pass
        return result

    @app.route("/ping", methods=["GET"])
    def ping():
        return "PONG", 200

    # verify deps
    @app.route("/pong", methods=["GET"])
    def pong():
        code = 200
        try:
            conn.ping()
            redis_status = "OK"
        except Exception as e:
            redis_status = e.__str__()
            code = 503
        res = {"redis": {"status": redis_status}}
        return jsonify(res), code

    @app.route("/api/gh/redis/redis/commits", methods=["POST"])
    def base():
        if verify_signature(request):
            print(request)
            # Get Request body from JSON
            request_data = request.json
            if type(request_data) is str:
                request_data = json.loads(request_data)
            if type(request_data) is bytes:
                request_data = json.loads(request_data.decode())

            gh_org = "redis"
            gh_repo = "redis"
            ref = None
            ref_label = None
            sha = None

            event_type = "Ignored event from webhook"
            use_event = False
            # Pull request labeled
            pull_request_number = None
            # Base repo of the PR (where the PR number is valid) — used to fetch the
            # changed files for diff-scoping. For a fork PR the head repo is the fork,
            # whose API has no such PR number, so we must query the base repo.
            pr_base_org = None
            pr_base_repo = None
            trigger_label = PULL_REQUEST_TRIGGER_LABEL
            if "pull_request" in request_data:
                action = request_data["action"]
                if should_action(action):
                    pull_request_dict = request_data["pull_request"]

                    head_dict = pull_request_dict["head"]
                    repo_dict = head_dict["repo"]
                    base_repo_dict = (pull_request_dict.get("base") or {}).get(
                        "repo"
                    ) or {}
                    base_full_name = base_repo_dict.get("full_name")
                    if base_full_name and "/" in base_full_name:
                        pr_base_org, pr_base_repo = base_full_name.split("/")[-2:]
                    labels = []
                    if "labels" in pull_request_dict:
                        labels = pull_request_dict["labels"]
                    ref = head_dict["ref"]
                    ref_label = head_dict["label"]
                    sha = head_dict["sha"]
                    html_url = repo_dict["html_url"].split("/")
                    gh_repo = html_url[-1]
                    gh_org = html_url[-2]
                    detected_label = False
                    for label in labels:
                        label_name = label["name"]
                        if trigger_label == label_name:
                            use_event = True
                            pull_request_number = request_data["number"]
                            event_type = f"Pull request #{pull_request_number} labeled with '{trigger_label}'"
                            detected_label = True
                    if detected_label is False:
                        app.logger.info(
                            "Unable to detected benchmark trigger label: {}".format(
                                trigger_label
                            )
                        )

            # Git pushes to repo
            before_sha = None
            if "ref" in request_data:
                repo_dict = request_data["repository"]
                html_url = repo_dict["html_url"].split("/")
                gh_repo = html_url[-1]
                gh_org = html_url[-2]
                ref = request_data["ref"].split("/")[-1]
                ref_label = request_data["ref"]
                sha = request_data.get("after")
                before_sha = request_data.get("before")
                # GitHub sends an all-zero sha for the absent end of a ref creation or
                # deletion, so neither end can be assumed buildable. is_buildable_sha is
                # non-throwing, so a malformed payload value is rejected rather than 500ing.
                if not is_buildable_sha(before_sha):
                    before_sha = None
                if not is_buildable_sha(sha):
                    sha = None
                # A deleted ref has no current tip, and a force-push's "before" is no longer
                # on the branch -- benchmarking it would attribute a datapoint to a commit the
                # branch does not contain. Gate on the event's own booleans.
                if request_data.get("deleted") or request_data.get("forced"):
                    before_sha = None

                allowed_orgs = [
                    o.strip() for o in BENCHMARK_TRIGGER_ORGS.split(",") if o.strip()
                ]
                allowed_branches = [
                    b.strip()
                    for b in BENCHMARK_TRIGGER_BRANCHES.split(",")
                    if b.strip()
                ]
                org_ok = gh_org in allowed_orgs
                branch_ok = ref in allowed_branches
                if org_ok and branch_ok:
                    use_event = True
                    event_type = "Git pushes to repo"
                else:
                    use_event = False
                    skip_reasons = []
                    if not org_ok:
                        skip_reasons.append(
                            f"org '{gh_org}' not in allowed orgs {allowed_orgs}"
                        )
                    if not branch_ok:
                        skip_reasons.append(
                            f"branch '{ref}' not in allowed branches {allowed_branches}"
                        )
                    event_type = f"Push event skipped: {'; '.join(skip_reasons)}"

            if use_event is True:
                # Diff-driven scoping: for a labeled PR, derive the affected command
                # group(s) from the changed files and scope the run; broad/infra/unmapped
                # diffs fall back to a curated core set. Empty -> full suite (today's
                # behavior). Gated by BENCHMARK_PR_DIFF_SCOPING so it can be disabled.
                scope_fields = {}
                if BENCHMARK_PR_DIFF_SCOPING and pull_request_number is not None:
                    # Query the PR's BASE repo (fall back to head if base is absent),
                    # since the PR number only exists there.
                    scope_fields, scope_msg = compute_pr_scope_fields(
                        GH_TOKEN,
                        pr_base_org or gh_org,
                        pr_base_repo or gh_repo,
                        pull_request_number,
                        BENCHMARK_PR_MAX_FILES,
                    )
                    app.logger.info(scope_msg)
                # The head entry is enqueued FIRST, deliberately. Each entry occupies a
                # platform for a full suite and the queue is oversubscribed, so whichever
                # entry is enqueued second is the one that starves. Enqueuing the head first
                # keeps the newest point on by.branch/<branch> equal to the branch tip: the
                # coordinator baselines every automated regression table on that series'
                # single newest point (self_contained_coordinator.py:2930,2939), so a starved
                # head would silently baseline PRs against the previous commit.
                if sha is None:
                    app.logger.info(
                        "Skipping benchmark trigger for {}: no buildable head commit".format(
                            ref_label
                        )
                    )
                    return jsonify({"message": "no buildable head commit"}), 200
                fields_after = {
                    "git_hash": sha,
                    "ref_label": ref_label,
                    "ref": ref,
                    "gh_repo": gh_repo,
                    "gh_org": gh_org,
                }
                if pull_request_number is not None:
                    fields_after["pull_request"] = pull_request_number
                fields_after.update(scope_fields)
                app.logger.info(
                    "Using event {} to trigger benchmark. final fields: {}".format(
                        event_type, fields_after
                    )
                )
                result, response_data, err_message = commit_schema_to_stream(
                    fields_after, conn, gh_org, gh_repo
                )
                app.logger.info(
                    "Using event {} to trigger benchmark. final fields: {}".format(
                        event_type, response_data
                    )
                )
                # The response describes the head entry, which is the primary trigger. The
                # baseline enqueue below must not overwrite it.
                head_response_data = response_data
                # NOTE: scope_fields is only ever non-empty for labeled-PR events. The
                # merge-base baseline below runs on PUSH events (where there is no PR),
                # so it is intentionally NOT scoped — scoped PR-head runs are compared
                # against the full-suite baseline of the corresponding subset.
                if before_sha is not None:
                    fields_before = {
                        # The previous tip, not the head: the head is enqueued above.
                        "git_hash": before_sha,
                        "ref_label": ref_label,
                        "ref": ref,
                        "gh_repo": gh_repo,
                        "gh_org": gh_org,
                    }
                    app.logger.info(
                        "Using event {} to trigger previous-tip baseline benchmark. final fields: {}".format(
                            event_type, fields_before
                        )
                    )
                    result, baseline_response, err_message = commit_schema_to_stream(
                        fields_before, conn, gh_org, gh_repo
                    )
                    if result is False:
                        app.logger.error(
                            "Baseline commit {} could not be enqueued: {}".format(
                                before_sha, err_message
                            )
                        )
                    else:
                        app.logger.info(
                            "Using event {} to trigger previous-tip baseline benchmark. final fields: {}".format(
                                event_type, baseline_response
                            )
                        )
                    response_data = head_response_data
            else:
                app.logger.info(
                    "{}. input json was: {}".format(event_type, request_data)
                )
                response_data = {"message": event_type}

            # Send data back as JSON
            return jsonify(response_data), 200
        else:
            return "Forbidden", 403

    return app
