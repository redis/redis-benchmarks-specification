#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import json
from hashlib import sha1
from hmac import HMAC
from unittest.mock import patch, MagicMock

import redis

import redis_benchmarks_specification.__api__.app as app_module
from redis_benchmarks_specification.__api__.app import (
    create_app,
    SIG_HEADER,
    should_action,
)
from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
)


def test_create_app():
    try:
        conn = redis.StrictRedis(port=6379, decode_responses=True)
        conn.ping()
        conn.flushall()
        auth_token = conn.acl_genpass()
        conn.set("default:auth_token", auth_token)
        flask_app = create_app(conn, "default")
        req_data = "{}".encode()
        expected_sign = HMAC(
            key=auth_token.encode(), msg=req_data, digestmod=sha1
        ).hexdigest()

        # Unathorized due to missing header
        with flask_app.test_client() as test_client:
            response = test_client.post(
                "/api/gh/redis/redis/commits",
                json={},
                headers={},
                content_type="application/json",
            )
            assert response.status_code == 403

        # Unathorized due to wrong header value
        with flask_app.test_client() as test_client:
            response = test_client.post(
                "/api/gh/redis/redis/commits",
                json={},
                headers={SIG_HEADER: "sha1=abc"},
                content_type="application/json",
            )
            assert response.status_code == 403

        # Authorized but ignored event
        with flask_app.test_client() as test_client:
            response = test_client.post(
                "/api/gh/redis/redis/commits",
                data=json.dumps(dict({})),
                headers={SIG_HEADER: "sha1={}".format(expected_sign)},
                content_type="application/json",
            )
            assert response.status_code == 200
            assert response.json == {"message": "Ignored event from webhook"}

        # Authorized and PR event
        with open(
            "./utils/tests/test_data/event_webhook_labelled_pr.json"
        ) as json_file:
            label_pr_json = json.load(json_file)
            json_str = json.dumps(label_pr_json)
            req_data = json_str.encode()
            expected_sign = HMAC(
                key=auth_token.encode(), msg=req_data, digestmod=sha1
            ).hexdigest()

            # Stub diff-scoping so this test does not depend on a live GitHub PR-files
            # lookup (the scope-merge itself is covered by the dedicated tests below).
            with flask_app.test_client() as test_client, patch.object(
                app_module, "compute_pr_scope_fields", return_value=({}, "noop")
            ):
                response = test_client.post(
                    "/api/gh/redis/redis/commits",
                    content_type="application/json",
                    data=req_data,
                    headers={
                        "Content-type": "application/json",
                        SIG_HEADER: "sha1={}".format(expected_sign),
                    },
                )
                assert response.status_code == 200
                assert (
                    response.json["git_hash"]
                    == "a3448f39efb8900f6f66778783461cf49de94b4f"
                )
                assert response.json["ref_label"] == "filipecosta90:unstable.55555"
                assert response.json["ref"] == "unstable.55555"
                assert conn.exists(STREAM_KEYNAME_GH_EVENTS_COMMIT)

        # Authorized push from fork org/non-default branch — should be SKIPPED
        with open(
            "./utils/tests/test_data/event_webhook_pushed_repo.json"
        ) as json_file:
            label_pr_json = json.load(json_file)
            json_str = json.dumps(label_pr_json)
            req_data = json_str.encode()
            expected_sign = HMAC(
                key=auth_token.encode(), msg=req_data, digestmod=sha1
            ).hexdigest()

            with flask_app.test_client() as test_client:
                response = test_client.post(
                    "/api/gh/redis/redis/commits",
                    content_type="application/json",
                    data=req_data,
                    headers={
                        "Content-type": "application/json",
                        SIG_HEADER: "sha1={}".format(expected_sign),
                    },
                )
                assert response.status_code == 200
                # Push from filipecosta90 org on branch unstable.55555
                # should be filtered out by default allowlists
                assert "Push event skipped" in response.json["message"]

        # Authorized push from allowed org+branch — should TRIGGER benchmarks
        with open(
            "./utils/tests/test_data/event_webhook_pushed_repo.json"
        ) as json_file:
            push_json = json.load(json_file)
            # Patch to look like a push from redis org on unstable branch
            push_json["repository"]["html_url"] = "https://github.com/redis/redis"
            push_json["ref"] = "refs/heads/unstable"
            json_str = json.dumps(push_json)
            req_data = json_str.encode()
            expected_sign = HMAC(
                key=auth_token.encode(), msg=req_data, digestmod=sha1
            ).hexdigest()

            with flask_app.test_client() as test_client:
                response = test_client.post(
                    "/api/gh/redis/redis/commits",
                    content_type="application/json",
                    data=req_data,
                    headers={
                        "Content-type": "application/json",
                        SIG_HEADER: "sha1={}".format(expected_sign),
                    },
                )
                assert response.status_code == 200
                assert (
                    response.json["git_hash"]
                    == "921489d5392a13e10493c6578a27b4bd5324a929"
                )
                assert response.json["ref_label"] == "refs/heads/unstable"
                assert response.json["ref"] == "unstable"
                assert conn.exists(STREAM_KEYNAME_GH_EVENTS_COMMIT)

    except redis.exceptions.ConnectionError:
        pass


def test_should_action():
    assert should_action("labeled") == True
    assert should_action("opened") == True
    assert should_action("closed") == False
    assert should_action("na") == False
    assert should_action("reopened") == True
    assert should_action("synchronize") == True
    # exact match: a substring of a real action must NOT trigger
    assert should_action("label") == False
    assert should_action("open") == False
    assert should_action("unlabeled") == False


def _post_event(flask_app, auth_token, payload):
    """POST a webhook payload, signed the way verify_signature requires."""
    req_data = json.dumps(payload).encode()
    sign = HMAC(key=auth_token.encode(), msg=req_data, digestmod=sha1).hexdigest()
    with flask_app.test_client() as client:
        return client.post(
            "/api/gh/redis/redis/commits",
            content_type="application/json",
            data=req_data,
            headers={
                "Content-type": "application/json",
                SIG_HEADER: "sha1={}".format(sign),
            },
        )


def _post_labelled_pr(flask_app, auth_token):
    with open("./utils/tests/test_data/event_webhook_labelled_pr.json") as fh:
        return _post_event(flask_app, auth_token, json.load(fh))


# These app-level tests run with NO redis and NO network: a MagicMock conn satisfies
# the auth lookup, and commit_schema_to_stream is stubbed to echo the event fields it
# is handed — so they deterministically prove the app.py scope-field merge in any CI
# lane (no silent skip when redis is absent).
def _mock_app(auth_token="diff-scope-test-token"):
    conn = MagicMock()
    conn.get.return_value = auth_token  # default:auth_token lookup in verify_signature
    return create_app(conn, "default"), auth_token


def _echo_commit_schema(fields, *args, **kwargs):
    # mirror commit_schema_to_stream's contract: (result, reply_fields, err)
    return True, dict(fields), None


def test_pr_event_merges_scope_fields_into_stream_event():
    # a labeled PR whose diff scopes to a group must carry that filter into the event
    flask_app, auth_token = _mock_app()
    with patch.object(
        app_module,
        "compute_pr_scope_fields",
        return_value=({"tests_groups_regexp": "^(?:hash)$"}, "stub"),
    ), patch.object(
        app_module, "commit_schema_to_stream", side_effect=_echo_commit_schema
    ):
        resp = _post_labelled_pr(flask_app, auth_token)
    assert resp.status_code == 200
    assert resp.json.get("tests_groups_regexp") == "^(?:hash)$"


def test_pr_event_disabled_gate_skips_scoping():
    # with scoping disabled, the GitHub helper must not be called and no filter is added
    flask_app, auth_token = _mock_app()

    def _boom(*a, **k):
        raise AssertionError("compute_pr_scope_fields called while disabled")

    with patch.object(app_module, "BENCHMARK_PR_DIFF_SCOPING", False), patch.object(
        app_module, "compute_pr_scope_fields", _boom
    ), patch.object(
        app_module, "commit_schema_to_stream", side_effect=_echo_commit_schema
    ):
        resp = _post_labelled_pr(flask_app, auth_token)
    assert resp.status_code == 200
    assert "tests_groups_regexp" not in resp.json
    assert "tests_regexp" not in resp.json


def _push_payload(**overrides):
    """Load the push fixture and normalise it to an allowlisted org/branch.

    Paired with _allowlisted() below: the payload is made to match and the module constants
    are pinned, so an operator with BENCHMARK_TRIGGER_BRANCHES exported in their shell does
    not see these tests fail for a reason that has nothing to do with what they assert.
    """
    with open("./utils/tests/test_data/event_webhook_pushed_repo.json") as fh:
        payload = json.load(fh)
    payload["ref"] = "refs/heads/unstable"
    payload["repository"]["html_url"] = "https://github.com/redis/redis"
    payload.update(overrides)
    return payload


def _allowlisted():
    """Pin the trigger allowlists so the gates are not an ambient dependency."""
    return patch.multiple(
        app_module,
        BENCHMARK_TRIGGER_ORGS="redis",
        BENCHMARK_TRIGGER_BRANCHES="unstable",
    )


def test_push_event_baseline_entry_carries_before_sha_not_the_head_sha():
    """The merge-base entry must name the commit the push moved FROM.

    Both entries previously carried request_data["after"], so the "merge-base commit
    benchmark" measured the head commit: the baseline commit was never benchmarked and
    every push enqueued the same commit twice.
    """
    flask_app, auth_token = _mock_app()
    payload = _push_payload()
    before_sha, head_sha = payload["before"], payload["after"]
    assert (
        before_sha != head_sha
    ), "fixture must have distinct before/after to be meaningful"

    calls = []

    def _record(fields, *args, **kwargs):
        calls.append(dict(fields))
        return True, dict(fields), None

    with _allowlisted(), patch.object(
        app_module, "commit_schema_to_stream", side_effect=_record
    ):
        resp = _post_event(flask_app, auth_token, payload)

    assert resp.status_code == 200
    hashes = [c.get("git_hash") for c in calls]
    assert len(calls) == 2, f"expected a baseline and a head entry, got {hashes}"
    assert (
        before_sha in hashes
    ), f"no entry carries the baseline sha {before_sha}: {hashes}"
    assert head_sha in hashes, f"no entry carries the head sha {head_sha}: {hashes}"
    assert hashes[0] != hashes[1], f"both entries carry the same commit: {hashes}"


def test_push_event_with_created_ref_sentinel_enqueues_only_the_head():
    """A ref creation sends an all-zero "before", which names no commit.

    Enqueuing it would ask the builder to fetch a nonexistent archive, so the sentinel
    must be treated as "no baseline" rather than sent downstream.
    """
    flask_app, auth_token = _mock_app()
    payload = _push_payload(before="0" * 40)

    calls = []

    def _record(fields, *args, **kwargs):
        calls.append(dict(fields))
        return True, dict(fields), None

    with _allowlisted(), patch.object(
        app_module, "commit_schema_to_stream", side_effect=_record
    ):
        resp = _post_event(flask_app, auth_token, payload)

    assert resp.status_code == 200
    hashes = [c.get("git_hash") for c in calls]
    assert "0" * 40 not in hashes, f"all-zero sentinel was enqueued: {hashes}"
    assert hashes == [payload["after"]], f"expected only the head entry, got {hashes}"


def _record_calls():
    calls = []

    def _rec(fields, *args, **kwargs):
        calls.append(dict(fields))
        return True, dict(fields), None

    return calls, _rec


def test_deleted_ref_push_enqueues_nothing():
    """Deleting a ref must not benchmark the tip it just removed.

    A deletion arrives as a push with a real `before` and an all-zero `after`. Repointing the
    baseline entry at `before` would otherwise turn a harmless no-op into a full-suite run
    attributed to a ref that no longer exists.
    """
    flask_app, auth_token = _mock_app()
    payload = _push_payload(before="a" * 40, after="0" * 40, deleted=True)
    calls, rec = _record_calls()
    with _allowlisted(), patch.object(
        app_module, "commit_schema_to_stream", side_effect=rec
    ):
        resp = _post_event(flask_app, auth_token, payload)
    assert resp.status_code == 200
    assert calls == [], f"deletion enqueued {[c.get('git_hash') for c in calls]}"


def test_forced_push_does_not_benchmark_the_overwritten_commit():
    """A force-push's `before` is no longer on the branch, so it is not a valid baseline."""
    flask_app, auth_token = _mock_app()
    payload = _push_payload(forced=True)
    calls, rec = _record_calls()
    with _allowlisted(), patch.object(
        app_module, "commit_schema_to_stream", side_effect=rec
    ):
        resp = _post_event(flask_app, auth_token, payload)
    assert resp.status_code == 200
    assert [c.get("git_hash") for c in calls] == [payload["after"]]


def test_malformed_before_does_not_500():
    """A non-string `before` must be rejected, not raise.

    `set(0)` raises TypeError, so a shape-sniffing guard turned four payload types that
    previously returned 200 into 500s -- on events the allowlist may not even accept.
    """
    flask_app, auth_token = _mock_app()
    for bad in (0, 12345, True, 1.5, "", []):
        payload = _push_payload(before=bad)
        calls, rec = _record_calls()
        with _allowlisted(), patch.object(
            app_module, "commit_schema_to_stream", side_effect=rec
        ):
            resp = _post_event(flask_app, auth_token, payload)
        assert resp.status_code == 200, f"before={bad!r} returned {resp.status_code}"
        assert [c.get("git_hash") for c in calls] == [
            payload["after"]
        ], f"before={bad!r} enqueued {[c.get('git_hash') for c in calls]}"


def test_head_is_enqueued_before_the_baseline():
    """Order is load-bearing, not cosmetic.

    Each entry occupies a platform for a full suite and the queue is oversubscribed, so the
    second-enqueued entry is the one that starves. The coordinator baselines every automated
    regression table on the single newest point of by.branch/<branch>, so if the head starved
    the baseline would silently become the previous commit.
    """
    flask_app, auth_token = _mock_app()
    payload = _push_payload()
    calls, rec = _record_calls()
    with _allowlisted(), patch.object(
        app_module, "commit_schema_to_stream", side_effect=rec
    ):
        _post_event(flask_app, auth_token, payload)
    assert [c.get("git_hash") for c in calls] == [payload["after"], payload["before"]]
