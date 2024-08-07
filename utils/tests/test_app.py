#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import json
from hashlib import sha1
from hmac import HMAC

import redis

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
                    == "a3448f39efb8900f6f66778783461cf49de94b4f"
                )
                assert response.json["ref_label"] == "filipecosta90:unstable.55555"
                assert response.json["ref"] == "unstable.55555"
                assert conn.exists(STREAM_KEYNAME_GH_EVENTS_COMMIT)

        # Authorized and git pushes to repo
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
                assert (
                    response.json["git_hash"]
                    == "921489d5392a13e10493c6578a27b4bd5324a929"
                )
                assert response.json["ref_label"] == "refs/heads/unstable.55555"
                assert response.json["ref"] == "unstable.55555"
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
