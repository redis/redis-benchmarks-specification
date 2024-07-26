import json

from flask import jsonify
import redis
from flask import Flask, request
from hmac import HMAC, compare_digest
from hashlib import sha1

from redis_benchmarks_specification.__common__.builder_schema import (
    commit_schema_to_stream,
)
from redis_benchmarks_specification.__common__.env import PULL_REQUEST_TRIGGER_LABEL

SIG_HEADER = "X-Hub-Signature"


def should_action(action):
    res = False
    types = ["synchronize", "opened", "reopened", "labeled"]
    for tt in types:
        if action in tt:
            res = True
    return res


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
            trigger_label = PULL_REQUEST_TRIGGER_LABEL
            if "pull_request" in request_data:
                action = request_data["action"]
                if should_action(action):
                    pull_request_dict = request_data["pull_request"]

                    head_dict = pull_request_dict["head"]
                    repo_dict = head_dict["repo"]
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
                sha = request_data["after"]
                before_sha = request_data["before"]
                use_event = True
                event_type = "Git pushes to repo"

            if use_event is True:
                if before_sha is not None:
                    fields_before = {
                        "git_hash": sha,
                        "ref_label": ref_label,
                        "ref": ref,
                        "gh_repo": gh_repo,
                        "gh_org": gh_org,
                    }
                    app.logger.info(
                        "Using event {} to trigger merge-base commit benchmark. final fields: {}".format(
                            event_type, fields_before
                        )
                    )
                    result, response_data, err_message = commit_schema_to_stream(
                        fields_before, conn, gh_org, gh_repo
                    )
                    app.logger.info(
                        "Using event {} to trigger merge-base commit benchmark. final fields: {}".format(
                            event_type, response_data
                        )
                    )
                fields_after = {
                    "git_hash": sha,
                    "ref_label": ref_label,
                    "ref": ref,
                    "gh_repo": gh_repo,
                    "gh_org": gh_org,
                }
                if pull_request_number is not None:
                    fields_after["pull_request"] = pull_request_number
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
