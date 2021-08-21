from flask import Flask, jsonify, request
from marshmallow import ValidationError
from json import dumps
import redis
from flask_httpauth import HTTPBasicAuth

from redis_benchmarks_specification.__api__.schema import (
    CommitSchema,
)
from redis_benchmarks_specification.__common__.builder_schema import (
    commit_schema_to_stream,
)
from redis_benchmarks_specification.__common__.env import (
    REDIS_AUTH_SERVER_HOST,
    REDIS_AUTH_SERVER_PORT,
)


def create_app(conn, test_config=None):
    app = Flask(__name__)
    auth = HTTPBasicAuth()
    conn = conn

    @auth.verify_password
    def verify_password(username, password):
        result = False
        try:
            auth_server_conn = redis.StrictRedis(
                host=REDIS_AUTH_SERVER_HOST,
                port=REDIS_AUTH_SERVER_PORT,
                decode_responses=True,
                username=username,
                password=password,
            )
            auth_server_conn.ping()
            result = True
        except redis.exceptions.ResponseError:
            result = False
        except redis.exceptions.AuthenticationError:
            result = False
        return result

    @app.route("/api/gh/redis/redis/commits", methods=["POST"])
    @auth.login_required
    def base():
        # Get Request body from JSON
        request_data = request.json
        gh_org = "redis"
        gh_repo = "redis"
        schema = CommitSchema()
        try:
            # Validate request body against schema data types
            result = schema.load(request_data)
        except ValidationError as err:
            err_message = err.messages
        if result is True:
            # Convert request body back to JSON str
            data_now_json_str = dumps(result)

            result, response_data, err_message = commit_schema_to_stream(
                data_now_json_str, conn, gh_org, gh_repo
            )
        if result is False:
            return jsonify(err_message), 400

        # Send data back as JSON
        return jsonify(response_data), 200

    return app
