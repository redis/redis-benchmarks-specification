#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs
#  All rights reserved.
#

import logging
from urllib.request import urlopen
from urllib.error import URLError
import logging.handlers
from flask import Flask, jsonify, request
from marshmallow import Schema, fields, ValidationError
from redis_benchmarks_specification import __version__
from json import dumps, loads
import redis
import argparse
from flask_httpauth import HTTPBasicAuth

from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
    GH_REDIS_SERVER_HOST,
    GH_REDIS_SERVER_PORT,
    GH_REDIS_SERVER_AUTH,
    REDIS_AUTH_SERVER_HOST,
    REDIS_AUTH_SERVER_PORT,
    LOG_FORMAT,
    LOG_DATEFMT,
    LOG_LEVEL,
)

auth = HTTPBasicAuth()

app = Flask(__name__)
conn = None


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
    return result


def commit_schema_to_stream(json_str: str):
    """ uses to the provided JSON dict of fields and pushes that info to the corresponding stream  """
    fields = loads(json_str)
    reply_fields = loads(json_str)
    result = False
    error_msg = None
    github_url = "https://github.com/redis/redis/archive/{}.zip".format(
        fields["git_hash"]
    )
    try:
        response = urlopen(github_url, timeout=5)
        content = response.read()
        fields["zip_archive"] = bytes(content)
        fields["zip_archive_len"] = len(bytes(content))
        reply_fields["archived_zip"] = True
        result = True
    except URLError as e:
        error_msg = "Catched URLError while fetching {} content. Error {}".format(
            github_url, e.__str__()
        )
        logging.error(error_msg)
        result = False

    if result is True:
        id = conn.xadd(STREAM_KEYNAME_GH_EVENTS_COMMIT, fields)
        reply_fields["id"] = id

    return result, reply_fields, error_msg


class CommitSchema(Schema):
    git_branch = fields.String(required=False)
    git_hash = fields.String(required=True)


@app.route("/api/gh/redis/redis/commits", methods=["POST"])
@auth.login_required
def base():
    # Get Request body from JSON
    request_data = request.json
    schema = CommitSchema()
    try:
        # Validate request body against schema data types
        result = schema.load(request_data)
    except ValidationError as err:
        # Return a nice message if validation fails
        return jsonify(err.messages), 400

    # Convert request body back to JSON str
    data_now_json_str = dumps(result)

    result, response_data, err_message = commit_schema_to_stream(data_now_json_str)
    if result is False:
        return jsonify(err_message), 400

    # Send data back as JSON
    return jsonify(response_data), 200


def main():
    global conn
    parser = argparse.ArgumentParser(
        description="redis-benchmarks-specification API",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    logging.info("redis-benchmarks-specification API. v{}".format(__version__))
    parser.add_argument(
        "--logname", type=str, default=None, help="logname to write the logs to"
    )
    args = parser.parse_args()
    print(
        "Using redis available at: {}:{} for event store.".format(
            GH_REDIS_SERVER_HOST, GH_REDIS_SERVER_PORT
        )
    )
    print(
        "Using redis available at: {}:{} as auth server.".format(
            REDIS_AUTH_SERVER_HOST, REDIS_AUTH_SERVER_PORT
        )
    )

    if args.logname is not None:
        print("Writting log to {}".format(args.logname))
        handler = logging.handlers.RotatingFileHandler(
            args.logname, maxBytes=1024 * 1024
        )
        logging.getLogger("werkzeug").setLevel(logging.DEBUG)
        logging.getLogger("werkzeug").addHandler(handler)
        app.logger.setLevel(LOG_LEVEL)
        app.logger.addHandler(handler)
    else:
        # logging settings
        logging.basicConfig(
            format=LOG_FORMAT,
            level=LOG_LEVEL,
            datefmt=LOG_DATEFMT,
        )
    logging.info(
        "Using redis available at: {}:{}".format(
            GH_REDIS_SERVER_HOST, GH_REDIS_SERVER_PORT
        )
    )
    conn = redis.StrictRedis(
        host=GH_REDIS_SERVER_HOST,
        port=GH_REDIS_SERVER_PORT,
        decode_responses=True,
        password=GH_REDIS_SERVER_AUTH,
    )

    app.run(host="0.0.0.0")


if __name__ == "__main__":
    main()
