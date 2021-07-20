#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs
#  All rights reserved.
#

import logging
import logging.handlers
import os
from flask import Flask, jsonify, request
from marshmallow import Schema, fields, ValidationError
from redis_benchmarks_specification import __version__
from json import dumps, loads
import redis
import argparse
from flask_httpauth import HTTPBasicAuth

auth = HTTPBasicAuth()

VERBOSE = os.getenv("VERBOSE", "1") == "0"
STREAM_KEYNAME_GH_EVENTS_COMMIT = os.getenv(
    "STREAM_KEYNAME_GH_EVENTS_COMMIT", "oss:api:gh/redis/redis/commits"
)

# host used to store the streams of events
GH_REDIS_SERVER_HOST = os.getenv("GH_REDIS_SERVER_HOST", "localhost")
GH_REDIS_SERVER_PORT = int(os.getenv("GH_REDIS_SERVER_PORT", "6379"))
GH_REDIS_SERVER_AUTH = os.getenv("GH_REDIS_SERVER_AUTH", None)

# DB used to authenticate ( read-only/non-dangerous access only )
REDIS_AUTH_SERVER_HOST = os.getenv("REDIS_AUTH_SERVER_HOST", "localhost")
REDIS_AUTH_SERVER_PORT = int(os.getenv("REDIS_AUTH_SERVER_PORT", "6380"))

LOG_LEVEL = logging.INFO
if VERBOSE:
    LOG_LEVEL = logging.WARN
LOG_FORMAT = "%(asctime)s %(levelname)-4s %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"


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
    id = conn.xadd(STREAM_KEYNAME_GH_EVENTS_COMMIT, fields)
    fields["id"] = id
    return fields


class CommitSchema(Schema):
    git_branch = fields.String(required=False)
    git_hash = fields.String(required=True)


app = Flask(__name__)


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

    response_data = commit_schema_to_stream(data_now_json_str)

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

    app.run()


if __name__ == "__main__":
    main()
