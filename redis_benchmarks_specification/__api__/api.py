#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs
#  All rights reserved.
#

import logging
import logging.handlers
from redis_benchmarks_specification import __version__
import redis
import argparse

from redis_benchmarks_specification.__api__.app import create_app
from redis_benchmarks_specification.__common__.env import (
    GH_REDIS_SERVER_HOST,
    GH_REDIS_SERVER_PORT,
    GH_REDIS_SERVER_AUTH,
    LOG_FORMAT,
    LOG_DATEFMT,
    LOG_LEVEL,
    REDIS_HEALTH_CHECK_INTERVAL,
    REDIS_SOCKET_TIMEOUT,
    GH_REDIS_SERVER_USER,
)
from redis_benchmarks_specification.__common__.package import (
    populate_with_poetry_data,
    get_version_string,
)


def main():
    _, _, project_version = populate_with_poetry_data()
    project_name = "redis-benchmarks-specification API"
    global conn
    parser = argparse.ArgumentParser(
        description=get_version_string(project_name, project_version),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    logging.info("redis-benchmarks-specification API. v{}".format(__version__))
    parser.add_argument(
        "--logname", type=str, default=None, help="logname to write the logs to"
    )
    parser.add_argument("--port", type=int, default=5000, help="port")
    args = parser.parse_args()
    print(
        "Using redis available at: {}:{} for event store.".format(
            GH_REDIS_SERVER_HOST, GH_REDIS_SERVER_PORT
        )
    )
    conn = redis.StrictRedis(
        host=GH_REDIS_SERVER_HOST,
        port=GH_REDIS_SERVER_PORT,
        decode_responses=True,
        password=GH_REDIS_SERVER_AUTH,
        health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
        socket_connect_timeout=REDIS_SOCKET_TIMEOUT,
        socket_keepalive=True,
    )
    app = create_app(conn, GH_REDIS_SERVER_USER)
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
    logging.info(get_version_string(project_name, project_version))
    logging.info(
        "Using redis available at: {}:{}".format(
            GH_REDIS_SERVER_HOST, GH_REDIS_SERVER_PORT
        )
    )
    logging.info("Listening on port {}".format(args.port))
    app.run(host="0.0.0.0", port=args.port)


if __name__ == "__main__":
    main()
