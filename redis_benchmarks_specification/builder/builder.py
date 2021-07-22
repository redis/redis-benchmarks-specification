import argparse
import logging

import redis

from redis_benchmarks_specification.common.env import (
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
    GH_REDIS_SERVER_HOST,
    GH_REDIS_SERVER_PORT,
    GH_REDIS_SERVER_AUTH,
    LOG_FORMAT,
    LOG_DATEFMT,
    LOG_LEVEL,
)


def main():
    global conn
    parser = argparse.ArgumentParser(
        description="redis-benchmarks-spec builder",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--logname", type=str, default=None, help="logname to write the logs to"
    )
    args = parser.parse_args()

    if args.logname is not None:
        print("Writting log to {}".format(args.logname))
        logging.basicConfig(
            filename=args.logname,
            filemode="a",
            format=LOG_FORMAT,
            datefmt=LOG_DATEFMT,
            level=LOG_LEVEL,
        )
    else:
        # logging settings
        logging.basicConfig(
            format=LOG_FORMAT,
            level=LOG_LEVEL,
            datefmt=LOG_DATEFMT,
        )
    logging.info(
        "Using redis available at: {}:{} to read the event streams".format(
            GH_REDIS_SERVER_HOST, GH_REDIS_SERVER_PORT
        )
    )
    conn = redis.StrictRedis(
        host=GH_REDIS_SERVER_HOST,
        port=GH_REDIS_SERVER_PORT,
        decode_responses=True,
        password=GH_REDIS_SERVER_AUTH,
    )
