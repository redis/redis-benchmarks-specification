#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import argparse
import datetime
import logging
import time

import redis

# logging settings
from redisbench_admin.run.common import get_start_time_vars
from redistimeseries.client import Client

from redis_benchmarks_specification.__watchdog__.args import spec_watchdog_args
from redis_benchmarks_specification.__common__.package import (
    get_version_string,
    populate_with_poetry_data,
)

logging.basicConfig(
    format="%(asctime)s %(levelname)-4s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main():
    _, _, project_version = populate_with_poetry_data()
    project_name = "redis-benchmarks-spec-watchdog"
    parser = argparse.ArgumentParser(
        description=get_version_string(project_name, project_version),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = spec_watchdog_args(parser)
    args = parser.parse_args()

    cli_command_logic(args, project_name, project_version)


def cli_command_logic(args, project_name, project_version):
    logging.info(
        "Using: {project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )
    logging.info("Checking connection to RedisTimeSeries.")
    rts = Client(
        host=args.redis_host,
        port=args.redis_port,
        password=args.redis_pass,
    )

    # tsname_overall_sc_coordinator_running = "{}/sc-coordinator/state:running".format(
    #     prefix
    # )
    # tsname_overall_api_running = "{}/api/state:running".format(prefix)
    # tsname_overall_builder_running = "{}/builder/state:running".format(prefix)

    rts.redis.ping()
    update_interval = args.update_interval
    logging.info(
        "Entering watching loop. Ticking every {} secs".format(update_interval)
    )
    while True:
        starttime, start_time_ms, _ = get_start_time_vars()
        try:

            prefix = "mgt-timeseries::benchmarks.redis.io/redis/redis::"
            create_consumer_group_rts_metrics(
                args.dry_run,
                prefix,
                args.events_stream_keyname_commits,
                "event stream from github webhook",
                rts,
                start_time_ms,
            )
            create_consumer_group_rts_metrics(
                args.dry_run,
                prefix,
                args.events_stream_keyname_builds,
                "event stream from built artifacts",
                rts,
                start_time_ms,
            )
        except redis.exceptions.ConnectionError as e:
            logging.error(
                "Detected an error while writing data to rts: {}".format(e.__str__())
            )
        sleep_time_secs = float(update_interval) - (
            (datetime.datetime.now() - starttime).total_seconds()
            % float(update_interval)
        )
        logging.info("Sleeping for {} secs".format(sleep_time_secs))
        time.sleep(sleep_time_secs)


def create_consumer_group_rts_metrics(
    dry_run, prefix, stream_name, event_stream_desc, rts, start_time_ms
):
    groups = rts.redis.xinfo_groups(stream_name)
    stream_info = rts.redis.xinfo_stream(stream_name)
    total_events = stream_info["length"]
    total_cgroups = stream_info["groups"]

    labels_dict = {
        "metric": "consumer_groups:total",
        "type": "mgt-timeseries",
        "stream": stream_name,
        "github_org_repo": "redis/redis",
    }
    tsname_overall_cgrougs_gh_events = "{}stream={}:consumer_groups:total".format(
        prefix, stream_name
    )
    logging.info(
        "stream={}; consumer_groups:total={}".format(stream_name, total_cgroups)
    )
    if dry_run is False:
        rts.add(
            tsname_overall_cgrougs_gh_events,
            start_time_ms,
            total_cgroups,
            labels=labels_dict,
        )
    labels_dict["metric"] = "total_events"
    tsname_total_events_gh_events = "{}stream={}:events:total".format(
        prefix, stream_name
    )
    logging.info("stream={}; total_events={}".format(stream_name, total_events))
    if dry_run is False:
        rts.add(
            tsname_total_events_gh_events,
            start_time_ms,
            total_events,
            labels=labels_dict,
        )

    for consumer_group in groups:
        consumer_group_name = consumer_group["name"]
        labels_dict["consumer_group"] = consumer_group_name
        events_prefix = "{}stream={}:consumer_group={}:events:".format(
            prefix, stream_name, consumer_group_name
        )
        consumer_group_pending = consumer_group["pending"]
        lag = total_events - consumer_group_pending
        tsname_pending_gh_events = "{}pending".format(events_prefix)
        tsname_total_gh_events = "{}total".format(events_prefix)
        logging.info("cg={}; total_events={}".format(consumer_group_name, total_events))
        logging.info(
            "cg={}; pending={}".format(consumer_group_name, consumer_group_pending)
        )
        if dry_run is False:
            labels_dict["metric"] = "events:pending"
            labels_dict["metric+consumer_group"] = "{} {}".format(
                "events:pending", consumer_group_name
            )
            rts.add(
                tsname_pending_gh_events,
                start_time_ms,
                consumer_group_pending,
                labels=labels_dict,
            )
            labels_dict["metric"] = "events:total"
            labels_dict["metric+consumer_group"] = "{} {}".format(
                "events:total", consumer_group_name
            )
            rts.add(
                tsname_total_gh_events,
                start_time_ms,
                lag,
                labels=labels_dict,
            )
