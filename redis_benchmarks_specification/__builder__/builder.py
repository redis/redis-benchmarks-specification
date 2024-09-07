import argparse
import datetime
import io
import json
import logging
import tempfile
import shutil
import docker
import redis
import os
from zipfile import ZipFile, ZipInfo

from redis_benchmarks_specification.__builder__.schema import (
    get_build_config,
    get_build_config_metadata,
)
from redis_benchmarks_specification.__common__.builder_schema import (
    get_branch_version_from_test_details,
)
from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
    GH_REDIS_SERVER_HOST,
    GH_REDIS_SERVER_PORT,
    GH_REDIS_SERVER_AUTH,
    LOG_FORMAT,
    LOG_DATEFMT,
    LOG_LEVEL,
    SPECS_PATH_SETUPS,
    STREAM_GH_EVENTS_COMMIT_BUILDERS_CG,
    STREAM_KEYNAME_NEW_BUILD_EVENTS,
    REDIS_HEALTH_CHECK_INTERVAL,
    REDIS_SOCKET_TIMEOUT,
    REDIS_BINS_EXPIRE_SECS,
)
from redis_benchmarks_specification.__common__.github import (
    check_github_available_and_actionable,
    generate_build_finished_pr_comment,
    update_comment_if_needed,
    create_new_pr_comment,
    generate_build_started_pr_comment,
)
from redis_benchmarks_specification.__common__.package import (
    populate_with_poetry_data,
    get_version_string,
)

PERFORMANCE_GH_TOKEN = os.getenv("PERFORMANCE_GH_TOKEN", None)


class ZipFileWithPermissions(ZipFile):
    def _extract_member(self, member, targetpath, pwd):
        if not isinstance(member, ZipInfo):
            member = self.getinfo(member)

        targetpath = super()._extract_member(member, targetpath, pwd)

        attr = member.external_attr >> 16
        if attr != 0:
            os.chmod(targetpath, attr)
        return targetpath


def main():
    _, _, project_version = populate_with_poetry_data()
    project_name = "redis-benchmarks-spec builder"
    parser = argparse.ArgumentParser(
        description=get_version_string(project_name, project_version),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--logname", type=str, default=None, help="logname to write the logs to"
    )
    parser.add_argument(
        "--arch", type=str, default="amd64", help="arch to build artifacts"
    )
    parser.add_argument(
        "--setups-folder",
        type=str,
        default=SPECS_PATH_SETUPS,
        help="Setups folder, containing the build environment variations sub-folder that we use to trigger different build artifacts",
    )
    parser.add_argument(
        "--consumer-start-id",
        type=str,
        default=">",
    )
    parser.add_argument(
        "--docker-air-gap",
        default=False,
        action="store_true",
        help="Store the docker images in redis keys.",
    )
    parser.add_argument("--github_token", type=str, default=PERFORMANCE_GH_TOKEN)
    parser.add_argument("--pull-request", type=str, default=None, nargs="?", const="")
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
    logging.info(get_version_string(project_name, project_version))
    builders_folder = os.path.abspath(args.setups_folder + "/builders")
    logging.info("Using package dir {} for inner file paths".format(builders_folder))
    different_build_specs = os.listdir(builders_folder)
    logging.info(
        "Using the following build specs folder {}, containing {} different specs.".format(
            builders_folder, len(different_build_specs)
        )
    )

    logging.info(
        "Using redis available at: {}:{} to read the event streams".format(
            GH_REDIS_SERVER_HOST, GH_REDIS_SERVER_PORT
        )
    )
    try:
        conn = redis.StrictRedis(
            host=GH_REDIS_SERVER_HOST,
            port=GH_REDIS_SERVER_PORT,
            decode_responses=False,  # dont decode due to zip archive
            password=GH_REDIS_SERVER_AUTH,
            health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
            socket_connect_timeout=REDIS_SOCKET_TIMEOUT,
            socket_keepalive=True,
        )
        conn.ping()
    except redis.exceptions.ConnectionError as e:
        logging.error(
            "Unable to connect to redis available at: {}:{} to read the event streams".format(
                GH_REDIS_SERVER_HOST, GH_REDIS_SERVER_PORT
            )
        )
        logging.error("Error message {}".format(e.__str__()))
        exit(1)

    arch = args.arch
    logging.info("Building for arch: {}".format(arch))

    build_spec_image_prefetch(builders_folder, different_build_specs)

    builder_consumer_group_create(conn)
    if args.github_token is not None:
        logging.info("detected a github token. will update as much as possible!!! =)")
    previous_id = args.consumer_start_id
    while True:
        previous_id, new_builds_count, _ = builder_process_stream(
            builders_folder,
            conn,
            different_build_specs,
            previous_id,
            args.docker_air_gap,
            arch,
            args.github_token,
        )


def builder_consumer_group_create(conn, id="$"):
    try:
        conn.xgroup_create(
            STREAM_KEYNAME_GH_EVENTS_COMMIT,
            STREAM_GH_EVENTS_COMMIT_BUILDERS_CG,
            mkstream=True,
            id=id,
        )
        logging.info(
            "Created consumer group named {} to distribute work.".format(
                STREAM_GH_EVENTS_COMMIT_BUILDERS_CG
            )
        )
    except redis.exceptions.ResponseError:
        logging.info(
            "Consumer group named {} already existed.".format(
                STREAM_GH_EVENTS_COMMIT_BUILDERS_CG
            )
        )


def check_benchmark_build_comment(comments):
    res = False
    pos = -1
    for n, comment in enumerate(comments):
        body = comment.body
        if "CE Performance Automation : step 1 of 2" in body:
            res = True
            pos = n
    return res, pos


def builder_process_stream(
    builders_folder,
    conn,
    different_build_specs,
    previous_id,
    docker_air_gap=False,
    arch="amd64",
    github_token=None,
):
    new_builds_count = 0
    auto_approve_github_comments = True
    build_stream_fields_arr = []
    logging.info("Entering blocking read waiting for work.")
    consumer_name = "{}-proc#{}".format(STREAM_GH_EVENTS_COMMIT_BUILDERS_CG, "1")
    newTestInfo = conn.xreadgroup(
        STREAM_GH_EVENTS_COMMIT_BUILDERS_CG,
        consumer_name,
        {STREAM_KEYNAME_GH_EVENTS_COMMIT: previous_id},
        count=1,
        block=0,
    )

    if len(newTestInfo[0]) < 2 or len(newTestInfo[0][1]) < 1:
        previous_id = ">"
    else:
        streamId, testDetails = newTestInfo[0][1][0]
        logging.info("Received work . Stream id {}.".format(streamId))
        # commit = None
        # commited_date = ""
        # tag = ""
        docker_client = docker.from_env()
        from pathlib import Path

        home = str(Path.home())
        if b"git_hash" in testDetails:
            git_hash = testDetails[b"git_hash"]
            logging.info("Received commit hash specifier {}.".format(git_hash))
            logging.info(f"Received the following build stream: {testDetails}.")
            binary_zip_key = testDetails[b"zip_archive_key"]
            logging.info(
                "Retriving zipped source from key {}.".format(
                    testDetails[b"zip_archive_key"]
                )
            )
            buffer = conn.get(binary_zip_key)
            git_timestamp_ms = None
            use_git_timestamp = False
            commit_datetime = "n/a"
            if b"commit_datetime" in testDetails:
                commit_datetime = testDetails[b"commit_datetime"].decode()
            commit_summary = "n/a"
            if b"commit_summary" in testDetails:
                commit_summary = testDetails[b"commit_summary"].decode()
            git_branch, git_version = get_branch_version_from_test_details(testDetails)
            if b"use_git_timestamp" in testDetails:
                use_git_timestamp = bool(testDetails[b"use_git_timestamp"])
            if b"git_timestamp_ms" in testDetails:
                git_timestamp_ms = int(testDetails[b"git_timestamp_ms"].decode())
            tests_regexp = ".*"
            if b"tests_regexp" in testDetails:
                tests_regexp = testDetails[b"tests_regexp"].decode()
            tests_priority_upper_limit = 10000
            if b"tests_priority_upper_limit" in testDetails:
                tests_priority_upper_limit = int(
                    testDetails[b"tests_priority_upper_limit"].decode()
                )
            tests_priority_lower_limit = 0
            if b"tests_priority_lower_limit" in testDetails:
                tests_priority_lower_limit = int(
                    testDetails[b"tests_priority_lower_limit"].decode()
                )
            tests_groups_regexp = ".*"
            if b"tests_groups_regexp" in testDetails:
                tests_groups_regexp = testDetails[b"tests_groups_regexp"].decode()

            github_org = "redis"
            if b"github_org" in testDetails:
                github_org = testDetails[b"github_org"].decode()
                logging.info(f"detected github_org info on build stream {github_org}")

            github_repo = "redis"
            if b"github_repo" in testDetails:
                github_repo = testDetails[b"github_repo"].decode()
                logging.info(f"detected github_repo info on build stream {github_repo}")

            # github updates
            is_actionable_pr = False
            contains_regression_comment = False
            github_pr = None
            old_regression_comment_body = ""
            pr_link = ""
            regression_comment = ""
            pull_request = None
            if b"pull_request" in testDetails:
                pull_request = testDetails[b"pull_request"].decode()
                logging.info(f"Detected PR info in builder. PR: {pull_request}")
                verbose = True

                fn = check_benchmark_build_comment
                (
                    contains_regression_comment,
                    github_pr,
                    is_actionable_pr,
                    old_regression_comment_body,
                    pr_link,
                    regression_comment,
                ) = check_github_available_and_actionable(
                    fn, github_token, pull_request, "redis", "redis", verbose
                )
            for build_spec in different_build_specs:
                build_config, id = get_build_config(builders_folder + "/" + build_spec)
                build_config_metadata = get_build_config_metadata(build_config)

                build_image = build_config["build_image"]
                build_arch = build_config["arch"]
                if build_arch != arch:
                    logging.info(
                        "skipping build spec {} given arch {}!={}".format(
                            build_spec, build_arch, arch
                        )
                    )
                    continue
                run_image = build_image
                if "run_image" in build_config:
                    run_image = build_config["run_image"]
                if docker_air_gap:
                    store_airgap_image_redis(conn, docker_client, run_image)

                compiler = build_config["compiler"]
                cpp_compiler = build_config["cpp_compiler"]
                build_os = build_config["os"]

                build_artifacts = ["redis-server"]
                if "build_artifacts" in build_config:
                    build_artifacts = build_config["build_artifacts"]
                if b"build_artifacts" in testDetails:
                    new_build_artifacts = (
                        testDetails[b"build_artifacts"].decode().split(",")
                    )
                    logging.info(
                        f"overriding default build artifacts {build_artifacts} by {new_build_artifacts}"
                    )
                    build_artifacts = new_build_artifacts
                build_vars_str = ""
                if "env" in build_config:
                    if build_config["env"] is not None:
                        for k, v in build_config["env"].items():
                            build_vars_str += '{}="{}" '.format(k, v)

                temporary_dir = tempfile.mkdtemp(dir=home)
                logging.info(
                    "Using local temporary dir to persist redis build artifacts. Path: {}".format(
                        temporary_dir
                    )
                )
                z = ZipFileWithPermissions(io.BytesIO(buffer))
                z.extractall(temporary_dir)
                redis_dir = os.listdir(temporary_dir + "/")[0]
                deps_dir = os.listdir(temporary_dir + "/" + redis_dir + "/deps")
                deps_list = [
                    "hiredis",
                    "jemalloc",
                    "linenoise",
                    "lua",
                ]
                if "fast_float" in deps_dir:
                    deps_list.append("fast_float")
                if "hdr_histogram" in deps_dir:
                    deps_list.append("hdr_histogram")
                if "fpconv" in deps_dir:
                    deps_list.append("fpconv")
                redis_temporary_dir = temporary_dir + "/" + redis_dir + "/"
                logging.info("Using redis temporary dir {}".format(redis_temporary_dir))
                # build_command = "bash -c 'make Makefile.dep && cd ./deps && CXX={} CC={} make {} {} -j && cd .. && CXX={} CC={} make {} {} -j'".format(
                #     cpp_compiler,
                #     compiler,
                #     " ".join(deps_list),
                #     build_vars_str,
                #     cpp_compiler,
                #     compiler,
                #     "redis-server",
                #     build_vars_str,
                # )
                build_command = "sh -c 'make -j'"
                if b"build_command" in testDetails:
                    build_command = testDetails[b"build_command"].decode()
                server_name = "redis"
                if b"server_name" in testDetails:
                    server_name = testDetails[b"server_name"].decode()

                build_start_datetime = datetime.datetime.utcnow()
                logging.info(
                    "Using the following build command {}.".format(build_command)
                )
                if is_actionable_pr:
                    logging.info(
                        f"updating on github we'll start the build at {build_start_datetime}"
                    )
                    comment_body = generate_build_started_pr_comment(
                        build_start_datetime,
                        commit_datetime,
                        commit_summary,
                        git_branch,
                        git_hash,
                        tests_groups_regexp,
                        tests_priority_lower_limit,
                        tests_priority_upper_limit,
                        tests_regexp,
                    )
                    if contains_regression_comment:
                        update_comment_if_needed(
                            auto_approve_github_comments,
                            comment_body,
                            old_regression_comment_body,
                            regression_comment,
                            verbose,
                        )
                    else:
                        regression_comment = create_new_pr_comment(
                            auto_approve_github_comments,
                            comment_body,
                            github_pr,
                            pr_link,
                        )

                docker_client.containers.run(
                    image=build_image,
                    volumes={
                        redis_temporary_dir: {"bind": "/mnt/redis/", "mode": "rw"},
                    },
                    auto_remove=True,
                    privileged=True,
                    working_dir="/mnt/redis/",
                    command=build_command,
                )
                build_end_datetime = datetime.datetime.utcnow()
                build_duration = build_end_datetime - build_start_datetime
                build_duration_secs = build_duration.total_seconds()

                build_stream_fields, result = generate_benchmark_stream_request(
                    id,
                    conn,
                    run_image,
                    build_arch,
                    testDetails,
                    build_os,
                    build_artifacts,
                    build_command,
                    build_config_metadata,
                    build_image,
                    build_vars_str,
                    compiler,
                    cpp_compiler,
                    git_branch,
                    git_hash,
                    git_timestamp_ms,
                    git_version,
                    pull_request,
                    redis_temporary_dir,
                    tests_groups_regexp,
                    tests_priority_lower_limit,
                    tests_priority_upper_limit,
                    tests_regexp,
                    use_git_timestamp,
                    server_name,
                    github_org,
                    github_repo,
                )
                if result is True:
                    benchmark_stream_id = conn.xadd(
                        STREAM_KEYNAME_NEW_BUILD_EVENTS, build_stream_fields
                    )
                    logging.info(
                        "sucessfully built build variant {} for redis git_sha {}. Stream id: {}".format(
                            id, git_hash, benchmark_stream_id
                        )
                    )
                    streamId_decoded = streamId.decode()
                    benchmark_stream_id_decoded = benchmark_stream_id.decode()
                    builder_list_completed = (
                        f"builder:{streamId_decoded}:builds_completed"
                    )
                    conn.lpush(builder_list_completed, benchmark_stream_id_decoded)
                    conn.expire(builder_list_completed, REDIS_BINS_EXPIRE_SECS)
                    logging.info(
                        f"Adding information of build->benchmark stream info in list {builder_list_completed}. Adding benchmark stream id: {benchmark_stream_id_decoded}"
                    )
                    benchmark_stream_ids = [benchmark_stream_id_decoded]

                    if is_actionable_pr:
                        logging.info(
                            f"updating on github that the build finished after {build_duration_secs} seconds"
                        )
                        comment_body = generate_build_finished_pr_comment(
                            benchmark_stream_ids,
                            commit_datetime,
                            commit_summary,
                            git_branch,
                            git_hash,
                            tests_groups_regexp,
                            tests_priority_lower_limit,
                            tests_priority_upper_limit,
                            tests_regexp,
                            build_start_datetime,
                            build_duration_secs,
                        )
                        if contains_regression_comment:
                            update_comment_if_needed(
                                auto_approve_github_comments,
                                comment_body,
                                old_regression_comment_body,
                                regression_comment,
                                verbose,
                            )
                        else:
                            create_new_pr_comment(
                                auto_approve_github_comments,
                                comment_body,
                                github_pr,
                                pr_link,
                            )
                shutil.rmtree(temporary_dir, ignore_errors=True)
                new_builds_count = new_builds_count + 1
                build_stream_fields_arr.append(build_stream_fields)
            ack_reply = conn.xack(
                STREAM_KEYNAME_GH_EVENTS_COMMIT,
                STREAM_GH_EVENTS_COMMIT_BUILDERS_CG,
                streamId,
            )
            if type(ack_reply) == bytes:
                ack_reply = ack_reply.decode()
            if ack_reply == "1" or ack_reply == 1:
                logging.info(
                    "Sucessfully acknowledge build variation stream with id {}.".format(
                        streamId
                    )
                )
            else:
                logging.error(
                    "Unable to acknowledge build variation stream with id {}. XACK reply {}".format(
                        streamId, ack_reply
                    )
                )
        else:
            logging.error("Missing commit information within received message.")
    return previous_id, new_builds_count, build_stream_fields_arr


def store_airgap_image_redis(conn, docker_client, run_image):
    airgap_key = "docker:air-gap:{}".format(run_image)
    logging.info(
        "DOCKER AIR GAP: storing run image named: {} in redis key {}".format(
            run_image, airgap_key
        )
    )
    # 7 days expire
    binary_exp_secs = 24 * 60 * 60 * 7
    if conn.exists(airgap_key):
        logging.info(
            f"DOCKER AIRGAP KEY ALREADY EXISTS: {airgap_key}. Updating only the expire time"
        )
        conn.expire(airgap_key, binary_exp_secs)
    else:
        run_image_binary_stream = io.BytesIO()
        run_image_docker = docker_client.images.get(run_image)
        for chunk in run_image_docker.save():
            run_image_binary_stream.write(chunk)
        res_airgap = conn.set(
            airgap_key,
            run_image_binary_stream.getbuffer(),
            ex=binary_exp_secs,
        )
        logging.info(
            "DOCKER AIR GAP: result of set bin data to {}: {}".format(
                airgap_key, res_airgap
            )
        )


def generate_benchmark_stream_request(
    id,
    conn,
    run_image,
    build_arch,
    testDetails,
    build_os,
    build_artifacts=[],
    build_command=None,
    build_config_metadata=None,
    build_image=None,
    build_vars_str=None,
    compiler=None,
    cpp_compiler=None,
    git_branch=None,
    git_hash=None,
    git_timestamp_ms=None,
    git_version=None,
    pull_request=None,
    redis_temporary_dir=None,
    tests_groups_regexp=".*",
    tests_priority_lower_limit=0,
    tests_priority_upper_limit=10000,
    tests_regexp=".*",
    use_git_timestamp=False,
    server_name="redis",
    github_org="redis",
    github_repo="redis",
):
    build_stream_fields = {
        "id": id,
        "use_git_timestamp": str(use_git_timestamp),
        "run_image": run_image,
        "os": build_os,
        "arch": build_arch,
        "build_artifacts": ",".join(build_artifacts),
        "tests_regexp": tests_regexp,
        "tests_priority_upper_limit": tests_priority_upper_limit,
        "tests_priority_lower_limit": tests_priority_lower_limit,
        "tests_groups_regexp": tests_groups_regexp,
        "server_name": server_name,
        "github_org": github_org,
        "github_repo": github_repo,
    }
    if build_config_metadata is not None:
        build_stream_fields["metadata"] = json.dumps(build_config_metadata)
    if compiler is not None:
        build_stream_fields["compiler"] = compiler
    if cpp_compiler is not None:
        build_stream_fields["cpp_compiler"] = cpp_compiler
    if build_vars_str is not None:
        build_stream_fields["build_vars"] = build_vars_str
    if build_command is not None:
        logging.info(f"adding build_command: {build_command}")
        build_stream_fields["build_command"] = build_command
    if build_image is not None:
        build_stream_fields["build_image"] = build_image
    else:
        build_stream_fields["build_image"] = run_image
    if git_hash is not None:
        build_stream_fields["git_hash"] = git_hash
    if pull_request is not None:
        build_stream_fields["pull_request"] = pull_request
    if git_branch is not None:
        build_stream_fields["git_branch"] = git_branch
    if git_version is not None:
        build_stream_fields["git_version"] = git_version
    if git_timestamp_ms is not None:
        build_stream_fields["git_timestamp_ms"] = git_timestamp_ms

    prefix = f"github_org={github_org}/github_repo={github_repo}/git_branch={str(git_branch)}/git_version={str(git_version)}/git_hash={str(git_hash)}"
    for artifact in build_artifacts:
        bin_key = f"zipped:artifacts:{prefix}:{id}:{artifact}.zip"
        bin_artifact = open(f"{redis_temporary_dir}src/{artifact}", "rb").read()
        bin_artifact_len = len(bytes(bin_artifact))
        assert bin_artifact_len > 0
        conn.set(bin_key, bytes(bin_artifact), ex=REDIS_BINS_EXPIRE_SECS)
        build_stream_fields[artifact] = bin_key
        build_stream_fields["{}_len_bytes".format(artifact)] = bin_artifact_len
    result = True
    if b"platform" in testDetails:
        build_stream_fields["platform"] = testDetails[b"platform"]
    return build_stream_fields, result


def build_spec_image_prefetch(builders_folder, different_build_specs):
    logging.info("checking build spec requirements")
    already_checked_images = []
    hub_pulled_images = 0
    client = docker.from_env()
    for build_spec in different_build_specs:
        build_config, id = get_build_config(builders_folder + "/" + build_spec)
        if build_config["kind"] == "docker":
            build_image = build_config["build_image"]
            hub_pulled_images = check_docker_image_available(
                already_checked_images, build_image, client, hub_pulled_images, id
            )
            if "run_image" in build_config:
                run_image = build_config["run_image"]
                hub_pulled_images = check_docker_image_available(
                    already_checked_images, run_image, client, hub_pulled_images, id
                )
    return already_checked_images, hub_pulled_images


def check_docker_image_available(
    already_checked_images, build_image, client, hub_pulled_images, id
):
    if build_image not in already_checked_images:
        logging.info(
            "Build {} requirement: checking docker image {} is available.".format(
                id, build_image
            )
        )
        local_images = [
            x.tags[0] for x in client.images.list(filters={"reference": build_image})
        ]
        if build_image not in local_images:
            logging.info(
                "Build {} requirement: docker image {} is not available locally. Fetching it from hub".format(
                    id, build_image
                )
            )
            client.images.pull(build_image)
            hub_pulled_images = hub_pulled_images + 1
        else:
            logging.info(
                "Build {} requirement: docker image {} is available locally.".format(
                    id, build_image
                )
            )
        already_checked_images.append(build_image)
    else:
        logging.info(
            "Build {} requirement: docker image {} availability was already checked.".format(
                id, build_image
            )
        )
    return hub_pulled_images
