import argparse
import io
import logging
import tempfile
import shutil
import redis
import os
from zipfile import ZipFile, ZipInfo

from redis_benchmarks_specification.__builder__.schema import get_build_config
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
)
from redis_benchmarks_specification.__common__.package import PACKAGE_DIR


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
    parser = argparse.ArgumentParser(
        description="redis-benchmarks-spec builder",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--logname", type=str, default=None, help="logname to write the logs to"
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
    logging.info("Using package dir {} for inner file paths".format(PACKAGE_DIR))
    builders_folder = os.path.abspath(
        PACKAGE_DIR + "/" + args.setups_folder + "/builders"
    )
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

    logging.info("checking build spec requirements")
    already_checked_images = []
    for build_spec in different_build_specs:
        build_config, id = get_build_config(builders_folder + "/" + build_spec)
        if build_config["kind"] == "docker":
            build_image = build_config["build_image"]
            if build_image not in already_checked_images:
                logging.info(
                    "Build {} requirement: checking build image {} is available.".format(
                        id, build_image
                    )
                )
                import docker

                client = docker.from_env()
                image = client.images.pull(build_image)
                logging.info(
                    "Build {} requirement: build image {} is available with id: {}.".format(
                        id, build_image, image.id
                    )
                )
                already_checked_images.append(build_image)
            else:
                logging.info(
                    "Build {} requirement: build image {} availability was already checked.".format(
                        id, build_image
                    )
                )

    try:
        conn.xgroup_create(
            STREAM_KEYNAME_GH_EVENTS_COMMIT,
            STREAM_GH_EVENTS_COMMIT_BUILDERS_CG,
            mkstream=True,
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
    previous_id = None
    while True:
        if previous_id is None:
            previous_id = args.consumer_start_id
        logging.info("Entering blocking read waiting for work.")
        newTestInfo = conn.xreadgroup(
            STREAM_GH_EVENTS_COMMIT_BUILDERS_CG,
            "{}-proc#{}".format(STREAM_GH_EVENTS_COMMIT_BUILDERS_CG, "1"),
            {STREAM_KEYNAME_GH_EVENTS_COMMIT: previous_id},
            count=1,
            block=0,
        )
        if len(newTestInfo[0]) < 2 or len(newTestInfo[0][1]) < 1:
            previous_id = ">"
            continue
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
            buffer = testDetails[b"zip_archive"]

            for build_spec in different_build_specs:
                build_config, id = get_build_config(builders_folder + "/" + build_spec)
                build_image = build_config["build_image"]
                run_image = build_image
                if "run_image" in build_config:
                    run_image = build_config["run_image"]
                compiler = build_config["compiler"]
                cpp_compiler = build_config["cpp_compiler"]
                build_os = build_config["os"]
                build_arch = build_config["arch"]

                build_artifacts = ["redis-server"]
                if "build_artifacts" in build_config:
                    build_artifacts = build_config["build_artifacts"]
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
                redis_temporary_dir = temporary_dir + "/" + redis_dir + "/"
                logging.info("Using redis temporary dir {}".format(redis_temporary_dir))
                build_command = 'bash -c "make Makefile.dep {} && cd ./deps && CXX={} CC={} make {} {} -j && cd .. && CXX={} CC={} make {} {} -j"'.format(
                    build_vars_str,
                    cpp_compiler,
                    compiler,
                    " ".join(
                        [
                            "hdr_histogram",
                            "hiredis",
                            "jemalloc",
                            "linenoise",
                            "lua",
                        ]
                    ),
                    build_vars_str,
                    cpp_compiler,
                    compiler,
                    "redis-server",
                    build_vars_str,
                )
                logging.info(
                    "Using the following build command {}".format(build_command)
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
                build_stream_fields = {
                    "id": id,
                    "git_hash": git_hash,
                    "build_image": build_image,
                    "run_image": run_image,
                    "compiler": compiler,
                    "cpp_compiler": cpp_compiler,
                    "os": build_os,
                    "arch": build_arch,
                    "build_vars": build_vars_str,
                    "build_command": build_command,
                    "build_artifacts": ",".join(build_artifacts),
                }
                for artifact in build_artifacts:
                    bin_artifact = open(
                        "{}src/{}".format(redis_temporary_dir, artifact), "rb"
                    ).read()
                    build_stream_fields[artifact] = bytes(bin_artifact)
                    build_stream_fields["{}_len_bytes".format(artifact)] = len(
                        bytes(bin_artifact)
                    )
                result = True
                if result is True:
                    stream_id = conn.xadd(
                        STREAM_KEYNAME_NEW_BUILD_EVENTS, build_stream_fields
                    )
                    logging.info(
                        "sucessfully built build variant {} for redis git_sha {}. Stream id: {}".format(
                            id, git_hash, stream_id
                        )
                    )
                shutil.rmtree(temporary_dir, ignore_errors=True)

        else:
            logging.error("Missing commit information within received message.")
            continue
