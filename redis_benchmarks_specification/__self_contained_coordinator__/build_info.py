import json
import logging

from redis_benchmarks_specification.__common__.builder_schema import (
    get_branch_version_from_test_details,
)


def extract_build_info_from_streamdata(testDetails):
    arch = "amd64"
    use_git_timestamp = False
    git_timestamp_ms = None
    metadata = {}
    build_variant_name = None
    fields = [fieldname.decode() for fieldname in testDetails.keys()]
    logging.info("Fields on stream {}".format(testDetails))
    git_hash = None
    if b"git_hash" in testDetails:
        git_hash = testDetails[b"git_hash"].decode()
    if b"use_git_timestamp" in testDetails:
        use_git_timestamp = bool(testDetails[b"use_git_timestamp"].decode())
    if b"git_timestamp_ms" in testDetails:
        git_timestamp_ms = int(testDetails[b"git_timestamp_ms"].decode())
    if b"id" in testDetails:
        build_variant_name = testDetails[b"id"]
        if type(build_variant_name) == bytes:
            build_variant_name = build_variant_name.decode()
    git_branch, git_version = get_branch_version_from_test_details(testDetails)
    if type(git_hash) == bytes:
        git_hash = git_hash.decode()
    logging.info("Received commit hash specifier {}.".format(git_hash))
    build_artifacts_str = "redis-server"
    build_image = testDetails[b"build_image"].decode()
    run_image = build_image
    if b"arch" in testDetails:
        arch = testDetails[b"arch"].decode()
        logging.info("detected arch info {}.".format(arch))
    else:
        logging.info("using default arch info {}.".format(arch))
    if b"run_image" in testDetails:
        run_image = testDetails[b"run_image"].decode()
        logging.info("detected run image info {}.".format(run_image))
    else:
        logging.info("using build image info {}.".format(build_image))
    if b"build_artifacts" in testDetails:
        build_artifacts_str = testDetails[b"build_artifacts"].decode()
    build_artifacts = build_artifacts_str.split(",")
    if b"metadata" in testDetails:
        metadata = json.loads(testDetails[b"metadata"].decode())
    return (
        build_variant_name,
        metadata,
        build_artifacts,
        git_hash,
        git_branch,
        git_version,
        run_image,
        use_git_timestamp,
        git_timestamp_ms,
        arch,
    )
