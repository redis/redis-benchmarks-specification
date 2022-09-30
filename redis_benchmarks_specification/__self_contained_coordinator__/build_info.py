import json
import logging


def extract_build_info_from_streamdata(testDetails):
    use_git_timestamp = False
    git_timestamp_ms = None
    git_version = None
    git_branch = None
    metadata = None
    build_variant_name = None
    fields = [fieldname.decode() for fieldname in testDetails.keys()]
    logging.info("Fields on stream {}".format(fields))
    git_hash = testDetails[b"git_hash"]
    if b"use_git_timestamp" in testDetails:
        use_git_timestamp = bool(testDetails[b"use_git_timestamp"].decode())
    if b"git_timestamp_ms" in testDetails:
        git_timestamp_ms = int(testDetails[b"git_timestamp_ms"].decode())
    if b"id" in testDetails:
        build_variant_name = testDetails[b"id"]
        if type(build_variant_name) == bytes:
            build_variant_name = build_variant_name.decode()
    if b"git_branch" in testDetails:
        git_branch = testDetails[b"git_branch"]
        if type(git_branch) == bytes:
            git_branch = git_branch.decode()
    if b"git_version" in testDetails:
        git_version = testDetails[b"git_version"]
        if type(git_version) == bytes:
            git_version = git_version.decode()
    if type(git_hash) == bytes:
        git_hash = git_hash.decode()
    logging.info("Received commit hash specifier {}.".format(git_hash))
    build_artifacts_str = "redis-server"
    build_image = testDetails[b"build_image"].decode()
    run_image = build_image
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
    )
