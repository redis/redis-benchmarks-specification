import logging
import os


def restore_build_artifacts_from_test_details(
    build_artifacts, conn, temporary_dir, testDetails
):
    for build_artifact in build_artifacts:
        buffer_key = testDetails["{}".format(build_artifact).encode()]
        logging.info(
            "Reading artifact binary {} from key {}".format(build_artifact, buffer_key)
        )
        buffer = bytes(conn.get(buffer_key))
        artifact_fname = "{}/{}".format(temporary_dir, build_artifact)
        with open(artifact_fname, "wb") as fd:
            fd.write(buffer)
            os.chmod(artifact_fname, 755)
        # TODO: re-enable
        # if build_artifact == "redis-server":
        #     redis_server_path = artifact_fname

        logging.info(
            "Successfully restored {} into {}".format(build_artifact, artifact_fname)
        )
