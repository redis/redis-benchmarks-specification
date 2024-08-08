import logging
import os


def restore_build_artifacts_from_test_details(
    build_artifacts, conn, temporary_dir, testDetails
):
    for build_artifact in build_artifacts:
        build_artifact_key = "{}".format(build_artifact).encode()
        if build_artifact_key in testDetails:
            buffer_key = testDetails[build_artifact_key]
            logging.info(
                "Reading artifact binary {} from key {}".format(
                    build_artifact, buffer_key
                )
            )
            buffer = bytes(conn.get(buffer_key))
            artifact_fname = "{}/{}".format(temporary_dir, build_artifact)
            with open(artifact_fname, "wb") as fd:
                fd.write(buffer)
                os.chmod(artifact_fname, 755)

            logging.info(
                "Successfully restored {} into {}".format(
                    build_artifact, artifact_fname
                )
            )
