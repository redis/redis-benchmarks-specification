import logging
import os
import pathlib


def get_benchmark_specs(testsuites_folder, test):
    if test == "":
        files = pathlib.Path(testsuites_folder).glob("*.yml")
        files = [str(x) for x in files]
        logging.info(
            "Running all specified benchmarks: {}".format(
                " ".join([str(x) for x in files])
            )
        )
    else:
        files = test.split(",")
        files = ["{}/{}".format(testsuites_folder, x) for x in files]
        logging.info("Running specific benchmark in file: {}".format(files))
    return files


def extract_testsuites(args):
    testsuites_folder = os.path.abspath(args.test_suites_folder)
    logging.info("Using test-suites folder dir {}".format(testsuites_folder))
    testsuite_spec_files = get_benchmark_specs(testsuites_folder, args.test)
    logging.info(
        "There are a total of {} test-suites in folder {}".format(
            len(testsuite_spec_files), testsuites_folder
        )
    )
    return testsuite_spec_files
