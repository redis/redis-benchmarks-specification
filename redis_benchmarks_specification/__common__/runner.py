import logging
import os
import pathlib
import re


def get_benchmark_specs(testsuites_folder, test="", test_regex=".*"):
    final_files = []
    if test == "":
        files = pathlib.Path(testsuites_folder).glob("*.yml")
        original_files = [str(x) for x in files]
        if test_regex == ".*":
            logging.info(
                "Acception all test files. If you need further filter specify a regular expression via --tests-regexp"
            )
            "Running all specified benchmarks: {}".format(" ".join(original_files))
            final_files = original_files
        else:
            logging.info(
                "Filtering all test names via a regular expression: {}".format(
                    test_regex
                )
            )
            test_regexp_string = re.compile(test_regex)
            for test_name in original_files:
                match_obj = re.search(test_regexp_string, test_name)
                if match_obj is None:
                    logging.info(
                        "Skipping test file: {} given it does not match regex {}".format(
                            test_name, test_regexp_string
                        )
                    )
                else:
                    final_files.append(test_name)

    else:
        files = test.split(",")
        final_files = ["{}/{}".format(testsuites_folder, x) for x in files]
        logging.info(
            "Running specific benchmark in {} files: {}".format(
                len(final_files), final_files
            )
        )
    return final_files


def extract_testsuites(args):
    testsuites_folder = os.path.abspath(args.test_suites_folder)
    logging.info("Using test-suites folder dir {}".format(testsuites_folder))
    testsuite_spec_files = get_benchmark_specs(
        testsuites_folder, args.test, args.tests_regexp
    )
    logging.info(
        "There are a total of {} test-suites in folder {}".format(
            len(testsuite_spec_files), testsuites_folder
        )
    )
    return testsuite_spec_files
