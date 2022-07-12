import logging
import os
import pathlib
import yaml


def get_tools(tools_folder):
    files = pathlib.Path(tools_folder).glob("*.yml")
    files = [str(x) for x in files]
    logging.info(
        "Running tools: {}".format(
            " ".join([str(x) for x in files])
        )
    )
    return files

def start_tools_if_required(tools_files):
    logging.info(
        "Running tools: {}".format(
            " ".join([str(x) for x in tools_files])
        )
    )
    for tool_file in tools_files:
        with open(tool_file) as stream:
            tool_config = yaml.safe_load(stream)
            command = tool_config["command"]
            tool_stream = os.popen(command)
            output = tool_stream.read()
            logging.info(output)
