from redis_benchmarks_specification import __version__

import os
import toml

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))


def populate_with_poetry_data():
    project_name = "redis-benchmarks-specification"
    project_version = __version__
    project_description = None
    try:
        poetry_data = toml.load("pyproject.toml")["tool"]["poetry"]
        project_name = poetry_data["name"]
        project_version = poetry_data["version"]
        project_description = poetry_data["description"]
    except FileNotFoundError:
        pass

    return project_name, project_description, project_version


def get_version_string(project_name, project_version):
    version_str = "{project_name} {project_version}".format(
        project_name=project_name, project_version=project_version
    )
    return version_str
