"""Runner-owned admission policy, independent of trigger selectors."""

import logging

import yaml


def filter_specs_by_runner_capabilities(test_files, capabilities=""):
    available = set(capabilities.split(",")) if capabilities else set()
    available = {value.strip() for value in available if value.strip()}
    admitted = []
    for test_file in test_files:
        with open(test_file) as stream:
            spec = yaml.safe_load(stream) or {}
        required = spec.get("required-runner-capabilities", [])
        if not isinstance(required, list) or any(
            not isinstance(value, str) or not value.strip() for value in required
        ):
            raise ValueError(
                f"Invalid required-runner-capabilities in {test_file}: expected a list of names"
            )
        missing = set(required) - available
        if missing:
            logging.info(
                "Skipping %s: runner lacks required capabilities %s",
                spec.get("name", test_file),
                ", ".join(sorted(missing)),
            )
        else:
            admitted.append(test_file)
    return admitted
