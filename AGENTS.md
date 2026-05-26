# Agent guidelines

Instructions for AI coding agents (Claude Code, Copilot, Cursor, etc.) working in this repo.

## Project overview

`redis-benchmarks-specification` defines the cross-language/tools requirements and expectations for Redis performance benchmarking. It is a Python package (Poetry-managed) that publishes benchmark specifications — command lists, encoding coverage, groups — as structured JSON/YAML, and provides a Flask API and tooling for running and comparing benchmarks via `redisbench-admin`.

## Local setup

```bash
git clone git@github.com:redis/redis-benchmarks-specification.git
cd redis-benchmarks-specification
pip install poetry
poetry install
```

## Branch naming

Same as human contributors: `<type>/<short-description>` (e.g. `fix/command-spec-encoding`).

## Coding standards

- Match the style already in the file you are editing.
- Prefer clear, minimal changes over large refactors unless explicitly asked.
- Do not add comments that describe *what* the code does — only add comments when the *why* is non-obvious.
- Do not introduce new dependencies without checking with the maintainer.
- Run `flake8` before declaring a task complete.

## Running tests

```bash
poetry run tox -e integration-tests
```

Always run tests before declaring a task complete.

## How to submit changes

1. Create a branch: `git checkout -b <type>/<description>`.
2. Commit with a clear message focused on *why*, not *what*.
3. Open a pull request against `main`.
4. Do **not** push directly to `main`.

## What to avoid

- Do not reformat files unrelated to your change.
- Do not remove error handling or tests.
- Do not commit secrets, credentials, or large binary files.
- Do not amend published commits.
