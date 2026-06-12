# Contributing

We treat this repo as "Open Source" within Redis: anyone who clears the bar below is welcome to contribute.

## Local setup

```bash
git clone git@github.com:redis/redis-benchmarks-specification.git
cd redis-benchmarks-specification
pip install poetry
poetry install
```

## Branch naming

```
<type>/<short-description>
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`

Example: `feat/add-new-command-spec`

## Coding standards

- Keep changes focused; one logical change per PR.
- Follow the conventions already present in the codebase (formatting, naming, error handling).
- No dead code, no commented-out blocks.
- Run `flake8` before opening a PR.

## Submitting changes

1. Fork or create a branch from `main`.
2. Make your changes with clear, atomic commits.
3. Open a pull request against `main` with a descriptive title and summary.
4. Address review comments promptly; force-push to the same branch to update.

## Testing

- All new behaviour must be covered by tests.
- Existing tests must pass: run the test suite locally before opening a PR.
- Coverage should not decrease.

```bash
poetry run tox -e integration-tests
```

## Review process

- At least one maintainer approval is required before merge.
- CI must be green.
- Maintainers may request changes or close PRs that don't meet the bar — this is normal and not personal.
