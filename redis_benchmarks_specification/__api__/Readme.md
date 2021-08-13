
## Run in local development mode

Within root project folder
```
poetry run redis-benchmarks-spec-api
```

## Testing triggering a new commit spec via local api

```
curl -u <USER>:<PASS> \
    -X POST -H "Content-Type: application/json" \
    --data '{"git_hash":"0cf2df84d4b27af4bffd2bf3543838f09e10f874"}' \
    http://localhost:5000/api/gh/redis/redis/commits
```