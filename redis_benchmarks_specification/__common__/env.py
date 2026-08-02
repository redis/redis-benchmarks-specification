import logging
import os

# default specification paths
import psutil

try:
    # Use importlib.resources for Python 3.9+ (preferred)
    from importlib.resources import files

    SPECS_PATH_SETUPS_DEFAULT_PATH = str(
        files("redis_benchmarks_specification") / "setups"
    )
    SPECS_PATH_TEST_SUITES_DEFAULT_PATH = str(
        files("redis_benchmarks_specification") / "test-suites"
    )
except ImportError:
    try:
        # Fallback to pkg_resources for older environments
        import pkg_resources

        SPECS_PATH_SETUPS_DEFAULT_PATH = pkg_resources.resource_filename(
            "redis_benchmarks_specification", "setups"
        )
        SPECS_PATH_TEST_SUITES_DEFAULT_PATH = pkg_resources.resource_filename(
            "redis_benchmarks_specification", "test-suites"
        )
    except ImportError:
        # Final fallback - construct paths manually
        import redis_benchmarks_specification
        import pathlib

        base_path = pathlib.Path(redis_benchmarks_specification.__file__).parent
        SPECS_PATH_SETUPS_DEFAULT_PATH = str(base_path / "setups")
        SPECS_PATH_TEST_SUITES_DEFAULT_PATH = str(base_path / "test-suites")

SPECS_PATH_SETUPS = os.getenv("SPECS_PATH_SETUPS", SPECS_PATH_SETUPS_DEFAULT_PATH)
SPECS_PATH_TEST_SUITES = os.getenv(
    "SPECS_PATH_TEST_SUITES", SPECS_PATH_TEST_SUITES_DEFAULT_PATH
)

# event stream from github
STREAM_KEYNAME_GH_EVENTS_COMMIT = os.getenv(
    "STREAM_KEYNAME_GH_EVENTS_COMMIT", "oss:api:gh/redis/redis/commits"
)

STREAM_GH_EVENTS_COMMIT_BUILDERS_CG = os.getenv(
    "STREAM_GH_EVENTS_COMMIT_BUILDERS_CG", "builders-cg:redis/redis/commits"
)

# build events stream. This is the stream read by the coordinators to kickoff benchmark variations
STREAM_KEYNAME_NEW_BUILD_EVENTS = os.getenv(
    "STREAM_KEYNAME_NEW_BUILD_EVENTS", "oss:api:gh/redis/redis/builds"
)


# Function to get architecture-specific build events stream name
def get_arch_specific_stream_name(arch):
    """Get architecture-specific stream name for build events"""
    base_stream = STREAM_KEYNAME_NEW_BUILD_EVENTS
    if arch in ["amd64", "x86_64"]:
        return f"{base_stream}:amd64"
    elif arch in ["arm64", "aarch64"]:
        return f"{base_stream}:arm64"
    else:
        # Fallback to base stream for unknown architectures
        return base_stream


STREAM_GH_NEW_BUILD_RUNNERS_CG = os.getenv(
    "STREAM_GH_NEW_BUILD_RUNNERS_CG", "runners-cg:redis/redis/commits"
)

# host used to store the streams of events
GH_TOKEN = os.getenv("GH_TOKEN", None)
GH_REDIS_SERVER_HOST = os.getenv("GH_REDIS_SERVER_HOST", "localhost")
GH_REDIS_SERVER_PORT = int(os.getenv("GH_REDIS_SERVER_PORT", "6379"))
GH_REDIS_SERVER_AUTH = os.getenv("GH_REDIS_SERVER_AUTH", None)
GH_REDIS_SERVER_USER = os.getenv("GH_REDIS_SERVER_USER", None)

# DB used to authenticate ( read-only/non-dangerous access only )
REDIS_AUTH_SERVER_HOST = os.getenv("REDIS_AUTH_SERVER_HOST", "localhost")
REDIS_AUTH_SERVER_PORT = int(os.getenv("REDIS_AUTH_SERVER_PORT", "6379"))
REDIS_HEALTH_CHECK_INTERVAL = int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "15"))
REDIS_SOCKET_TIMEOUT = int(os.getenv("REDIS_SOCKET_TIMEOUT", "300"))
REDIS_BINS_EXPIRE_SECS = int(
    os.getenv("REDIS_BINS_EXPIRE_SECS", "{}".format(24 * 7 * 60 * 60))
)

TRUE_STRINGS = ("1", "true", "t", "yes", "y", "on")
FALSE_STRINGS = ("0", "false", "f", "no", "n", "off", "")


def parse_bool(value, default=False):
    """Decode a boolean that has been through a string round-trip.

    Booleans reach us as strings: written to the commits/builds streams with str(), and read from
    the environment. A bare bool() on that is wrong in the one direction that matters -- every
    non-empty string is truthy, so "False", "0" and "no" all decode to True and the flag is stuck
    on. bytes are accepted because stream reads are not always decoded first.

    An unrecognised string returns `default` rather than guessing, since guessing is what bool()
    already does. Callers that need to distinguish "absent" from "unparseable" should check
    membership before calling.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode()
        except UnicodeDecodeError:
            return default
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in TRUE_STRINGS:
            return True
        if normalized in FALSE_STRINGS:
            return False
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def parse_bool_arg(value):
    """argparse `type=` for a boolean flag.

    Same accepted spellings as parse_bool, but unrecognised input raises so argparse turns it into
    a usage error instead of silently substituting a default -- on a command line, a typo should be
    reported, not guessed at. The empty string is rejected for the same reason.

    Accepts exactly the spellings distutils.util.strtobool did, which this replaces: distutils was
    removed from the standard library in 3.12 and only resolves today via a setuptools shim, so
    importing it made the CLI fail at import time in any environment without setuptools.
    """
    normalized = str(value).strip().lower()
    if normalized in TRUE_STRINGS:
        return True
    if normalized and normalized in FALSE_STRINGS:
        return False
    raise ValueError(
        "invalid boolean {!r}; expected one of {}".format(
            value, ", ".join(TRUE_STRINGS + tuple(f for f in FALSE_STRINGS if f))
        )
    )


# environment variables
PULL_REQUEST_TRIGGER_LABEL = os.getenv(
    "PULL_REQUEST_TRIGGER_LABEL", "action:run-benchmark"
)
# parse_bool, not bool(): this is the default of a --datasink_push_results_redistimeseries
# store_true flag, so a stray truthy read cannot be overridden on the command line -- setting
# DATASINK_PUSH_RTS=0 would enable pushing forever. Note the sibling PUSH_RTS in __compare__/args
# already reads 0 correctly, via int().
DATASINK_RTS_PUSH = parse_bool(os.getenv("DATASINK_PUSH_RTS"), default=False)
DATASINK_RTS_AUTH = os.getenv("DATASINK_RTS_AUTH", None)
DATASINK_RTS_USER = os.getenv("DATASINK_RTS_USER", None)
DATASINK_RTS_HOST = os.getenv("DATASINK_RTS_HOST", "localhost")
DATASINK_RTS_PORT = int(os.getenv("DATASINK_RTS_PORT", "6379"))
ALLOWED_PROFILERS = "perf:record,vtune"
PROFILERS_DEFAULT = "perf:record"
PROFILE_FREQ_DEFAULT = "99"
PROFILERS_DSO = os.getenv("PROFILERS_DSO", None)
PROFILERS_ENABLED = bool(int(os.getenv("PROFILE", 0)))
PROFILERS = os.getenv("PROFILERS", PROFILERS_DEFAULT)
MAX_PROFILERS_PER_TYPE = int(os.getenv("MAX_PROFILERS", 1))
PROFILE_FREQ = os.getenv("PROFILE_FREQ", PROFILE_FREQ_DEFAULT)
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "redis.benchmarks.spec")
# logging related
VERBOSE = os.getenv("VERBOSE", "1") == "0"
LOG_FORMAT = "%(asctime)s %(levelname)-4s %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
LOG_LEVEL = logging.INFO
if VERBOSE:
    LOG_LEVEL = logging.WARN

MACHINE_CPU_COUNT = psutil.cpu_count()
MACHINE_NAME = os.uname()[1]

# Webhook push filtering — comma-separated allowlists
BENCHMARK_TRIGGER_BRANCHES = os.getenv("BENCHMARK_TRIGGER_BRANCHES", "unstable")
BENCHMARK_TRIGGER_ORGS = os.getenv("BENCHMARK_TRIGGER_ORGS", "redis")

# Webhook PR diff-driven scoping. When a PR is labeled with the trigger label, derive
# which command groups its diff touches and scope the run instead of executing the full
# suite. Set BENCHMARK_PR_DIFF_SCOPING to a falsey value (0/false/no/off) to restore
# full-suite-on-label.
BENCHMARK_PR_DIFF_SCOPING = os.getenv(
    "BENCHMARK_PR_DIFF_SCOPING", "1"
).strip().lower() in ("1", "true", "yes", "on")
# PRs changing more than this many files are treated as inherently broad -> full suite
# (also bounds the synchronous GitHub pagination done inside the webhook request).
try:
    BENCHMARK_PR_MAX_FILES = int(os.getenv("BENCHMARK_PR_MAX_FILES", "100"))
    if BENCHMARK_PR_MAX_FILES <= 0:
        raise ValueError("must be > 0")
except ValueError:
    logging.warning(
        "invalid BENCHMARK_PR_MAX_FILES (must be a positive int); using 100"
    )
    BENCHMARK_PR_MAX_FILES = 100
# Diff-scoping needs to read PR files from the GitHub API. Without a token it runs
# unauthenticated (60 req/hr) and will usually rate-limit -> silent full-suite fallback.
if BENCHMARK_PR_DIFF_SCOPING and not GH_TOKEN:
    logging.warning(
        "BENCHMARK_PR_DIFF_SCOPING is on but GH_TOKEN is unset; PR diff lookups will hit "
        "the 60/hr unauthenticated GitHub limit and degrade to full-suite runs"
    )
