import logging
import os

# default specification paths
import pkg_resources
import psutil

SPECS_PATH_SETUPS_DEFAULT_PATH = pkg_resources.resource_filename(
    "redis_benchmarks_specification", "setups"
)

SPECS_PATH_TEST_SUITES_DEFAULT_PATH = pkg_resources.resource_filename(
    "redis_benchmarks_specification", "test-suites"
)

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

# environment variables
PULL_REQUEST_TRIGGER_LABEL = os.getenv(
    "PULL_REQUEST_TRIGGER_LABEL", "action:run-benchmark"
)
DATASINK_RTS_PUSH = bool(os.getenv("DATASINK_PUSH_RTS", False))
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
