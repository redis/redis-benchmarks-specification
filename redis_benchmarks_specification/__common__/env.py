import argparse
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

_TRUE_STRINGS = ("1", "true", "t", "yes", "y", "on")
_FALSE_STRINGS = ("0", "false", "f", "no", "n", "off")


def accepted_bool_spellings():
    """The spellings parse_bool accepts, for error and warning messages."""
    return _TRUE_STRINGS + _FALSE_STRINGS


def parse_bool(value, default=...):
    """Decode a boolean that has been through a string round-trip.

    Booleans reach us as strings: written to the commits and builds streams with str(), and read
    from the environment. A bare bool() on that is wrong in the one direction that matters -- every
    non-empty string is truthy, so "False", "0" and "no" all decode to True and the flag is stuck
    on. bytes are accepted because stream reads are not always decoded first.

    Ellipsis marks "no default given". A module-level object() sentinel would not survive an
    importlib.reload: the signature default is captured at definition time while the identity test
    reads the module global, so after a reload the two stop matching and the strict form silently
    starts returning the sentinel instead of raising. Ellipsis is a builtin singleton, so it cannot
    drift.

    With no `default`, an unrecognised value raises -- which is why this is the correct callable for
    argparse's `type=`, where a typo should be a usage error rather than a guess. Pass an explicit
    `default` to get the value back instead, with a warning; that form is for values that arrive
    from a stream or the environment, where raising would abort a consumer loop or an import.

    Accepts the spellings distutils.util.strtobool did, plus surrounding whitespace. distutils was
    removed from the standard library in 3.12 and resolves only via a setuptools shim.

    Deliberately not accepting numbers: parse_bool(2) would have to be True while
    parse_bool(str(2)) is not, which contradicts the round-trip property this exists to provide.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode()
        except UnicodeDecodeError:
            # Left as-is rather than blanked to None: blanking erased the offending bytes from the
            # message, making an undecodable field indistinguishable from an absent one.
            pass
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _TRUE_STRINGS:
            return True
        if normalized in _FALSE_STRINGS:
            return False
        if not normalized:
            # Empty or whitespace-only. On the command line this is what an unset shell variable
            # expands to -- `--verbose "$VERBOSE"` -- so the strict form returns False rather than
            # turning a common accident into a usage error.
            #
            # This is uniform across the nine converted flags, but it was not uniform before: the
            # eight that used type=bool already read "" as False, while --use-git-timestamp used
            # strtobool, which raised. So that one flag moves from a usage error to False. It is
            # unified deliberately -- one rule beats two -- and it is moot in practice, because the
            # value is discarded downstream regardless (see issue #464).
            #
            # For a stream or environment value, empty means "not supplied" and takes the caller's
            # default; several of those flags default True and must not be flipped off by an absent
            # field.
            return False if default is ... else default
        value = value.strip()

    expected = ", ".join(accepted_bool_spellings())
    if default is ...:
        # ArgumentTypeError, not ValueError: argparse reproduces this message verbatim, whereas a
        # ValueError is replaced with "invalid parse_bool value: ..." -- which names an internal
        # function and drops the list of spellings the user needs.
        raise argparse.ArgumentTypeError(
            "invalid boolean {!r}; expected one of {}".format(value, expected)
        )
    # Warn when something was supplied and not understood -- the previous bool() read every
    # non-empty string as True, so an unrecognised truthy-looking value like "2" silently flips to
    # this default, and where the flag gates recording benchmark results that turns a
    # misconfiguration into missing data rather than an error.
    #
    # Silent for None only, which means nothing was supplied. That is the normal case for an unset
    # environment variable and warning about it would put noise in every run. Undecodable bytes are
    # deliberately NOT folded into None: they are a supplied value that could not be read, and the
    # reader needs to see which one.
    if value is not None:
        logging.getLogger(__name__).warning(
            "Unrecognised boolean %r; expected one of %s. Using %r.",
            value,
            expected,
            default,
        )
    return default


# environment variables
PULL_REQUEST_TRIGGER_LABEL = os.getenv(
    "PULL_REQUEST_TRIGGER_LABEL", "action:run-benchmark"
)
# parse_bool, not bool(): this is the default of a --datasink_push_results_redistimeseries
# store_true flag, so a stray truthy read cannot be overridden on the command line -- setting
# DATASINK_PUSH_RTS=0 would enable pushing forever. Note the sibling PUSH_RTS in __compare__/args
# already reads 0 correctly, though via int(), which raises on a word -- see issue #465.
_DATASINK_PUSH_RTS_RAW = os.getenv("DATASINK_PUSH_RTS")
# The default deliberately reproduces the old bool() reading -- any non-empty string is True -- so
# an unrecognised value cannot flip this off. When the flag is False the datasink connection is
# None and nothing is exported, so a wrong answer in that direction means benchmark results stop
# being recorded with nothing to indicate why. Only recognised falsy spellings change meaning.
DATASINK_RTS_PUSH = parse_bool(
    _DATASINK_PUSH_RTS_RAW, default=bool(_DATASINK_PUSH_RTS_RAW)
)
DATASINK_RTS_AUTH = os.getenv("DATASINK_RTS_AUTH", None)
DATASINK_RTS_USER = os.getenv("DATASINK_RTS_USER", None)
DATASINK_RTS_HOST = os.getenv("DATASINK_RTS_HOST", "localhost")
DATASINK_RTS_PORT = int(os.getenv("DATASINK_RTS_PORT", "6379"))
ALLOWED_PROFILERS = "perf:record,vtune"
PROFILERS_DEFAULT = "perf:record"
PROFILE_FREQ_DEFAULT = "99"
PROFILERS_DSO = os.getenv("PROFILERS_DSO", None)
# parse_bool, not bool(int(...)): the latter raises ValueError at import time on PROFILE=false or
# off, which aborts every entrypoint. The default preserves the old reading for anything parse_bool
# does not recognise -- bool(int(...)) treated EVERY non-zero integer as enabled, so narrowing to
# the literal "1" would flip PROFILE=2 from on to off and silently stop profiling artifacts being
# collected. Same policy as DATASINK_PUSH_RTS below: only recognised falsy spellings may change
# meaning.
_PROFILE_RAW = os.getenv("PROFILE")


def _profile_was_enabled(raw):
    """The pre-existing bool(int(raw)) reading, for values parse_bool does not recognise."""
    try:
        return bool(int(str(raw).strip()))
    except (TypeError, ValueError):
        return False


PROFILERS_ENABLED = parse_bool(_PROFILE_RAW, default=_profile_was_enabled(_PROFILE_RAW))
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
# Was a hand-rolled copy of parse_bool accepting a different set (no "t", no "y"). Kept at
# default=False so an unrecognised value still disables scoping as before, rather than
# enabling it.
BENCHMARK_PR_DIFF_SCOPING = parse_bool(
    os.getenv("BENCHMARK_PR_DIFF_SCOPING", "1"), default=False
)
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
