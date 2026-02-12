#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs
#  All rights reserved.
#

# This attribute is the only one place that the version number is written down,
# so there is only one place to change it when the version number changes.
try:
    # Use importlib.metadata for Python 3.8+ (preferred)
    from importlib.metadata import version, PackageNotFoundError

    PKG_NAME = "redis-benchmarks-specification"
    __version__ = version(PKG_NAME)
except (ImportError, PackageNotFoundError):
    try:
        # Fallback to pkg_resources for older environments
        import pkg_resources

        PKG_NAME = "redis-benchmarks-specification"
        __version__ = pkg_resources.get_distribution(PKG_NAME).version
    except (ImportError, pkg_resources.DistributionNotFound, AttributeError):
        __version__ = "99.99.99"  # like redis
