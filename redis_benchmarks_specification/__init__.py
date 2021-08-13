#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs
#  All rights reserved.
#

# This attribute is the only one place that the version number is written down,
# so there is only one place to change it when the version number changes.
import pkg_resources

PKG_NAME = "redis-benchmarks-specification"
try:
    __version__ = pkg_resources.get_distribution(PKG_NAME).version
except (pkg_resources.DistributionNotFound, AttributeError):
    __version__ = "99.99.99"  # like redis
