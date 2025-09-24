"""
Warning suppression module that should be imported first to suppress known warnings.
"""

import warnings

# Suppress cryptography deprecation warnings from paramiko
warnings.filterwarnings("ignore", category=DeprecationWarning, module="paramiko")
warnings.filterwarnings("ignore", message=".*TripleDES.*", category=DeprecationWarning)
warnings.filterwarnings(
    "ignore", message=".*cryptography.*", category=DeprecationWarning
)

# Also suppress the specific CryptographyDeprecationWarning if it exists
try:
    from cryptography.utils import CryptographyDeprecationWarning

    warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
except ImportError:
    pass
