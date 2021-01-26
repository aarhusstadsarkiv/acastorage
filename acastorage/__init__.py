# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from importlib.metadata import version  # type: ignore

from .main import ACAStorage

# -----------------------------------------------------------------------------
# Version
# -----------------------------------------------------------------------------
__version__ = version("acastorage")

# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
__all__ = ["ACAStorage"]
