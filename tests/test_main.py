# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import os

import pytest

from acastorage import ACAStorage

# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------
pytestmark = pytest.mark.asyncio


class TestACAStorage:
    cred = os.getenv("ACASTORAGE_KEY")

    async def test_init(self):
        assert ACAStorage("test", credential=self.cred)
