# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import os
from pathlib import Path

import pytest

from acastorage import ACAStorage

# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------
pytestmark = pytest.mark.asyncio


async def delete(storage: ACAStorage) -> None:
    await storage.delete_blobs(*[b async for b in storage.list_blobs()])


class TestACAStorage:
    cred = os.getenv("ACASTORAGE_TOKEN")

    async def test_init(self):
        assert ACAStorage("test", credential=self.cred)

    async def test_upload_file(self, temp_dir):
        test_file: Path = temp_dir / "test.txt"
        test_file.write_text("Testing")
        test_storage = ACAStorage("test", credential=self.cred)
        await test_storage.upload_file(test_file)
        await delete(test_storage)
