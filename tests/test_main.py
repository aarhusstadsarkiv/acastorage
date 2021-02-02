# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import os
from pathlib import Path

import pytest
from azure.storage.blob.aio import ContainerClient

from acastorage import ACAStorage
from acastorage.exceptions import UploadError

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
        await test_storage.upload_file(test_file, Path("test"))
        await delete(test_storage)

    async def test_file_not_found(self, temp_dir):
        test_file: Path = temp_dir / "test_not_found.txt"
        test_storage = ACAStorage("test", credential=self.cred)
        err_match = f"Source {test_file} is not a file"
        with pytest.raises(FileNotFoundError, match=err_match):
            await test_storage.upload_file(test_file, Path("test"))

    async def test_upload_error(self, temp_dir, monkeypatch):
        err_msg = "fail"

        async def upload_error(*args, **kwargs):
            raise Exception(err_msg)

        monkeypatch.setattr(ContainerClient, "upload_blob", upload_error)
        test_file: Path = temp_dir / "test.txt"
        test_file.write_text("Testing")
        test_storage = ACAStorage("test", credential=self.cred)
        err_match = f"Upload of {test_file} failed with {err_msg}"
        with pytest.raises(UploadError, match=err_match):
            await test_storage.upload_file(test_file, Path("test"))
