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
        # Init files
        test_file: Path = temp_dir / "test.txt"
        test_file.write_text("Testing")
        test_storage = ACAStorage("test", credential=self.cred)

        # Should work w/o exceptions
        await test_storage.upload_file(test_file, Path("test"))
        await delete(test_storage)

    async def test_overwrite(self, temp_dir):
        # Init
        test_file: Path = temp_dir / "test.txt"
        test_file.write_text("Testing")
        test_storage = ACAStorage("test", credential=self.cred)
        await test_storage.upload_file(test_file, Path("test"))

        # Overwrite False -> UploadError
        err_match = "The specified blob already exists."
        with pytest.raises(UploadError, match=err_match):
            await test_storage.upload_file(
                test_file, Path("test"), overwrite=False
            )

        # Overwrite True -> No error
        await test_storage.upload_file(test_file, Path("test"), overwrite=True)
        await delete(test_storage)

    async def test_file_not_found(self, temp_dir):
        # Init - note that file is not created
        test_file: Path = temp_dir / "test_not_found.txt"
        test_storage = ACAStorage("test", credential=self.cred)

        # Should raise FileNotFoundError
        err_match = f"Source {test_file} is not a file"
        with pytest.raises(FileNotFoundError, match=err_match):
            await test_storage.upload_file(test_file, Path("test"))

    async def test_upload_error(self, temp_dir, monkeypatch):
        # Monkeypatch ContainerClient
        err_msg = "fail"

        async def upload_error(*args, **kwargs):
            raise Exception(err_msg)

        monkeypatch.setattr(ContainerClient, "upload_blob", upload_error)

        # Init files
        test_file: Path = temp_dir / "test.txt"
        test_file.write_text("Testing")
        test_storage = ACAStorage("test", credential=self.cred)

        # Generic exception should reraise as UploadError
        err_match = f"Upload of {test_file} failed: {err_msg}"
        with pytest.raises(UploadError, match=err_match):
            await test_storage.upload_file(test_file, Path("test"))
