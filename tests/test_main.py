# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from pathlib import Path
from typing import Any

import pytest
from azure.identity.aio import EnvironmentCredential
from azure.keyvault.secrets.aio import SecretClient
from azure.storage.blob.aio import BlobClient

from acastorage import ACAStorage
from acastorage.exceptions import UploadError


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------
pytestmark = pytest.mark.asyncio


async def delete(storage: ACAStorage) -> None:
    await storage.delete_blobs(*[b async for b in storage.list_blobs()])


@pytest.fixture
async def cred() -> Any:
    credential = EnvironmentCredential()
    vault = SecretClient(
        vault_url="https://aca-keys.vault.azure.net", credential=credential
    )
    yield await vault.get_secret("acastorage-key1")
    await credential.close()
    await vault.close()


class TestACAStorage:
    async def test_init(self, cred):
        assert ACAStorage("test", credential=cred.value)

    async def test_upload_file(self, temp_dir, cred):
        # Init files
        test_file: Path = temp_dir / "test.txt"
        test_file.write_text("Testing")
        test_storage = ACAStorage("test", credential=cred.value)

        # Should work w/o exceptions
        await test_storage.upload_file(test_file, Path("test"))
        await delete(test_storage)

    async def test_overwrite(self, temp_dir, cred):
        # Init
        test_file: Path = temp_dir / "test.txt"
        test_file.write_text("Testing")
        test_storage = ACAStorage("test", credential=cred.value)
        await test_storage.upload_file(test_file, Path("test"))

        # Overwrite True -> No error
        await test_storage.upload_file(test_file, Path("test"), overwrite=True)

        # Overwrite False -> UploadError
        err_match = "The specified blob already exists."
        with pytest.raises(UploadError, match=err_match):
            await test_storage.upload_file(
                test_file, Path("test"), overwrite=False
            )

        await delete(test_storage)

    async def test_file_not_found(self, temp_dir, cred):
        # Init - note that file is not created
        test_file: Path = temp_dir / "test_not_found.txt"
        test_storage = ACAStorage("test", credential=cred.value)

        # Should raise FileNotFoundError
        err_match = f"Source {test_file} is not a file"
        with pytest.raises(FileNotFoundError, match=err_match):
            await test_storage.upload_file(test_file, Path("test"))

    async def test_upload_error(self, temp_dir, monkeypatch, cred):
        # Monkeypatch ContainerClient
        err_msg = "fail"

        def upload_error(*args, **kwargs):
            raise Exception(err_msg)

        monkeypatch.setattr(BlobClient, "upload_blob", upload_error)

        # Init files
        test_file: Path = temp_dir / "test.txt"
        test_file.write_text("Testing")
        test_storage = ACAStorage("test", credential=cred.value)

        # Generic exception should reraise as UploadError
        err_match = f"Upload of {test_file} failed: {err_msg}"
        with pytest.raises(UploadError, match=err_match):
            await test_storage.upload_file(test_file, Path("test"))
