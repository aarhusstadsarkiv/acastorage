# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from pathlib import Path
from typing import Any

from azure.storage.blob.aio import ContainerClient

# -----------------------------------------------------------------------------
# Main ACAStorage Class
# -----------------------------------------------------------------------------


class ACAStorage(ContainerClient):
    def __init__(
        self,
        container: str,
        credential: Any,
    ) -> None:
        """
        Azure Blob Storage Backend
        """
        super().__init__(
            "https://acastorage.blob.core.windows.net/",
            container,
            credential=credential,
        )
        # TODO: Implement exists() check when MS adds it, cf.
        # https://github.com/Azure/azure-sdk-for-python/pull/16315

    async def upload(self, source: Path, dest: Path) -> None:
        if source.is_dir():
            pass
        elif source.is_file():
            await self.upload_file(source, dest)

    async def upload_file(self, source: Path, dest: Path) -> None:
        if not source.is_file():
            raise ValueError(f"Source {source} must be a file.")

        with source.open("rb") as data:
            await self.upload_blob(name=str(dest), data=data)
