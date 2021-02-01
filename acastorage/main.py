# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from pathlib import Path
from typing import Any

from azure.storage.blob.aio import ContainerClient

from acastorage.exceptions import UploadError

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

    async def upload_file(self, source: Path, dest: Path = Path(".")) -> None:
        """Upload source file to a specified destination. The destination
        is always assumed to be a directory, and defaults to the container
        root.

        Parameters
        ----------
        source : pathlib.Path
            The source file to upload.
        dest: pathlib.Path, optional
            The destination folder to upload to. Defaults to pathlib.Path(".").

        Raises
        ------
        OSError
            If the source is not a file.
        """
        if not source.is_file():
            raise ValueError(f"Source {source} is not a file.")

        with source.open("rb") as data:
            upload_dest = dest / source.name
            try:
                await self.upload_blob(name=str(upload_dest), data=data)
            except Exception as err:
                raise UploadError(f"Upload of {source} failed with {err}")
