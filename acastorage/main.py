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

    async def upload_file(
        self, source: Path, dest_dir: Path, overwrite: bool = False
    ) -> None:
        """Upload source file to a specified destination. The destination
        is always assumed to be a directory.

        Parameters
        ----------
        source : pathlib.Path
            The source file to upload.
        dest_dir: pathlib.Path
            The destination folder to upload to.
        overwrite: bool, optional
            Whether to overwrite the target file if it exists.
            Defaults to False.

        Raises
        ------
        FileNotFoundError
            If the source is not a file.
        UploadError
            If upload of the file fails. Reraises exceptions from
            Azure's ContainerClient in a more user-friendly format.
        """
        if not source.is_file():
            raise FileNotFoundError(f"Source {source} is not a file.")

        with source.open("rb") as data:
            upload_dest = dest_dir / source.name
            try:
                await self.upload_blob(
                    name=str(upload_dest), data=data, overwrite=overwrite
                )
            except Exception as err:
                raise UploadError(f"Upload of {source} failed: {err}")
