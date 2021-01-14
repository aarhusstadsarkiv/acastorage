# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from typing import Any
from typing import Optional

from azure.storage.blob.aio import ContainerClient

# -----------------------------------------------------------------------------
# Main ACAStorage Class
# -----------------------------------------------------------------------------


class ACAStorage(ContainerClient):
    def __init__(
        self,
        container: str,
        credential: Optional[Any] = None,
        create: bool = True,
    ) -> None:
        """
        Azure Blob Storage Backend
        """
        super().__init__(
            "https://acastorage.blob.core.windows.net/",
            container,
            credential=credential,
        )
