# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
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
