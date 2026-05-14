from abc import ABC, abstractmethod
from typing import Any

class BaseAIClient(ABC):
    @abstractmethod
    async def send(self, prompt: str, step: str = "default", **kwargs) -> Any:
        pass
