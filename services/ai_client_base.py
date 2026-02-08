from abc import ABC, abstractmethod

class BaseAIClient(ABC):
    @abstractmethod
    async def send(self, prompt: str) -> str:
        pass
