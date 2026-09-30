from __future__ import annotations
from abc import ABC, abstractmethod
from app.models.domain import Edge

class ExchangeConnector(ABC):
    name: str

    @abstractmethod
    async def get_edges(self, notional_usd: float) -> list[Edge]:
        raise NotImplementedError
