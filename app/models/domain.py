from __future__ import annotations
from dataclasses import dataclass
from typing import Literal

EdgeKind = Literal['trade','transfer']

@dataclass(frozen=True)
class Node:
    venue: str
    asset: str
    network: str | None = None

    @property
    def key(self) -> str:
        suffix = f':{self.network}' if self.network else ''
        return f'{self.venue}:{self.asset}{suffix}'

@dataclass
class Edge:
    src: Node
    dst: Node
    kind: EdgeKind
    rate: float
    fee_rate: float = 0.0
    fixed_fee: float = 0.0
    slippage_rate: float = 0.0
    capacity: float = float('inf')
    eta_seconds: int = 0
    executable: bool = True
    meta: dict | None = None

    def apply(self, amount: float) -> float:
        if not self.executable or amount <= 0 or amount > self.capacity:
            return 0.0
        gross = amount * self.rate
        variable_fee = gross * self.fee_rate
        slip = gross * self.slippage_rate
        return max(0.0, gross - variable_fee - slip - self.fixed_fee)
