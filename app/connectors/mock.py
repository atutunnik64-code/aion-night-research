from __future__ import annotations
from app.connectors.base import ExchangeConnector
from app.models.domain import Node, Edge

class MockConnector(ExchangeConnector):
    name = 'MOCK'

    async def get_edges(self, notional_usd: float) -> list[Edge]:
        b_usdt = Node('BetaX','USDT')
        b_btc = Node('BetaX','BTC')
        b_eth = Node('BetaX','ETH')
        a_usdt = Node('AlphaX','USDT')
        a_btc = Node('AlphaX','BTC')
        a_eth = Node('AlphaX','ETH')
        edges = [
            Edge(a_usdt,a_btc,'trade',1/64000,0.001,slippage_rate=0.0004,capacity=50000,meta={'pair':'BTC/USDT'}),
            Edge(a_btc,a_eth,'trade',31.8,0.001,slippage_rate=0.0005,capacity=2,meta={'pair':'ETH/BTC'}),
            Edge(a_eth,a_usdt,'trade',2065,0.001,slippage_rate=0.0006,capacity=60,meta={'pair':'ETH/USDT'}),
            Edge(b_usdt,b_btc,'trade',1/63800,0.001,slippage_rate=0.0005,capacity=45000,meta={'pair':'BTC/USDT'}),
            Edge(b_btc,b_usdt,'trade',64550,0.001,slippage_rate=0.0005,capacity=1.5,meta={'pair':'BTC/USDT'}),
            Edge(a_btc,b_btc,'transfer',1.0,0.0,fixed_fee=0.00008,eta_seconds=600,capacity=3,meta={'network':'BTC'}),
            Edge(b_btc,a_btc,'transfer',1.0,0.0,fixed_fee=0.00008,eta_seconds=600,capacity=3,meta={'network':'BTC'}),
            Edge(a_usdt,b_usdt,'transfer',1.0,0.0,fixed_fee=1.0,eta_seconds=90,capacity=100000,meta={'network':'TRC20'}),
            Edge(b_usdt,a_usdt,'transfer',1.0,0.0,fixed_fee=1.0,eta_seconds=90,capacity=100000,meta={'network':'TRC20'}),
        ]
        return edges
