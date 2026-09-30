from __future__ import annotations
from collections import defaultdict
from app.models.domain import Edge

class PrefundedCycleFinder:
    def __init__(self, min_profit_pct: float = 0.001, min_start: float = 10.0, max_hops: int = 3):
        self.min_profit_pct=min_profit_pct
        self.min_start=min_start
        self.max_hops=max_hops

    @staticmethod
    def _multiplier(edge: Edge) -> float:
        return max(0.0, edge.rate * (1.0-edge.fee_rate) * (1.0-edge.slippage_rate))

    def _route_capacity(self, path: list[Edge], requested: float) -> float:
        limit=float(requested); cumulative=1.0
        for e in path:
            if cumulative<=0: return 0.0
            limit=min(limit, float(e.capacity)/cumulative)
            cumulative*=self._multiplier(e)
        return max(0.0,limit)

    def _evaluate(self, path: list[Edge], requested: float):
        start=self._route_capacity(path,requested)
        if start<self.min_start:return None
        amount=start
        for e in path:
            amount=e.apply(amount)
            if amount<=0:return None
        pct=(amount/start-1)*100
        if pct<self.min_profit_pct:return None
        return self._format(path,start,amount,pct)
    def find(self, edges: list[Edge], requested: float, start_assets: set[str]) -> list[dict]:
        graph=defaultdict(list)
        for e in edges:
            if e.kind=='trade' and e.executable:
                graph[e.src.asset].append(e)
        results=[]
        for start in start_assets:
            for e1 in graph.get(start,[]):
                a1=e1.dst.asset
                if a1==start: continue
                for e2 in graph.get(a1,[]):
                    a2=e2.dst.asset
                    if a2==start:
                        row=self._evaluate([e1,e2],requested)
                        if row: results.append(row)
                        continue
                    if self.max_hops<3 or a2 in (start,a1): continue
                    for e3 in graph.get(a2,[]):
                        if e3.dst.asset!=start: continue
                        row=self._evaluate([e1,e2,e3],requested)
                        if row: results.append(row)
        unique={}
        for row in results:
            sig=row['signature']
            if sig not in unique or row['profit_amount']>unique[sig]['profit_amount']:
                unique[sig]=row
        return sorted(unique.values(),key=lambda x:(x['profit_amount'],x['profit_pct']),reverse=True)

    def confirm(self, candidates: list[dict], edges: list[Edge], requested: float) -> list[dict]:
        edge_map={}
        for e in edges:
            if e.kind!='trade' or not e.executable: continue
            m=e.meta or {}; key=(e.src.venue,m.get('symbol'),e.src.asset,e.dst.asset)
            edge_map[key]=e
        out=[]
        for candidate in candidates:
            path=[]
            for step in candidate.get('steps') or []:
                e=edge_map.get((step.get('venue'),step.get('symbol'),step.get('from_asset'),step.get('to_asset')))
                if e is None: path=[]; break
                path.append(e)
            if not path: continue
            row=self._evaluate(path,requested)
            if row is not None: out.append(row)
        return out

    def _format(self,path,start,final,pct):
        venues=[e.src.venue for e in path]
        assets=[path[0].src.asset]+[e.dst.asset for e in path]
        signature='>'.join(f"{e.src.venue}:{(e.meta or {}).get('symbol')}:{e.src.asset}->{e.dst.asset}" for e in path)
        return {
            'kind':'prefunded_cycle','signature':signature,'start_asset':assets[0],
            'start_amount':round(start,8),'final_amount':round(final,8),
            'profit_amount':round(final-start,8),'profit_pct':round(pct,5),
            'hops':len(path),'venues':venues,'assets':assets,
            'market_closed':True,'venue_inventory_closed':False,'rebalance_required':True,
            'steps':[self._step(e,i) for i,e in enumerate(path,1)],
        }
    def _step(self,e:Edge,index:int):
        m=e.meta or {}
        return {
            'index':index,'venue':e.src.venue,'from_asset':e.src.asset,'to_asset':e.dst.asset,
            'symbol':m.get('symbol'),'side':m.get('side'),'rate':e.rate,
            'fee_rate':e.fee_rate,'capacity':e.capacity,'best_ask':m.get('best_ask'),
            'best_bid':m.get('best_bid'),'l1_qty':m.get('l1_qty'),
        }
