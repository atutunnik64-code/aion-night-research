from __future__ import annotations
import json
from pathlib import Path
from app.services.opportunity_memory import memory

PATH=Path(__file__).parents[2]/'data'/'capital.json'

class CapitalManager:
    def __init__(self):
        self.state={'paper_capital_usdt':10000.0,'reserve_pct':20.0,'max_route_pct':15.0,'balances':{}}
        self.load()

    def load(self):
        if PATH.exists():
            try:self.state.update(json.loads(PATH.read_text(encoding='utf-8')))
            except Exception:pass
        self.save()

    def save(self):
        PATH.parent.mkdir(parents=True,exist_ok=True)
        PATH.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')

    def update(self,payload):
        for key in ('paper_capital_usdt','reserve_pct','max_route_pct','balances'):
            if key in payload:self.state[key]=payload[key]
        self.save(); return self.status()

    def status(self):
        capital=float(self.state.get('paper_capital_usdt') or 0); reserve=capital*float(self.state.get('reserve_pct') or 0)/100
        deploy=max(0,capital-reserve); per=capital*float(self.state.get('max_route_pct') or 0)/100
        return {**self.state,'reserve_usdt':round(reserve,2),'deployable_usdt':round(deploy,2),'max_per_route_usdt':round(per,2),
                'recommendations':self.recommendations(deploy,per)}
    def recommendations(self,deployable,per_route):
        rows=memory.leaderboard(limit=30,min_hits=2)
        rows=[x for x in rows if x.get('aion_score',0)>=35 and x.get('avg_net_pct',0)>0]
        total=sum(max(1.0,x['aion_score']) for x in rows[:10]) or 1.0
        remaining=deployable; out=[]
        for x in rows[:10]:
            raw=deployable*max(1.0,x['aion_score'])/total
            amount=min(per_route,raw,remaining,float(x.get('avg_liquidity') or per_route))
            if amount<10:continue
            out.append({'signature':x['signature'],'mode':x['mode'],'label':x['label'],'aion_score':x['aion_score'],
                        'target_usdt':round(amount,2),'avg_net_pct':x['avg_net_pct'],'historical_hits':x['hits']})
            remaining-=amount
            if remaining<10:break
        return out

capital_manager=CapitalManager()
