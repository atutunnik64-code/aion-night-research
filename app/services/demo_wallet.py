from __future__ import annotations
import asyncio,json,time
from pathlib import Path

PATH=Path(__file__).parents[2]/'data'/'demo_wallet.json'

class DemoWallet:
    def __init__(self):
        self.lock=asyncio.Lock(); self.balances={}; self.initial={}
        self.trade_count=0; self.realized_profit=0.0; self.updated_at=time.time(); self._load()
    def _load(self):
        try:
            d=json.loads(PATH.read_text(encoding='utf-8'))
            self.balances=d.get('balances') or {}; self.initial=d.get('initial') or {}
            self.trade_count=int(d.get('trade_count') or 0); self.realized_profit=float(d.get('realized_profit') or 0)
        except Exception:pass
    def _save(self):
        PATH.parent.mkdir(parents=True,exist_ok=True)
        PATH.write_text(json.dumps({'balances':self.balances,'initial':self.initial,'trade_count':self.trade_count,
            'realized_profit':self.realized_profit,'updated_at':self.updated_at},ensure_ascii=False,indent=2),encoding='utf-8')
    def _get(self,venue,asset):
        return float((self.balances.get(venue) or {}).get(asset,0) or 0)
    def _set(self,venue,asset,value):
        self.balances.setdefault(venue,{})[asset]=float(value)
        self.initial.setdefault(venue,{}).setdefault(asset,float(value))
    async def ensure_prefunded(self,legs,reserve_pct=15.0):
        async with self.lock:
            seeded=[]
            for leg in legs:
                venue=str(leg.get('venue')); asset=str(leg.get('input_asset') or '').upper()
                need=max(0.0,float(leg.get('input_required') or 0))
                if not venue or not asset or need<=0:continue
                current=self._get(venue,asset)
                if current<=0:
                    seed=max(need*5.0,10000.0 if asset in {'USDT','USDC','USD','EUR'} else need*8.0)
                    self._set(venue,asset,seed); seeded.append({'venue':venue,'asset':asset,'amount':seed})
                initial=float((self.initial.get(venue) or {}).get(asset,self._get(venue,asset)) or 0)
                reserve=initial*max(0.0,min(90.0,float(reserve_pct)))/100.0
                if self._get(venue,asset)-need<reserve:
                    return {'ok':False,'status':'SHADOW_RESERVE_GUARD','venue':venue,'asset':asset,
                            'free':self._get(venue,asset),'required':need,'reserve':reserve,'seeded':seeded}
            self.updated_at=time.time(); self._save()
            return {'ok':True,'status':'SHADOW_PREFUNDED','seeded':seeded}

    async def settle_cycle(self,legs,profit_quote=0.0,depth=None):
        async with self.lock:
            depth=depth or {}; buy=next((x for x in legs if str(x.get('side')).lower()=='buy'),{}); sell=next((x for x in legs if str(x.get('side')).lower()=='sell'),{})
            if not buy or not sell:return {**self.status(),'cycle_profit':0.0}
            bv=str(buy.get('venue')); sv=str(sell.get('venue')); base=str(buy.get('base') or '').upper(); quote=str(buy.get('quote') or '').upper()
            bqty=float(buy.get('qty') or 0); sqty=float(sell.get('qty') or 0); qty=min(bqty,sqty)
            bval=float(buy.get('depth_value') or bqty*float(buy.get('depth_vwap') or buy.get('price') or 0)); sval=float(sell.get('depth_value') or sqty*float(sell.get('depth_vwap') or sell.get('price') or 0))
            bcost=bval*(1+float(buy.get('fee_rate') or 0)); sproceeds=sval*(1-float(sell.get('fee_rate') or 0)); rebalance=float(depth.get('rebalance_cost_quote') or 0)
            self._set(bv,quote,self._get(bv,quote)-bcost); self._set(bv,base,self._get(bv,base)+qty)
            self._set(sv,base,self._get(sv,base)-qty); self._set(sv,quote,self._get(sv,quote)+sproceeds)
            self._set(bv,base,self._get(bv,base)-qty); self._set(sv,base,self._get(sv,base)+qty)
            self._set(sv,quote,self._get(sv,quote)-bcost-rebalance); self._set(bv,quote,self._get(bv,quote)+bcost)
            cycle_profit=sproceeds-bcost-rebalance
            self.trade_count+=1; self.realized_profit+=cycle_profit; self.updated_at=time.time(); self._save()
            return {**self.status(),'cycle_profit':round(cycle_profit,10),'rebalance_cost_quote':round(rebalance,10)}
    def status(self):
        return {'balances':self.balances,'trade_count':self.trade_count,
                'realized_profit':round(self.realized_profit,8),'updated_at':self.updated_at,
                'environment':'SHADOW_LIVE','capital_model':'UNBOUNDED_AUTO_PREFUND_EXECUTION_PROBE',
                'capital_return_valid':False}
    def reset(self):
        self.balances={}; self.initial={}; self.trade_count=0; self.realized_profit=0.0
        self.updated_at=time.time(); self._save(); return self.status()

demo_wallet=DemoWallet()
