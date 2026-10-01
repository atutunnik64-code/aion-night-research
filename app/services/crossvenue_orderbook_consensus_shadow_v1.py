from __future__ import annotations
import asyncio,json,time
from pathlib import Path
import httpx
from app.http_shared import SHARED_SSL_CONTEXT

ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'; STATE=DATA/'crossvenue_orderbook_consensus_shadow_v1.json'
ASSETS=('BTC','ETH','SOL','XRP','DOGE','ADA','LINK','AVAX','LTC','SUI')
VENUES=('Bybit','OKX','Bitget')
IMB_THRESHOLD=0.35; MIN_AGREE=2; HOLD_SEC=900; COOLDOWN_SEC=1800
ROUND_TRIP_COST=0.0025; ALLOC=0.05; DEPTH=10

class CrossVenueOrderbookConsensusShadowV1:
    def __init__(self):
        self.enabled=True; self.live_enabled=False; self.task=None; self.last_error=None; self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state={'version':'CROSSVENUE_ORDERBOOK_CONSENSUS_V1','rule_frozen_at':time.time(),'last_event_ts':{},'pending':[],'resolved':[],'equity':100.0,'peak':100.0,'max_dd_pct':0.0}
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    async def _book(self,c,v,b):
        try:
            if v=='Bybit':
                r=await c.get('https://api.bybit.com/v5/market/orderbook',params={'category':'spot','symbol':b+'USDT','limit':DEPTH}); d=r.json(); x=d.get('result') or {}; bids=x.get('b') or []; asks=x.get('a') or []
            elif v=='OKX':
                r=await c.get('https://www.okx.com/api/v5/market/books',params={'instId':b+'-USDT','sz':DEPTH}); d=r.json(); x=(d.get('data') or [{}])[0]; bids=x.get('bids') or []; asks=x.get('asks') or []
            else:
                r=await c.get('https://api.bitget.com/api/v2/spot/market/orderbook',params={'symbol':b+'USDT','type':'step0','limit':DEPTH}); d=r.json(); x=d.get('data') or {}; bids=x.get('bids') or []; asks=x.get('asks') or []
            def val(rows):
                out=[]
                for z in rows:
                    try:out.append((float(z[0]),float(z[1])))
                    except Exception:pass
                return out
            B,A=val(bids),val(asks)
            if not B or not A:return None
            bid_not=sum(p*q for p,q in B); ask_not=sum(p*q for p,q in A); den=bid_not+ask_not
            imb=(bid_not-ask_not)/den if den>0 else 0.0; mid=(B[0][0]+A[0][0])/2
            return {'venue':v,'base':b,'imbalance':imb,'mid':mid,'ts':time.time()}
        except Exception:return None
    async def refresh(self):
        try:
            now=time.time(); timeout=httpx.Timeout(7.0)
            async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT,timeout=timeout,follow_redirects=True) as c:
                vals=await asyncio.gather(*[self._book(c,v,b) for b in ASSETS for v in VENUES])
            by={}
            for x in vals:
                if x:by.setdefault(x['base'],[]).append(x)
            keep=[]
            for p in self.state.get('pending') or []:
                rows=by.get(p['base']) or []
                if now<float(p['due_ts']) or not rows:keep.append(p);continue
                px=sum(x['mid'] for x in rows)/len(rows); gross=float(p['side'])*(px/float(p['entry'])-1.0); net=gross-ROUND_TRIP_COST
                out={**p,'exit':px,'exit_ts':now,'net_return_pct':net*100}; self.state.setdefault('resolved',[]).append(out); self.state['resolved']=self.state['resolved'][-1000:]
                eq=float(self.state.get('equity') or 100)*(1+ALLOC*net); peak=max(float(self.state.get('peak') or 100),eq); self.state['equity']=eq; self.state['peak']=peak; self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0),(eq/peak-1)*100)
            self.state['pending']=keep
            new=0
            for base,rows in by.items():
                up=[x for x in rows if x['imbalance']>=IMB_THRESHOLD]; dn=[x for x in rows if x['imbalance']<=-IMB_THRESHOLD]
                side=1 if len(up)>=MIN_AGREE and len(up)>len(dn) else (-1 if len(dn)>=MIN_AGREE and len(dn)>len(up) else 0)
                if not side:continue
                prev=float((self.state.get('last_event_ts') or {}).get(base) or 0)
                if now-prev<COOLDOWN_SEC:continue
                entry=sum(x['mid'] for x in rows)/len(rows)
                self.state.setdefault('pending',[]).append({'id':f'{base}:{int(now)}','base':base,'ts':now,'due_ts':now+HOLD_SEC,'side':side,'entry':entry,'agree_count':len(up) if side>0 else len(dn),'venue_imbalances':{x['venue']:x['imbalance'] for x in rows},'round_trip_cost':ROUND_TRIP_COST})
                self.state.setdefault('last_event_ts',{})[base]=now; new+=1
            self.state['last_new_events']=new; self.state['scan_count']=int(self.state.get('scan_count') or 0)+1; self._save(); self.last_error=None; self.last_refresh=now
        except Exception as exc:self.last_error=str(exc)[:500]
        return self.status()
    def status(self):
        r=self.state.get('resolved') or []; wins=sum(1 for x in r if float(x.get('net_return_pct') or 0)>0)
        return {'ok':self.last_error is None,'strategy':'CROSSVENUE_ORDERBOOK_CONSENSUS_V1','mode':'FUTURE_ONLY_SHADOW','paper_only':True,'live_enabled':False,'pending_count':len(self.state.get('pending') or []),'resolved_count':len(r),'return_pct':float(self.state.get('equity',100))-100,'max_dd_pct':self.state.get('max_dd_pct',0),'wins':wins,'win_rate':wins/len(r) if r else None,'last_new_events':self.state.get('last_new_events',0),'locked_rule':{'depth_levels':DEPTH,'imbalance_threshold':IMB_THRESHOLD,'min_agree_venues':MIN_AGREE,'hold_minutes':15,'cooldown_minutes':30,'round_trip_cost':ROUND_TRIP_COST,'allocation_fraction':ALLOC},'promotion_eligible':False,'last_error':self.last_error,'policy':{'no_grid':True,'no_martingale':True,'no_dca':True,'no_parameter_tuning':True,'no_live_orders':True}}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='crossvenue-orderbook-consensus-v1')
    async def stop(self):
        if self.task and not self.task.done():self.task.cancel();await asyncio.gather(self.task,return_exceptions=True)
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(60)
            if self.enabled:await self.refresh()

crossvenue_orderbook_consensus_shadow_v1=CrossVenueOrderbookConsensusShadowV1()
