from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.funding_dislocation_paper_v1 import funding_dislocation_paper_v1
ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
SRC=DATA/'liquidity_migration_perp_snapshots_v1.jsonl';DISC=DATA/'funding_dislocation_probe_v2.json'
STATE=DATA/'funding_dislocation_persistence_v3.json';INTERVAL=300.0
VENUES=('Binance','Bybit','OKX');MAX_BASIS_BPS=8.0;REQUIRED_PERSIST=3

def _load_json(p,default):
    try:return json.loads(p.read_text(encoding='utf-8'))
    except Exception:return default

def _latest_snapshot():
    try:
        with SRC.open('rb') as f:
            f.seek(0,2);pos=f.tell()-1;buf=b''
            while pos>=0:
                f.seek(pos);c=f.read(1)
                if c==b'\n' and buf:break
                if c!=b'\n':buf=c+buf
                pos-=1
        return json.loads(buf.decode('utf-8')) if buf else {}
    except Exception:return {}

def _vm(s,v):
    x=((s.get('venues') or {}).get(v) or {});m=x.get('mid');r=x.get('funding_rate');h=x.get('funding_interval_hours')
    if m is None or r is None or h is None or float(h)<=0:return None
    return {'venue':v,'mid':float(m),'per_hour':float(r)/float(h)}
def _thresholds():
    d=_load_json(DISC,{})
    return {s:float((x or {}).get('p95_spread_per_hour') or 0.0) for s,x in (d.get('symbols') or {}).items()}

def _signals(snap,thr):
    out=[]
    for sym,s in (snap.get('symbols') or {}).items():
        vs=[x for x in (_vm(s,v) for v in VENUES) if x]
        if len(vs)<2 or sym not in thr:continue
        hi=max(vs,key=lambda x:x['per_hour']);lo=min(vs,key=lambda x:x['per_hour'])
        mid=(hi['mid']+lo['mid'])/2.0;basis=(hi['mid']-lo['mid'])/mid*10000 if mid else 999
        spread=hi['per_hour']-lo['per_hour']
        if spread>=thr[sym] and abs(basis)<=MAX_BASIS_BPS:
            out.append({'symbol':sym,'long':lo['venue'],'short':hi['venue'],'spread_per_hour':spread,'basis_bps':basis})
    return out

class FundingDislocationPersistenceV3:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None
        self.thresholds=_thresholds();self.frozen_at=time.time();self.paper=funding_dislocation_paper_v1
        self.state=_load_json(STATE,{'version':'FUNDING_DISLOCATION_PERSISTENCE_V3','started_at':time.time(),'last_ts':None,'runs':0,'streaks':{},'qualified':[]})
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    async def refresh(self):
        try:
            snap=_latest_snapshot();ts=float(snap.get('ts') or 0)
            if ts and ts!=self.state.get('last_ts'):
                sigs=_signals(snap,self.thresholds);active={(x['symbol'],x['long'],x['short']):x for x in sigs}
                streaks=dict(self.state.get('streaks') or {});new={}
                for key,x in active.items():
                    sk='|'.join(key);n=int((streaks.get(sk) or {}).get('count') or 0)+1
                    new[sk]={'count':n,'last':x,'last_ts':ts}
                self.state['streaks']=new;self.state['last_ts']=ts;self.state['runs']=int(self.state.get('runs') or 0)+1
                self.state['qualified']=[v['last']|{'persistence':v['count']} for v in new.values() if v['count']>=REQUIRED_PERSIST]
                self.state['last_signals']=sigs;self.state['last_refresh']=time.time();self.paper.refresh(snap,self.state['qualified']);self._save()
            self.last_error=None
        except Exception as e:self.last_error=str(e)[:500]
        return self.status()
    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_PERSISTENCE','strategy':'FUNDING_DISLOCATION_PERSISTENCE_V3','live_enabled':False,'paper_only':True,'frozen_at':self.frozen_at,'locked_thresholds':self.thresholds,'required_persistence':REQUIRED_PERSIST,'runs':int(self.state.get('runs') or 0),'last_signals':self.state.get('last_signals') or [],'qualified':self.state.get('qualified') or [],'paper':self.paper.status(),'last_error':self.last_error,'policy':{'thresholds_frozen':True,'no_retrospective_winner':True,'no_live_orders':True,'no_grid':True,'no_martingale':True,'no_dca':True}}
    async def start(self):
        if not self.task or self.task.done():self.task=asyncio.create_task(self._loop(),name='funding-dislocation-persistence-v3')
    async def stop(self):
        if self.task and not self.task.done():self.task.cancel()
        self.task=None
    async def _loop(self):
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(INTERVAL)
funding_dislocation_persistence_v3=FundingDislocationPersistenceV3()
