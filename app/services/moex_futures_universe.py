from __future__ import annotations
import asyncio,json,time,urllib.parse,urllib.request,subprocess
from collections import defaultdict
from pathlib import Path
ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
STATE=DATA/'moex_futures_universe_v1.json'; SNAP=DATA/'moex_futures_universe_v1.jsonl'
URL='https://iss.moex.com/iss/engines/futures/markets/forts/securities.json'
INTERVAL=300.0

def _map(block):
    c=block.get('columns') or []
    return [{c[i]:r[i] for i in range(min(len(c),len(r)))} for r in (block.get('data') or [])]

def _fetch_once():
    p={'iss.meta':'off','iss.only':'securities,marketdata',
       'securities.columns':'SECID,SHORTNAME,ASSETCODE,LASTTRADEDATE',
       'marketdata.columns':'SECID,LAST,LAST_RUB,SETTLEPRICE,OPENPOSITION,VOLTODAY,VALTODAY,SYSTIME'}
    u=URL+'?'+urllib.parse.urlencode(p)
    try:
        req=urllib.request.Request(u,headers={'User-Agent':'AION-Crypto-Radar/0.11.58'})
        with urllib.request.urlopen(req,timeout=10) as r:return json.loads(r.read().decode('utf-8'))
    except Exception:
        q=subprocess.run(['curl.exe','-fsS','--connect-timeout','8','--max-time','15',u],capture_output=True,text=True,encoding='utf-8',timeout=18)
        if q.returncode:return None
        return json.loads(q.stdout)
def _tier(row):
    val=float(row.get('VALTODAY') or 0); vol=float(row.get('VOLTODAY') or 0); oi=float(row.get('OPENPOSITION') or 0)
    if oi<=0 and vol<=0:return 'INACTIVE'
    if val>=50_000_000 and oi>0:return 'CORE'
    if val>=10_000_000 and oi>0:return 'LIQUID'
    if val>=1_000_000 or vol>=100:return 'WATCH'
    return 'THIN'

def _build():
    d=_fetch_once()
    if not d: raise RuntimeError('MOEX_ISS_UNAVAILABLE')
    ss=_map(d.get('securities') or {}); md={str(x.get('SECID')):x for x in _map(d.get('marketdata') or {})}
    rows=[]
    for s in ss:
        r={**s,**(md.get(str(s.get('SECID'))) or {})}; r['tier']=_tier(r)
        r['active']=r['tier']!='INACTIVE'; rows.append(r)
    active=[r for r in rows if r['active']]
    groups=defaultdict(list)
    for r in active: groups[str(r.get('ASSETCODE') or 'UNKNOWN')].append(r)
    summary=[]
    for asset,items in groups.items():
        items=sorted(items,key=lambda x:float(x.get('VALTODAY') or 0),reverse=True)
        summary.append({'asset':asset,'contracts':len(items),'total_value_today':sum(float(x.get('VALTODAY') or 0) for x in items),
                        'total_volume_today':sum(float(x.get('VOLTODAY') or 0) for x in items),'total_oi':sum(float(x.get('OPENPOSITION') or 0) for x in items),
                        'best_tier':min((x['tier'] for x in items),key=lambda z:['CORE','LIQUID','WATCH','THIN'].index(z)),
                        'front':items[0].get('SECID'),'members':[x.get('SECID') for x in items]})
    summary.sort(key=lambda x:x['total_value_today'],reverse=True)
    return {'ts':time.time(),'mode':'FUTURE_ONLY_UNIVERSE','source':'MOEX_ISS_DELAYED','source_delay_minutes':15,
            'contracts_total':len(rows),'contracts_active':len(active),'assets_active':len(groups),
            'tiers':{t:sum(1 for r in active if r['tier']==t) for t in ('CORE','LIQUID','WATCH','THIN')},
            'assets':summary,'contracts':active}
class MoexFuturesUniverse:
    def __init__(self):
        self.enabled=True; self.live_enabled=False; self.task=None; self.last_error=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state={'version':'MOEX_FUTURES_UNIVERSE_V1','started_at':time.time(),'refresh_count':0,'last':None}
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    async def refresh(self):
        try:
            row=await asyncio.to_thread(_build)
            self.state['refresh_count']=int(self.state.get('refresh_count') or 0)+1
            self.state['last']=row; self.state['last_refresh']=row['ts']; self.last_error=None
            self._save()
            with SNAP.open('a',encoding='utf-8') as f:f.write(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n')
        except Exception as exc:self.last_error=str(exc)[:500]
        return self.status()
    def status(self):
        last=self.state.get('last') or {}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_UNIVERSE','live_enabled':False,'paper_only':True,
                'refresh_count':int(self.state.get('refresh_count') or 0),'last_refresh':self.state.get('last_refresh'),
                'contracts_total':last.get('contracts_total'),'contracts_active':last.get('contracts_active'),'assets_active':last.get('assets_active'),
                'tiers':last.get('tiers') or {},'top_assets':(last.get('assets') or [])[:40],'last_error':self.last_error,
                'policy':{'whole_forts_universe':True,'objective_liquidity_tiers':True,'no_live_orders':True,'no_grid':True,'no_martingale':True,'no_dca':True}}
    async def start(self):
        if not self.task or self.task.done():self.task=asyncio.create_task(self._loop(),name='moex-futures-universe')
    async def stop(self):
        if self.task and not self.task.done():self.task.cancel()
        self.task=None
    async def _loop(self):
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(INTERVAL)

moex_futures_universe=MoexFuturesUniverse()
