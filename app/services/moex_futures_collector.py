from __future__ import annotations
import asyncio,json,time,urllib.parse,urllib.request,subprocess
from pathlib import Path

ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
RAW=DATA/'moex_futures_snapshots_v1.jsonl'
STATE=DATA/'moex_futures_collector_v1.json'
INTERVAL=300.0
URL='https://iss.moex.com/iss/engines/futures/markets/forts/securities.json'
HEADERS={'User-Agent':'AION-Crypto-Radar/0.11.58'}
TARGET_PREFIXES=('GOLD','SILV','GLDRUBF','SLVRUBF','SBER','SBRF')

def _append(row):
    with RAW.open('a',encoding='utf-8') as f:
        f.write(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n')

def _table_map(block):
    cols=block.get('columns') or []; out=[]
    for row in block.get('data') or []:
        out.append({cols[i]:row[i] for i in range(min(len(cols),len(row)))})
    return out
def _get_once():
    params={
        'iss.meta':'off','iss.only':'securities,marketdata',
        'securities.columns':'SECID,SHORTNAME,ASSETCODE,LASTTRADEDATE',
        'marketdata.columns':'SECID,LAST,LAST_RUB,SETTLEPRICE,OPENPOSITION,VOLTODAY,VALTODAY,SYSTIME'}
    req=urllib.request.Request(URL+'?'+urllib.parse.urlencode(params),headers=HEADERS)
    with urllib.request.urlopen(req,timeout=8) as r:
        return json.loads(r.read().decode('utf-8'))

def _fetch():
    last=None
    for attempt in range(2):
        try:return _get_once()
        except Exception as exc:
            last=exc; time.sleep(1.0*(attempt+1))
    params={'iss.meta':'off','iss.only':'securities,marketdata','securities.columns':'SECID,SHORTNAME,ASSETCODE,LASTTRADEDATE','marketdata.columns':'SECID,LAST,LAST_RUB,SETTLEPRICE,OPENPOSITION,VOLTODAY,VALTODAY,SYSTIME'}
    url=URL+'?'+urllib.parse.urlencode(params)
    try:
        r=subprocess.run(['curl.exe','-fsS','--connect-timeout','8','--max-time','15',url],capture_output=True,text=True,encoding='utf-8',timeout=18)
        if r.returncode==0:return json.loads(r.stdout)
        last=RuntimeError(r.stderr[:240])
    except Exception as exc:last=exc
    raise RuntimeError(f'MOEX_ISS_UNAVAILABLE: {last}')

def _wanted(sec):
    name=str(sec.get('SHORTNAME') or '').upper()
    asset=str(sec.get('ASSETCODE') or '').upper()
    return any(name.startswith(x) or asset.startswith(x) for x in TARGET_PREFIXES)
def _snapshot_sync():
    now=time.time(); data=_fetch()
    secs=[x for x in _table_map(data.get('securities') or {}) if _wanted(x)]
    md={str(x.get('SECID')):x for x in _table_map(data.get('marketdata') or {})}
    rows=[]
    for sec in secs:
        sid=str(sec.get('SECID') or '')
        row={**sec,**(md.get(sid) or {})}
        if row.get('LAST') is not None or row.get('SETTLEPRICE') is not None:
            rows.append(row)
    out={'ts':now,'mode':'FUTURE_ONLY_COLLECT','source':'MOEX_ISS_DELAYED',
         'source_delay_minutes':15,'rows':rows,'contracts':len(rows)}
    _append(out); return out

class MoexFuturesCollector:
    def __init__(self):
        self.enabled=True; self.live_enabled=False; self.task=None; self.last_error=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state={'version':'MOEX_FUTURES_COLLECTOR_V1','started_at':time.time(),'snapshot_count':0,'last_good':None}
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    async def refresh(self):
        try:
            row=await asyncio.to_thread(_snapshot_sync)
            self.state['snapshot_count']=int(self.state.get('snapshot_count') or 0)+1
            self.state['last_good']=row; self.state['last_refresh']=row['ts']
            self.last_error=None; self._save()
        except Exception as exc:
            self.last_error=str(exc)[:500]
            self.state['last_attempt']=time.time(); self.state['last_error']=self.last_error; self._save()
        return self.status()
    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_COLLECT',
                'strategy':'MOEX_FUTURES_MULTI_ASSET_V1','live_enabled':False,'paper_only':True,
                'targets':['GOLD','SILV','GLDRUBF','SLVRUBF','SBER','SBRF'],'interval_seconds':INTERVAL,
                'snapshot_count':int(self.state.get('snapshot_count') or 0),'last_good':self.state.get('last_good'),
                'last_refresh':self.state.get('last_refresh'),'last_error':self.last_error,
                'policy':{'source':'MOEX_ISS','free_data_delay_minutes':15,'no_live_orders':True,'no_grid':True,'no_martingale':True,'no_dca':True}}
    async def start(self):
        if not self.task or self.task.done(): self.task=asyncio.create_task(self._loop(),name='moex-futures-collector')
    async def stop(self):
        if self.task and not self.task.done(): self.task.cancel()
        self.task=None
    async def _loop(self):
        while True:
            if self.enabled: await self.refresh()
            await asyncio.sleep(INTERVAL)

moex_futures_collector=MoexFuturesCollector()


