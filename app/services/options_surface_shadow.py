from __future__ import annotations
import asyncio,json,time,statistics,urllib.request,urllib.parse,subprocess,sys
from collections import defaultdict
from pathlib import Path
ROOT=Path(__file__).parents[2];STATE=ROOT/'data'/'options_surface_shadow.json';HIST=ROOT/'data'/'deribit_options_surface.jsonl'
BASE='https://www.deribit.com/api/v2/public/get_book_summary_by_currency'

class OptionsSurfaceShadow:
    def __init__(self):
        self.enabled=True;self.task=None;self.interval=900.;self.last_error=None;self.last_refresh=None;self.latest={}
    @staticmethod
    def _fetch(cur):
        url=BASE+'?'+urllib.parse.urlencode({'currency':cur,'kind':'option'})
        with urllib.request.urlopen(url,timeout=20) as r:items=json.loads(r.read().decode())['result']
        groups=defaultdict(list)
        for x in items:
            p=x.get('instrument_name','').split('-')
            if len(p)<4:continue
            try:
                strike=float(p[-2]);under=float(x.get('underlying_price') or 0);iv=float(x.get('mark_iv') or 0)
                row={'instrument':x.get('instrument_name',''),'strike':strike,'under':under,'iv':iv,'kind':p[-1],'oi':float(x.get('open_interest') or 0),
                     'mark':float(x.get('mark_price') or 0),'bid':float(x.get('bid_price') or 0),'ask':float(x.get('ask_price') or 0)}
            except:continue
            if under>0 and iv>0:groups[p[-3]].append(row)
        exps=[]
        for exp,a in groups.items():
            under=statistics.median(x['under'] for x in a); strikes=sorted(set(x['strike'] for x in a))
            atm_s=min(strikes,key=lambda k:abs(k/under-1)); put_s=min(strikes,key=lambda k:abs(k/under-.9)); call_s=min(strikes,key=lambda k:abs(k/under-1.1))
            put80_s=min(strikes,key=lambda k:abs(k/under-.8)); call120_s=min(strikes,key=lambda k:abs(k/under-1.2))
            def pick(k,kind): return next((x for x in a if x['strike']==k and x['kind']==kind),None)
            ac,ap=pick(atm_s,'C'),pick(atm_s,'P'); wp,wc=pick(put_s,'P'),pick(call_s,'C'); wp80,wc120=pick(put80_s,'P'),pick(call120_s,'C')
            atm_iv=statistics.mean([x['iv'] for x in (ac,ap) if x]) if (ac or ap) else 0
            skew=(wp['iv']-wc['iv']) if wp and wc else None
            def pair(field): return ((ac or {}).get(field,0)+(ap or {}).get(field,0))*100
            exps.append({'expiry':exp,'underlying':under,'atm_strike':atm_s,'atm_iv':atm_iv,'skew_down_minus_up':skew,
                         'open_interest':sum(x['oi'] for x in a),'contracts':len(a),'atm_straddle_mark_pct':pair('mark'),
                         'atm_straddle_bid_pct':pair('bid'),'atm_straddle_ask_pct':pair('ask'),
                         'put90_strike':put_s,'put90_mark':(wp or {}).get('mark'),'call110_strike':call_s,'call110_mark':(wc or {}).get('mark'),
                         'atm_call_instrument':(ac or {}).get('instrument'),'atm_put_instrument':(ap or {}).get('instrument'),
                         'put90_instrument':(wp or {}).get('instrument'),'call110_instrument':(wc or {}).get('instrument'),
                         'put80_instrument':(wp80 or {}).get('instrument'),'call120_instrument':(wc120 or {}).get('instrument'),
                         'put80_mark':(wp80 or {}).get('mark'),'call120_mark':(wc120 or {}).get('mark')})
        return {'contracts':len(items),'expiries':exps}
    def _refresh_sync(self):
        snap={'ts':time.time(),'source':'deribit_public','currencies':{}}
        for cur in ('BTC','ETH','SOL'):
            try:snap['currencies'][cur]=self._fetch(cur)
            except Exception as exc:snap['currencies'][cur]={'contracts':0,'expiries':[],'error':str(exc)[:160]}
        with HIST.open('a',encoding='utf-8') as f:f.write(json.dumps(snap,ensure_ascii=False)+'\n')
        old={}
        try:old=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:pass
        obs=int(old.get('observation_count') or 0)+1
        state={'strategy':'options_surface_v2','observation_count':obs,'last_snapshot':snap,'last_refresh':time.time()}
        STATE.write_text(json.dumps(state,ensure_ascii=False,indent=2),encoding='utf-8')
        edge_error=None; edge={}
        try:
            for script in ('refresh_options_rv_prices.py','options_iv_rv_edge_v2.py','options_edge_future_tracker.py'):
                cp=subprocess.run([sys.executable,str(ROOT/'research'/script)],cwd=str(ROOT),capture_output=True,text=True,timeout=45)
                if cp.returncode!=0: raise RuntimeError((cp.stderr or cp.stdout)[-500:])
            edge=json.loads((ROOT/'data'/'options_iv_rv_edge_v2.json').read_text(encoding='utf-8'))
        except Exception as exc: edge_error=str(exc)[:500]
        state['edge_map']=edge;state['edge_error']=edge_error
        try: state['edge_tracker']=json.loads((ROOT/'data'/'options_edge_future_tracker.json').read_text(encoding='utf-8')).get('summary') or {}
        except Exception: state['edge_tracker']={}
        STATE.write_text(json.dumps(state,ensure_ascii=False,indent=2),encoding='utf-8');return state
    async def refresh(self):
        try:
            st=await asyncio.to_thread(self._refresh_sync);self.latest=st;self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status()
    def status(self):
        st=self.latest
        if not st:
            try:st=json.loads(STATE.read_text(encoding='utf-8'))
            except Exception:st={}
        cur=(st.get('last_snapshot') or {}).get('currencies') or {}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'ANALYSIS_SHADOW','strategy':'options_surface_v2','future_only':True,
                'promotion_eligible':False,'live_enabled':False,'naked_options_enabled':False,'grid':False,'martingale':False,'dca':False,
                'observation_count':int(st.get('observation_count') or 0),'currencies':cur,'edge_map':st.get('edge_map') or {},'edge_tracker':st.get('edge_tracker') or {},'edge_error':st.get('edge_error'),'last_refresh':self.last_refresh or st.get('last_refresh'),'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='options-surface-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        await asyncio.sleep(8.)
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(60. if self.last_error else self.interval)

options_surface_shadow=OptionsSurfaceShadow()
