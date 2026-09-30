from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.raven_v63_microstructure_shadow_v2 import raven_v63_microstructure_shadow_v2 as source
from app.services.raven_v63_microstructure_shadow_v2 import BASE_ROUND_TRIP_BPS,SLIPPAGE_RESERVE_BPS

ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'raven_v80_slow_micro_shadow.json'
HORIZONS=(1800,3600)
CAPITAL_PER_SIGNAL=0.10
SLEEVE_SHARE=0.50
DECISION_SECONDS=300

class RavenV80SlowMicroShadow:
    def __init__(self):
        self.enabled=True;self.task=None;self.last_error=None;self.last_refresh=None;self.latest={}
        self.state=self._load()
    @staticmethod
    def _blank():
        return {'mode':'PAPER_SHADOW','version':'v80','future_only':True,'equity':100.0,'peak':100.0,
                'max_dd_pct':0.0,'started_at':time.time(),'last_decision_bucket':None,
                'resolved':0,'wins':0,'net_sum':0.0,'cost_sum':0.0,'open_trials':[],'history':[]}
    def _load(self):
        x=self._blank()
        try:x.update(json.loads(STATE.read_text(encoding='utf-8')))
        except Exception:pass
        return x
    def _save(self):
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _resolve(self,snap):
        now=float(snap.get('ts') or time.time());keep=[];delta=0.0;resolved=[]
        for t in self.state.get('open_trials') or []:
            if now-float(t['entry_ts'])<int(t['horizon'])-5:
                keep.append(t);continue
            x=(snap.get('symbols') or {}).get(t['symbol'])
            if not x:
                keep.append(t);continue
            raw=int(t['signal'])*(float(x['mid'])/float(t['entry_mid'])-1.0)
            cost_bps=BASE_ROUND_TRIP_BPS+SLIPPAGE_RESERVE_BPS+0.5*(float(t['entry_spread_bps'])+float(x['spread_bps']))
            net=raw-cost_bps/10000.0;ret=CAPITAL_PER_SIGNAL*SLEEVE_SHARE*net;delta+=ret
            self.state['resolved']=int(self.state.get('resolved') or 0)+1
            self.state['wins']=int(self.state.get('wins') or 0)+int(net>0)
            self.state['net_sum']=float(self.state.get('net_sum') or 0.0)+net
            self.state['cost_sum']=float(self.state.get('cost_sum') or 0.0)+cost_bps/10000.0
            resolved.append({'symbol':t['symbol'],'horizon':t['horizon'],'net_pct':net*100.0})
        self.state['open_trials']=keep
        if delta:
            eq=float(self.state.get('equity') or 100.0)*(1.0+delta);self.state['equity']=eq
            self.state['peak']=max(float(self.state.get('peak') or eq),eq)
            self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0.0),(eq/self.state['peak']-1.0)*100.0)
        return resolved
    def _open(self,snap):
        active={(t['symbol'],int(t['horizon'])) for t in self.state.get('open_trials') or []};opened=[]
        for sym,x in (snap.get('symbols') or {}).items():
            sig=int(x.get('signal_v2') or 0)
            if not sig:continue
            for h in HORIZONS:
                if (sym,h) in active:continue
                row={'symbol':sym,'horizon':h,'signal':sig,'entry_ts':float(snap['ts']),
                     'entry_mid':float(x['mid']),'entry_spread_bps':float(x['spread_bps']),
                     'score':float(x.get('score_v2') or 0.0),'persistence':float(x.get('persistence') or 0.0)}
                self.state['open_trials'].append(row);opened.append(row)
        return opened

    async def refresh(self):
        try:
            snap=dict(source.latest or {})
            if not snap.get('ts') or not snap.get('symbols'):
                self.last_error=None;self.last_refresh=time.time();return self.status()
            resolved=self._resolve(snap);db=int(float(snap['ts'])//DECISION_SECONDS);opened=[]
            if db!=self.state.get('last_decision_bucket'):
                opened=self._open(snap);self.state['last_decision_bucket']=db
            if resolved or opened:
                h=list(self.state.get('history') or []);h.append({'ts':float(snap['ts']),'equity':float(self.state['equity']),
                    'resolved':resolved,'opened':[{'symbol':x['symbol'],'horizon':x['horizon'],'signal':x['signal']} for x in opened]})
                self.state['history']=h[-500:]
            self._save();self.last_error=None;self.last_refresh=time.time();self.latest={'source_ts':snap['ts'],'resolved_now':resolved,'opened_now':opened}
        except Exception as exc:
            self.last_error=str(exc)[:400];self.last_refresh=time.time()
        return self.status()
    def status(self):
        eq=float(self.state.get('equity') or 100.0);n=int(self.state.get('resolved') or 0);age=max(0.0,time.time()-float(self.state.get('started_at') or time.time()))
        phase='WARMUP' if age<259200 or n<100 else 'FUTURE_VALIDATION'
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW','version':'v80',
                'strategy':'slow_micro_30m_60m','future_only':True,'development_source':'v63.2_pre_v80_only',
                'promotion_eligible':False,'allocator_eligible':False,'live_enabled':False,
                'grid':False,'martingale':False,'dca':False,'horizons_seconds':list(HORIZONS),
                'sleeve_share':SLEEVE_SHARE,'capital_per_signal_pct':CAPITAL_PER_SIGNAL*100.0,
                'equity':round(eq,6),'return_pct':round(eq-100.0,4),'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0.0),4),
                'resolved':n,'wins':int(self.state.get('wins') or 0),'win_rate':round(int(self.state.get('wins') or 0)/n,4) if n else None,
                'mean_net_signal_pct':round(float(self.state.get('net_sum') or 0.0)/n*100.0,5) if n else None,
                'open_trials':len(self.state.get('open_trials') or []),'phase':phase,'latest':self.latest,
                'last_refresh':self.last_refresh,'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        asyncio.create_task(self.refresh(),name='initial-refresh-'+self.__class__.__name__);self.task=asyncio.create_task(self._loop(),name='raven-v80-slow-micro-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None;self._save()
    async def _loop(self):
        while True:
            await asyncio.sleep(10.0)
            if self.enabled:await self.refresh()

raven_v80_slow_micro_shadow=RavenV80SlowMicroShadow()
