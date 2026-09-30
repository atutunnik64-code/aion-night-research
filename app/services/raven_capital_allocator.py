from __future__ import annotations
import asyncio, json, math, statistics, time
from pathlib import Path
from app.services.raven_engine import raven_engine
from app.services.raven_universal_shadow import raven_universal_shadow

ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'raven_allocator_state.json'

class RavenCapitalAllocator:
    def __init__(self):
        self.enabled=True; self.interval=900.0; self.task=None
        self.last_error=None; self.last_refresh=None; self.latest={}
        self.state=self._load()

    def _load(self):
        base={'meta_equity':100.0,'weights':{'v8':0.5,'v14':0.5,'cash':0.0},
              'last_engine_equity':{},'observations':[],'meta_high':100.0}
        try: base.update(json.loads(STATE.read_text(encoding='utf-8')))
        except Exception: pass
        return base

    def _save(self):
        STATE.parent.mkdir(parents=True,exist_ok=True)
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _engine_equity(status):
        try:return float((status.get('paper') or {}).get('equity') or 0.0)
        except Exception:return 0.0

    @staticmethod
    def _series_metrics(obs,key):
        vals=[float(x[key]) for x in obs if float(x.get(key) or 0)>0]
        if len(vals)<2:return {'ret':0.0,'dd':0.0,'vol':0.0,'win':0.5}
        rets=[vals[i]/vals[i-1]-1.0 for i in range(1,len(vals))]
        peak=vals[0]; dd=0.0
        for v in vals:
            peak=max(peak,v); dd=min(dd,v/peak-1.0)
        total=vals[-1]/vals[0]-1.0
        vol=statistics.pstdev(rets) if len(rets)>1 else 0.0
        win=sum(r>0 for r in rets)/len(rets)
        return {'ret':total,'dd':dd,'vol':vol,'win':win}

    def _choose_weights(self,obs,ok8,ok14):
        n=len(obs); phase='WARMUP' if n<24 else ('LEARNING' if n<96 else 'ADAPTIVE')
        if not ok8 and not ok14:return {'v8':0.0,'v14':0.0,'cash':1.0},phase,{}
        if ok8 and not ok14:return {'v8':1.0,'v14':0.0,'cash':0.0},phase,{}
        if ok14 and not ok8:return {'v8':0.0,'v14':1.0,'cash':0.0},phase,{}
        m8=self._series_metrics(obs[-96:],'v8'); m14=self._series_metrics(obs[-96:],'v14')
        metrics={'v8':m8,'v14':m14}
        if n<24:return {'v8':0.5,'v14':0.5,'cash':0.0},phase,metrics
        def score(m):
            return 100*m['ret']-70*abs(m['dd'])-25*m['vol']+2.0*(m['win']-0.5)
        s8=score(m8); s14=score(m14); cap=0.65 if n<96 else 0.75
        cash=0.5 if (m8['ret']<0 and m14['ret']<0) else 0.0
        tradable=1.0-cash; diff=abs(s8-s14)
        winner=min(cap,0.5+min(0.25,diff/20.0))
        if s8>=s14:w8=winner*tradable; w14=tradable-w8
        else:w14=winner*tradable; w8=tradable-w14
        if m8['dd']<=-0.10:w8=min(w8,0.10)
        if m14['dd']<=-0.10:w14=min(w14,0.10)
        used=w8+w14; cash=max(cash,1.0-used)
        return {'v8':round(w8,4),'v14':round(w14,4),'cash':round(cash,4)},phase,{**metrics,'scores':{'v8':round(s8,4),'v14':round(s14,4)}}

    async def refresh(self):
        try:
            s8=raven_engine.status(); s14=raven_universal_shadow.status()
            e8=self._engine_equity(s8); e14=self._engine_equity(s14); now=time.time()
            last=self.state.get('last_engine_equity') or {}; w=self.state.get('weights') or {'v8':.5,'v14':.5,'cash':0}
            meta=float(self.state.get('meta_equity') or 100.0)
            if float(last.get('v8') or 0)>0 and float(last.get('v14') or 0)>0:
                r8=e8/float(last['v8'])-1.0; r14=e14/float(last['v14'])-1.0
                meta*=max(0.01,1.0+float(w.get('v8',0))*r8+float(w.get('v14',0))*r14)
            obs=list(self.state.get('observations') or []); obs.append({'ts':now,'v8':e8,'v14':e14}); obs=obs[-2000:]
            nw,phase,metrics=self._choose_weights(obs,bool(s8.get('ok')),bool(s14.get('ok')))
            self.state.update({'meta_equity':meta,'weights':nw,'last_engine_equity':{'v8':e8,'v14':e14},'observations':obs,
                               'meta_high':max(float(self.state.get('meta_high') or 100),meta)})
            self._save(); self.last_refresh=now; self.last_error=None
            self.latest={'mode':'PAPER_SHADOW','phase':phase,'observation_count':len(obs),'weights':nw,'metrics':metrics,
                         'meta_equity':round(meta,6),'meta_return_pct':round((meta/100-1)*100,4),
                         'engines':{'v8':{'equity':e8,'return_pct':round((e8/100-1)*100,4),'ok':bool(s8.get('ok'))},
                                    'v14':{'equity':e14,'return_pct':round((e14/100-1)*100,4),'ok':bool(s14.get('ok'))}}}
        except Exception as exc:self.last_error=str(exc)[:300]
        return self.status()

    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW','interval_seconds':self.interval,
                'last_refresh':self.last_refresh,'last_error':self.last_error,'latest':self.latest}

    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh(); self.task=asyncio.create_task(self._loop(),name='raven-capital-allocator')

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None

    async def _loop(self):
        while True:
            await asyncio.sleep(max(300.0,self.interval)); await self.refresh()

raven_capital_allocator=RavenCapitalAllocator()
