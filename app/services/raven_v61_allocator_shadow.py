from __future__ import annotations
import asyncio,json,math,time
from pathlib import Path

ROOT=Path(__file__).parents[2]
V47=ROOT/'data'/'raven_v47_shadow_state.json'
V55=ROOT/'data'/'raven_positioning_v55_shadow.json'
V60=ROOT/'data'/'raven_v60_cash_overlay_shadow.json'
STATE=ROOT/'data'/'raven_v61_allocator_shadow.json'
SWITCH_COST=0.0002

class RavenV61AllocatorShadow:
    def __init__(self):
        self.enabled=True; self.interval=300.0; self.task=None
        self.last_error=None; self.last_refresh=None; self.latest={}
        self.state=self._load()
    def _blank(self):
        return {'mode':'PAPER_SHADOW','equity':100.0,'peak':100.0,'max_dd_pct':0.0,
                'weights':{'v47':0.2,'v55':0.2,'v60':0.2,'cash':0.4},
                'last_component_equity':{},'last_bar_ts':None,'observations':0,'history':[],'costs':0.0}
    def _load(self):
        try:return json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:return self._blank()
    def _save(self):
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _read(path):
        return json.loads(path.read_text(encoding='utf-8'))
    def _series(self):
        a=self._read(V47); b=self._read(V55); c=self._read(V60)
        s47={str(x['bar_ts']):float(x['equity']) for x in (a.get('equity_history') or [])}
        s55={str(x['ts']):float(x['equity']) for x in (b.get('history') or [])}
        s60={str(x['bar_ts']):float(x['equity']) for x in (c.get('history') or [])}
        common=sorted(set(s47)&set(s55)&set(s60))
        return {'v47':s47,'v55':s55,'v60':s60},common
    @staticmethod
    def _stats(vals):
        if len(vals)<2:return {'ret':0.0,'dd':0.0,'vol':0.0,'n':max(0,len(vals)-1)}
        rets=[vals[i]/vals[i-1]-1.0 for i in range(1,len(vals))]
        peak=vals[0];dd=0.0
        for v in vals:
            peak=max(peak,v);dd=min(dd,v/peak-1.0)
        mean=sum(rets)/len(rets)
        var=sum((x-mean)**2 for x in rets)/len(rets)
        return {'ret':vals[-1]/vals[0]-1.0,'dd':dd,'vol':math.sqrt(var),'n':len(rets)}
    @staticmethod
    def _turn(a,b):
        return sum(abs(float(b.get(k,0))-float(a.get(k,0))) for k in ('v47','v55','v60','cash'))
    def _choose(self,series,ts):
        metrics={}; raw={}; confs=[]
        common=sorted(set(series['v47'])&set(series['v55'])&set(series['v60']))
        keys=[x for x in common if x<=ts][-25:]
        for k in ('v47','v55','v60'):
            vals=[series[k][x] for x in keys]
            m=self._stats(vals); metrics[k]=m
            conf=min(1.0,m['n']/12.0); confs.append(conf)
            edge=m['ret']-1.25*abs(m['dd'])-0.75*m['vol']
            raw[k]=max(0.0,edge)*conf if not (m['n']>=3 and m['ret']<0) else 0.0
        if max((m['n'] for m in metrics.values()),default=0)<3:
            return {'v47':0.2,'v55':0.2,'v60':0.2,'cash':0.4},metrics,'WARMUP'
        total=sum(raw.values()); avg_conf=sum(confs)/3.0
        if total<=1e-12:
            return {'v47':0.08,'v55':0.08,'v60':0.08,'cash':0.76},metrics,'DEFENSIVE'
        tradable=min(0.9,0.45+0.45*avg_conf); w={}
        for k in ('v47','v55','v60'):
            x=tradable*raw[k]/total
            if metrics[k]['dd']<=-0.08:x=min(x,0.05)
            w[k]=min(0.55,x)
        used=sum(w.values()); w['cash']=max(0.0,1.0-used)
        return w,metrics,'ADAPTIVE' if avg_conf>=0.5 else 'LEARNING'
    async def refresh(self):
        try:
            series,common=self._series()
            if not common:raise RuntimeError('V61_NO_COMMON_COMPONENT_BARS')
            latest=common[-1]; last=self.state.get('last_bar_ts')
            if last is None:
                self.state.update({'last_bar_ts':latest,'started_at':time.time(),
                    'last_component_equity':{k:series[k][latest] for k in ('v47','v55','v60')}})
                w,metrics,phase=self._choose(series,latest); self.state['weights']=w; self._save()
            else:
                pending=[x for x in common if x>str(last)]
                for ts in pending:
                    prev=self.state.get('last_component_equity') or {}; w=self.state.get('weights') or {}
                    port=0.0
                    for k in ('v47','v55','v60'):
                        p=float(prev.get(k) or series[k][ts]); cur=float(series[k][ts])
                        port+=float(w.get(k,0.0))*(cur/p-1.0 if p>0 else 0.0)
                    eq=float(self.state.get('equity') or 100.0)*max(0.01,1.0+port)
                    nw,metrics,phase=self._choose(series,ts); turn=self._turn(w,nw)
                    fee=eq*SWITCH_COST*turn; eq-=fee
                    peak=max(float(self.state.get('peak') or 100.0),eq); dd=(eq/peak-1.0)*100.0
                    self.state.update({'equity':eq,'peak':peak,'max_dd_pct':min(float(self.state.get('max_dd_pct') or 0.0),dd),
                        'weights':nw,'last_component_equity':{k:series[k][ts] for k in ('v47','v55','v60')},
                        'last_bar_ts':ts,'observations':int(self.state.get('observations') or 0)+1,
                        'costs':float(self.state.get('costs') or 0.0)+fee})
                    h=self.state.get('history') or []
                    h.append({'ts':ts,'equity':eq,'return_pct':eq-100.0,'drawdown_pct':dd,
                              'weights':nw,'metrics':metrics,'phase':phase,'switch_cost':fee})
                    self.state['history']=h[-500:]
                self._save()
            w,metrics,phase=self._choose(series,latest)
            eq=float(self.state.get('equity') or 100.0)
            self.latest={'mode':'PAPER_SHADOW','version':'v61','phase':phase,
                'equity':round(eq,6),'return_pct':round(eq-100.0,4),
                'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0.0),4),
                'weights':self.state.get('weights') or w,'metrics':metrics,
                'observations':int(self.state.get('observations') or 0),'last_bar_ts':self.state.get('last_bar_ts'),
                'component_latest':{k:series[k][latest] for k in ('v47','v55','v60')},
                'switch_costs':round(float(self.state.get('costs') or 0.0),6),
                'future_only':True,'promotion_eligible':False,'live_enabled':False,
                'grid':False,'martingale':False}
            self.last_refresh=time.time(); self.last_error=None
        except Exception as exc:
            self.last_error=str(exc)[:500]; self.last_refresh=time.time()
        return self.status()
    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,
                'interval_seconds':self.interval,'last_refresh':self.last_refresh,
                'last_error':self.last_error,'latest':self.latest}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh(); self.task=asyncio.create_task(self._loop(),name='raven-v61-allocator-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None; self._save()
    async def _loop(self):
        while True:
            await asyncio.sleep(max(300.0,self.interval))
            if self.enabled:await self.refresh()

raven_v61_allocator_shadow=RavenV61AllocatorShadow()
