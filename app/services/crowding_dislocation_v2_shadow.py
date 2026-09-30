from __future__ import annotations
import asyncio,json,time,urllib.parse,urllib.request
from pathlib import Path
import numpy as np,pandas as pd
ROOT=Path(__file__).parents[2];STATE=ROOT/'data'/'crowding_dislocation_v2_shadow.json'
SYMS=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT']
COST=.0012;HOLD_BARS=12;K=1;GATE=.38447745709671594
FUTURE_START_MS=int(pd.Timestamp('2026-09-26 00:00:00+00:00').timestamp()*1000)
BF='https://fapi.binance.com';BY='https://api.bybit.com/v5/market/'

def get(url,params):
    q=urllib.parse.urlencode(params);req=urllib.request.Request(url+'?'+q,headers={'User-Agent':'AION-Crypto-Radar/0.11.58'})
    with urllib.request.urlopen(req,timeout=15) as r:return json.loads(r.read().decode('utf-8'))

def latest_closed_bucket():
    now=int(time.time()*1000);kl=get(BF+'/fapi/v1/klines',{'symbol':'BTCUSDT','interval':'8h','limit':3})
    rows=[x for x in kl if int(x[6])<now]
    if not rows:raise RuntimeError('NO_CLOSED_8H_BAR')
    return int((int(rows[-1][0])//(8*3600*1000))*(8*3600*1000))

def frame(sym):
    now=int(time.time()*1000);kl=get(BF+'/fapi/v1/klines',{'symbol':sym,'interval':'8h','limit':20})
    pr=[x for x in kl if int(x[6])<now];p=pd.DataFrame([{'ts':int(x[0]),'close':float(x[4])} for x in pr])
    oi=get(BY+'open-interest',{'category':'linear','symbol':sym,'intervalTime':'4h','limit':50})
    ol=oi.get('result',{}).get('list') or [];o=pd.DataFrame([{'ts':int(x['timestamp']),'oi':float(x['openInterest'])} for x in ol])
    ar=get(BY+'account-ratio',{'category':'linear','symbol':sym,'period':'4h','limit':50})
    al=ar.get('result',{}).get('list') or [];a=pd.DataFrame([{'ts':int(x['timestamp']),'ls':float(x['buyRatio'])/max(float(x['sellRatio']),1e-12)} for x in al])
    fr=get(BF+'/fapi/v1/fundingRate',{'symbol':sym,'limit':20});f=pd.DataFrame([{'ts':int(x['fundingTime']),'fund':float(x['fundingRate'])} for x in fr])
    for d,col in ((o,'oi'),(a,'ls'),(f,'fund')):
        if d.empty:raise RuntimeError(sym+'_'+col+'_EMPTY')
        d['bucket']=(d.ts//(8*3600*1000))*(8*3600*1000);d.sort_values('ts',inplace=True)
    p['bucket']=(p.ts//(8*3600*1000))*(8*3600*1000)
    z=p.set_index('bucket')[['close']].join(o.groupby('bucket').oi.last()).join(a.groupby('bucket').ls.last()).join(f.groupby('bucket').fund.last()).sort_index().ffill().dropna()
    z['ret24']=z.close.pct_change(3);z['oi24']=z.oi.pct_change(3);z['fund24']=z.fund.rolling(3).sum();z['ls_log']=np.log(z.ls.clip(lower=1e-9));z['ls_d24']=z.ls_log.diff(3)
    return z.dropna()

def target(frames,t):
    rows=[]
    for s,z in frames.items():
        if t not in z.index:continue
        r=z.loc[t];rows.append({'symbol':s,'ret24':float(r.ret24),'oi24':float(r.oi24),'fund24':float(r.fund24),'ls_log':float(r.ls_log),'ls_d24':float(r.ls_d24)})
    q=pd.DataFrame(rows)
    if len(q)<8:return {},None
    crowd=sum(q[x].rank(pct=True,method='average').to_numpy(float) for x in ['oi24','fund24','ls_log','ls_d24'])/4.0
    q['d']=q.ret24.rank(pct=True,method='average').to_numpy(float)-crowd;disp=float(q.d.std())
    if disp<GATE:return {},{'dispersion':disp,'active':False}
    q=q.sort_values('d');lo=str(q.iloc[0].symbol);hi=str(q.iloc[-1].symbol)
    return {hi:.5,lo:-.5},{'dispersion':disp,'active':True,'long':hi,'short':lo,'long_d':float(q.iloc[-1].d),'short_d':float(q.iloc[0].d)}

class CrowdingDislocationV2Shadow:
    def __init__(self):self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_refresh=None;self.state=self._load();self.latest={}
    @staticmethod
    def _blank():return {'equity':100.,'peak':100.,'max_dd_pct':0.,'weights':{},'last_prices':{},'last_bar_ts':None,'bars_since_rebalance':0,'observations':0,'active_observations':0,'history':[],'started_at':time.time()}
    def _load(self):
        x=self._blank()
        try:x.update(json.loads(STATE.read_text(encoding='utf-8')))
        except Exception:pass
        return x
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _turn(a,b):return sum(abs(float(b.get(k,0))-float(a.get(k,0))) for k in set(a)|set(b))
    def _mark(self,frames,t,prev):
        eq=float(self.state.get('equity') or 100.);w=self.state.get('weights') or {};gross=fund=0.
        for s,x in w.items():
            z=frames[s]
            if t in z.index and prev in z.index:
                gross+=float(x)*float(z.at[t,'close']/z.at[prev,'close']-1);fund+=float(x)*float(z.at[t,'fund'])
        eq*=max(.001,1+gross-fund);self.state['equity']=eq;self.state['peak']=max(float(self.state.get('peak') or eq),eq)
        self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0),(eq/self.state['peak']-1)*100)
        return gross-fund
    def _rebalance(self,nw):
        old=self.state.get('weights') or {};turn=self._turn(old,nw);eq=float(self.state.get('equity') or 100.);cost=eq*COST*turn
        eq-=cost;self.state['equity']=eq;self.state['weights']={k:float(v) for k,v in nw.items() if abs(float(v))>1e-12}
        self.state['peak']=max(float(self.state.get('peak') or eq),eq);self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0),(eq/self.state['peak']-1)*100)
        return turn,cost
    def _refresh_sync(self):
        last=self.state.get('last_bar_ts');probe=latest_closed_bucket()
        if last is not None and int(probe)<=int(last):
            self.latest={'bar_ts':int(last),'waiting_for':'new_closed_8h_bar','weights':self.state.get('weights') or {}};return
        frames={s:frame(s) for s in SYMS};common=None
        for z in frames.values():common=z.index if common is None else common.intersection(z.index)
        common=common.sort_values()
        if len(common)<6:raise RuntimeError('CROWDING_DISLOCATION_INSUFFICIENT_COMMON_BARS')
        latest=int(common[-1])
        if last is None:
            seeds=[int(t) for t in common if int(t)<FUTURE_START_MS]
            if not seeds:raise RuntimeError('CROWDING_NO_PRE_CUTOFF_SEED')
            last=seeds[-1];self.state['last_bar_ts']=last;self.state['bars_since_rebalance']=HOLD_BARS-1;self.state['weights']={};self.state['equity']=100.;self.state['peak']=100.;self.state['max_dd_pct']=0.
            self.state['last_prices']={s:float(frames[s].at[last,'close']) for s in SYMS}
        bars=[int(t) for t in common if int(t)>int(last) and int(t)>=FUTURE_START_MS]
        prev=int(last)
        for t in bars:
            net=self._mark(frames,t,prev);count=int(self.state.get('bars_since_rebalance') or 0)+1;info=None;turn=cost=0.;reb=False
            if count>=HOLD_BARS:
                nw,info=target(frames,t);turn,cost=self._rebalance(nw);count=0;reb=True
            self.state['bars_since_rebalance']=count;self.state['observations']=int(self.state.get('observations') or 0)+1
            self.state['active_observations']=int(self.state.get('active_observations') or 0)+int(bool(self.state.get('weights')))
            self.state['last_bar_ts']=t;self.state['last_prices']={s:float(frames[s].at[t,'close']) for s in SYMS}
            h=list(self.state.get('history') or []);h.append({'bar_ts':t,'equity':self.state['equity'],'net_pct':net*100,'rebalanced':reb,'turnover':turn,'cost':cost,'signal':info,'weights':self.state.get('weights') or {}});self.state['history']=h[-300:]
            prev=t
        self._save();self.latest={'bar_ts':self.state.get('last_bar_ts'),'weights':self.state.get('weights') or {},'bars_since_rebalance':self.state.get('bars_since_rebalance'),'initial_signal':self.state.get('initial_signal')}
    async def refresh(self):
        try:await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status()
    def status(self):
        eq=float(self.state.get('equity') or 100.);obs=int(self.state.get('observations') or 0);active=int(self.state.get('active_observations') or 0);dd=float(self.state.get('max_dd_pct') or 0)
        gate={'required_observations':24,'required_active_observations':8,'observations':obs,'active_observations':active,'return_pct':eq-100.0,'max_dd_pct':dd,'ready_for_review':bool(obs>=24 and active>=8 and eq>100.0 and dd>-8.0)}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW','strategy':'crowding_dislocation_bybit_v2','future_only':True,'promotion_eligible':False,'live_enabled':False,'grid':False,'martingale':False,'dca':False,
                'locked_config':{'hold_bars':HOLD_BARS,'topn_each_side':K,'direction':'follow','dispersion_gate':GATE,'cost_per_turnover':COST},'equity':round(eq,6),'return_pct':round(eq-100,4),'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0),4),
                'observation_count':obs,'active_observation_count':active,'future_gate':gate,'phase':'WARMUP' if obs<24 or active<3 else 'FUTURE_VALIDATION','latest':self.latest,'last_refresh':self.last_refresh,'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='crowding-dislocation-v2-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None;self._save()
    async def _loop(self):
        await asyncio.sleep(20)
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(60 if self.last_error else self.interval)

crowding_dislocation_v2_shadow=CrowdingDislocationV2Shadow()
