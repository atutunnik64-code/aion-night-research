from __future__ import annotations
import asyncio,json,time
from pathlib import Path
import numpy as np,pandas as pd

ROOT=Path(__file__).parents[2]
DH=ROOT/'data'/'derivatives_history'
LIVE=ROOT/'data'/'positioning_history.jsonl'
STATE=ROOT/'data'/'raven_positioning_v55_shadow.json'
SYMS=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT']
COST=.0012; HORIZON=6; ALPHA=5.0

class RavenPositioningV55Shadow:
    def __init__(self):
        self.enabled=True; self.interval=900.; self.task=None; self.last_error=None; self.last_refresh=None
        self.state=self._load(); self.latest={}
    def _load(self):
        try:return json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:return {'mode':'PAPER_SHADOW','equity':100.0,'peak':100.0,'max_dd_pct':0.0,'weights':{},'observations':0,'history':[]}
    def _save(self):
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _hist_symbol(s):
        p=pd.read_csv(DH/f'binance_price8h_{s}.csv'); p['ts']=pd.to_datetime(p.close_time,unit='ms',utc=True).dt.floor('8h')
        p['close']=pd.to_numeric(p.close,errors='coerce'); p=p.set_index('ts').sort_index()
        oi=pd.read_csv(DH/f'binance_oi_1h_{s}.csv'); oi['ts']=pd.to_datetime(oi.timestamp,unit='ms',utc=True).dt.floor('8h')
        oi['oi']=pd.to_numeric(oi.sumOpenInterestValue,errors='coerce'); oi=oi.groupby('ts').oi.last()
        f=pd.read_csv(DH/f'binance_funding_{s}.csv'); f['ts']=pd.to_datetime(f.fundingTime,unit='ms',utc=True).dt.floor('8h')
        f['fund']=pd.to_numeric(f.fundingRate,errors='coerce'); f=f.groupby('ts').fund.last()
        z=pd.DataFrame(index=p.index); z['close']=p.close; z['oi']=oi.reindex(z.index,method='ffill'); z['fund']=f.reindex(z.index,method='ffill')
        return z

    @staticmethod
    def _live_frames():
        out={s:[] for s in SYMS}
        if not LIVE.exists():return out
        for line in LIVE.read_text(encoding='utf-8').splitlines():
            try:r=json.loads(line); ts=pd.Timestamp(float(r['ts']),unit='s',tz='UTC'); sy=r.get('symbols') or {}
            except Exception:continue
            for s in SYMS:
                x=sy.get(s) or {}; oi=x.get('oi') or {}
                try:out[s].append((ts,float(x.get('price') or 0),float(oi.get('sumOpenInterestValue') or 0),float(x.get('funding_rate') or 0)))
                except Exception:pass
        return out
    def _dataset(self):
        live=self._live_frames(); frames={}
        now=pd.Timestamp.now(tz='UTC'); latest_closed=now.floor('8h')-pd.Timedelta(hours=8)
        for s in SYMS:
            z=self._hist_symbol(s)
            if live.get(s):
                q=pd.DataFrame(live[s],columns=['ts','close','oi','fund']).set_index('ts').sort_index()
                q=q[q.close>0]; q=q.resample('8h').last(); q=q[q.index<=latest_closed]
                if len(q):z=pd.concat([z,q]).groupby(level=0).last().sort_index()
            z=z[z.index<=latest_closed]; z['ret8']=z.close.pct_change(); z['oi8']=z.oi.pct_change(); z['target48']=z.close.shift(-HORIZON)/z.close-1
            frames[s]=z
        common=None
        for z in frames.values():common=z.index if common is None else common.intersection(z.index)
        common=common.sort_values(); return {s:z.reindex(common) for s,z in frames.items()},common

    @staticmethod
    def _fit(train,cur):
        X=train[['ret8','oi8']].to_numpy(float); y=train['yrel'].to_numpy(float); Xc=cur[['ret8','oi8']].to_numpy(float)
        mu=X.mean(0); sd=X.std(0); sd[sd<1e-9]=1.; X=(X-mu)/sd; Xc=(Xc-mu)/sd; yc=y-y.mean()
        b=np.linalg.solve(X.T@X+ALPHA*np.eye(2),X.T@yc); return Xc@b
    def _signal(self,frames,t):
        known=t-pd.Timedelta(hours=8*HORIZON); rows=[]; current=[]
        for s,z in frames.items():
            hist=z[(z.index>=known-pd.Timedelta(days=7))&(z.index<=known)][['ret8','oi8','target48']].dropna().copy()
            if len(hist): hist['symbol']=s; hist['ts']=hist.index; rows.append(hist.reset_index(drop=True))
            if t in z.index:
                r=z.loc[t]; current.append({'symbol':s,'ret8':r.ret8,'oi8':r.oi8})
        if not rows:return None
        train=pd.concat(rows,ignore_index=True).dropna(); cur=pd.DataFrame(current).dropna()
        if len(train)<150 or len(cur)<8:return None
        train['yrel']=train.target48-train.groupby('ts').target48.transform('mean')
        pred=self._fit(train,cur); cur=cur.assign(pred=pred).sort_values('pred'); k=max(1,int(round(len(cur)*.30)))
        weights={s:0.0 for s in SYMS}; shorts=cur.head(k); longs=cur.tail(k)
        for s in longs.symbol:weights[s]=.5/len(longs)
        for s in shorts.symbol:weights[s]=-.5/len(shorts)
        return {'ts':str(t),'weights':weights,'longs':list(longs.symbol),'shorts':list(shorts.symbol),'train_rows':int(len(train))}

    def _mark_bar(self,frames,t,prev_t):
        w={s:float((self.state.get('weights') or {}).get(s,0)) for s in SYMS}; gross=0.; funding=0.
        for s,z in frames.items():
            if t not in z.index or prev_t not in z.index:continue
            gross+=w[s]*float(z.at[t,'close']/z.at[prev_t,'close']-1); funding+=w[s]*float(z.at[t,'fund'] or 0)
        return gross-funding,gross,funding
    async def refresh(self):
        try:
            frames,common=self._dataset()
            if len(common)<60:raise RuntimeError('INSUFFICIENT_POSITIONING_HISTORY')
            latest=common[-1]; last_raw=self.state.get('last_bar_ts')
            if not last_raw:
                sig=self._signal(frames,latest)
                if not sig:raise RuntimeError('INITIAL_SIGNAL_UNAVAILABLE')
                self.state['weights']=sig['weights']; self.state['last_signal']=sig; self.state['last_bar_ts']=str(latest)
                self.state['last_rebalance_ts']=str(latest); self.state['started_at']=time.time(); self._save()
            else:
                last=pd.Timestamp(last_raw); bars=[t for t in common if t>last]
                for t in bars:
                    net,gross,funding=self._mark_bar(frames,t,last); eq=float(self.state.get('equity') or 100.)*(1+net)
                    reb=False; turn=0.; sig=None; lr=pd.Timestamp(self.state.get('last_rebalance_ts') or last)
                    if t-lr>=pd.Timedelta(hours=48):
                        sig=self._signal(frames,t)
                        if sig:
                            old=self.state.get('weights') or {}; turn=sum(abs(float(sig['weights'].get(s,0))-float(old.get(s,0))) for s in SYMS)
                            eq*=max(.001,1-COST*turn); self.state['weights']=sig['weights']; self.state['last_signal']=sig
                            self.state['last_rebalance_ts']=str(t); reb=True
                    self.state['equity']=eq; self.state['peak']=max(float(self.state.get('peak') or 100.),eq)
                    self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0.),(eq/float(self.state['peak'])-1)*100)
                    self.state['observations']=int(self.state.get('observations') or 0)+1; self.state['last_bar_ts']=str(t)
                    h=self.state.get('history') or []; h.append({'ts':str(t),'net_ret_pct':net*100,'gross_ret_pct':gross*100,'funding_pct':funding*100,'turnover':turn,'rebalanced':reb,'equity':eq})
                    self.state['history']=h[-300:]; last=t
                self._save()
            self.latest={'mode':'PAPER_SHADOW','version':'v55','equity':round(float(self.state.get('equity') or 100),6),
                'return_pct':round(float(self.state.get('equity') or 100)-100,4),'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0),4),
                'observations':int(self.state.get('observations') or 0),'last_bar_ts':self.state.get('last_bar_ts'),
                'last_signal':self.state.get('last_signal'),'weights':self.state.get('weights') or {},
                'future_only':True,'promotion_eligible':False,'live_enabled':False,'grid':False,'martingale':False}
            self.last_refresh=time.time(); self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:300]
        return self.status()
    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'last_refresh':self.last_refresh,'last_error':self.last_error,'latest':self.latest}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='raven-positioning-v55-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        while True:
            await self.refresh(); await asyncio.sleep(max(300.,self.interval))

raven_positioning_v55_shadow=RavenPositioningV55Shadow()
