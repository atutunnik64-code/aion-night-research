from __future__ import annotations
import asyncio,json,time
from pathlib import Path
import numpy as np,pandas as pd

ROOT=Path(__file__).parents[2]
PARENT_STATE=ROOT/'data'/'raven_smart_positioning_v61_shadow.json'
DH=ROOT/'data'/'derivatives_history'
LIVE=ROOT/'data'/'positioning_history.jsonl'
STATE=ROOT/'data'/'raven_smartpos_g2_drop_toppos_shadow.json'
SYMS=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT']
FEATS=['ret8','oi8','fund','global_d','topacct_d','taker_log']
COST=.0012; HORIZON=6; ALPHA=10.0

class RavenSmartposG2DropTopposShadow:
    def __init__(self):
        self.enabled=True;self.interval=900.0;self.task=None
        self.last_error=None;self.last_refresh=None;self.latest={}
        self.refresh_lock=asyncio.Lock();self.state=self._load()
    @staticmethod
    def _blank():
        return {'mode':'PAPER_SHADOW','future_only':True,'equity':100.0,'peak':100.0,
                'max_dd_pct':0.0,'weights':{},'observations':0,'history':[],
                'last_bar_ts':None,'last_rebalance_ts':None,'started_at':time.time()}
    def _load(self):
        x=self._blank()
        try:x.update(json.loads(STATE.read_text(encoding='utf-8')))
        except Exception:pass
        return x
    def _save(self):
        STATE.parent.mkdir(parents=True,exist_ok=True)
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _series(path,tscol,valcol,mean=False):
        d=pd.read_csv(path);d['ts']=pd.to_datetime(d[tscol],unit='ms',utc=True)
        d[valcol]=pd.to_numeric(d[valcol],errors='coerce')
        s=d.set_index('ts')[valcol].sort_index()
        return s.resample('8h').mean() if mean else s.resample('8h').last()
    def _hist_symbol(self,s):
        p=pd.read_csv(DH/f'binance_price8h_{s}.csv')
        p['ts']=pd.to_datetime(p.close_time,unit='ms',utc=True).dt.floor('8h')
        p['close']=pd.to_numeric(p.close,errors='coerce');p=p.set_index('ts').sort_index()
        z=pd.DataFrame(index=p.index);z['close']=p.close
        z['oi']=self._series(DH/f'binance_oi_1h_{s}.csv','timestamp','sumOpenInterestValue').reindex(z.index,method='ffill')
        z['fund']=self._series(DH/f'binance_funding_{s}.csv','fundingTime','fundingRate').reindex(z.index,method='ffill')
        z['global']=self._series(DH/f'binance_pos_global_{s}.csv','timestamp','longShortRatio').reindex(z.index,method='ffill')
        z['topacct']=self._series(DH/f'binance_pos_top_account_{s}.csv','timestamp','longShortRatio').reindex(z.index,method='ffill')
        z['toppos']=self._series(DH/f'binance_pos_top_position_{s}.csv','timestamp','longShortRatio').reindex(z.index,method='ffill')
        z['taker']=self._series(DH/f'binance_pos_taker_{s}.csv','timestamp','buySellRatio',True).reindex(z.index,method='ffill')
        return z
    @staticmethod
    def _live_rows():
        out={s:[] for s in SYMS}
        if not LIVE.exists():return out
        for line in LIVE.read_text(encoding='utf-8').splitlines():
            try:r=json.loads(line);ts=pd.Timestamp(float(r['ts']),unit='s',tz='UTC');data=r.get('symbols') or {}
            except Exception:continue
            for s in SYMS:
                x=data.get(s) or {}
                try:
                    out[s].append((ts,float(x.get('price') or 0),float((x.get('oi') or {}).get('sumOpenInterestValue') or 0),
                        float(x.get('funding_rate') or 0),float((x.get('global_ls') or {}).get('longShortRatio') or 0),
                        float((x.get('top_account_ls') or {}).get('longShortRatio') or 0),float((x.get('top_position_ls') or {}).get('longShortRatio') or 0),
                        float((x.get('taker_ls') or {}).get('buySellRatio') or 0)))
                except Exception:pass
        return out
    def _dataset(self):
        live=self._live_rows();frames={};closed_before=pd.Timestamp.now(tz='UTC').floor('8h')
        for s in SYMS:
            z=self._hist_symbol(s)
            if live.get(s):
                q=pd.DataFrame(live[s],columns=['ts','close','oi','fund','global','topacct','toppos','taker']).set_index('ts').sort_index()
                q=q[q.close>0];last=q[['close','oi','fund','global','topacct','toppos']].resample('8h').last()
                last['taker']=q.taker.resample('8h').mean();q=last[last.index<closed_before]
                z=pd.concat([z,q]).groupby(level=0).last().sort_index()
            z=z[z.index<closed_before];z['ret8']=z.close.pct_change();z['oi8']=z.oi.pct_change()
            z['target48']=z.close.shift(-HORIZON)/z.close-1
            for name in ('global','topacct','toppos'):z[name+'_d']=np.log(z[name].replace(0,np.nan)).diff()
            z['taker_log']=np.log(z.taker.replace(0,np.nan));frames[s]=z
        common=None
        for z in frames.values():common=z.index if common is None else common.intersection(z.index)
        return frames,common.sort_values()
    @staticmethod
    def _fit(train,cur):
        X=train[FEATS].to_numpy(float);y=train.yrel.to_numpy(float);Xc=cur[FEATS].to_numpy(float)
        mu=X.mean(0);sd=X.std(0);sd[sd<1e-9]=1.;X=(X-mu)/sd;Xc=(Xc-mu)/sd;yc=y-y.mean()
        b=np.linalg.solve(X.T@X+ALPHA*np.eye(len(FEATS)),X.T@yc);return Xc@b
    def _signal(self,frames,t):
        known=t-pd.Timedelta(hours=48);rows=[];cur=[]
        for s,z in frames.items():
            h=z[(z.index>=known-pd.Timedelta(days=7))&(z.index<=known)][FEATS+['target48']].dropna().copy()
            if len(h):h['symbol']=s;h['ts']=h.index;rows.append(h.reset_index(drop=True))
            if t in z.index:
                r=z.loc[t];cur.append({'symbol':s,**{f:r[f] for f in FEATS}})
        if not rows:return None
        tr=pd.concat(rows,ignore_index=True).dropna();cu=pd.DataFrame(cur).dropna()
        if len(tr)<180 or len(cu)<8:return None
        tr['yrel']=tr.target48-tr.groupby('ts').target48.transform('mean')
        cu=cu.assign(pred=self._fit(tr,cu)).sort_values('pred')
        k=min(3,max(1,len(cu)//3));w={s:0. for s in SYMS};shorts=cu.head(k);longs=cu.tail(k)
        for s in longs.symbol:w[s]=.5/k
        for s in shorts.symbol:w[s]=-.5/k
        return {'ts':str(t),'weights':w,'longs':list(longs.symbol),'shorts':list(shorts.symbol),'train_rows':int(len(tr))}
    def _mark(self,frames,t,prev):
        w={s:float((self.state.get('weights') or {}).get(s,0)) for s in SYMS};gross=fund=0.
        for s,z in frames.items():
            if t not in z.index or prev not in z.index:continue
            gross+=w[s]*float(z.at[t,'close']/z.at[prev,'close']-1)
            fv=float(z.at[t,'fund']) if np.isfinite(z.at[t,'fund']) else 0.0
            fund+=w[s]*fv
        return gross-fund,gross,fund

    def _refresh_sync(self):
        try:
            frames,common=self._dataset()
            if len(common)<60:raise RuntimeError('SMARTPOS_INSUFFICIENT_HISTORY')
            latest=common[-1];last_raw=self.state.get('last_bar_ts')
            if not last_raw:
                sig=self._signal(frames,latest)
                if not sig:raise RuntimeError('SMARTPOS_INITIAL_SIGNAL_UNAVAILABLE')
                self.state.update({'weights':sig['weights'],'last_signal':sig,'last_bar_ts':str(latest),
                                   'last_rebalance_ts':str(latest),'started_at':time.time()})
            else:
                last=pd.Timestamp(last_raw);bars=[t for t in common if t>last]
                for t in bars:
                    net,gross,funding=self._mark(frames,t,last)
                    eq=float(self.state.get('equity') or 100.)*max(.001,1+net);turn=0.;reb=False
                    lr=pd.Timestamp(self.state.get('last_rebalance_ts') or last)
                    if t-lr>=pd.Timedelta(hours=48):
                        sig=self._signal(frames,t)
                        if sig:
                            old=self.state.get('weights') or {}
                            turn=sum(abs(float(sig['weights'].get(s,0))-float(old.get(s,0))) for s in SYMS)
                            eq*=max(.001,1-COST*turn);self.state['weights']=sig['weights']
                            self.state['last_signal']=sig;self.state['last_rebalance_ts']=str(t);reb=True
                    self.state['equity']=eq;self.state['peak']=max(float(self.state.get('peak') or 100.),eq)
                    self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0.),(eq/float(self.state['peak'])-1)*100)
                    self.state['observations']=int(self.state.get('observations') or 0)+1
                    self.state['last_bar_ts']=str(t)
                    h=list(self.state.get('history') or [])
                    h.append({'bar_ts':str(t),'equity':round(eq,6),'net_pct':round(net*100,6),
                              'gross_pct':round(gross*100,6),'funding_pct':round(funding*100,6),
                              'turnover':round(turn,6),'rebalanced':reb})
                    self.state['history']=h[-300:];last=t
            self._save();self.last_error=None;self.last_refresh=time.time()
            eq=float(self.state.get('equity') or 100.)
            self.latest={'strategy':'smartpos_g2_drop_toppos','bar_ts':self.state.get('last_bar_ts'),
                         'paper_equity':eq,'paper_return_pct':(eq/100.-1.)*100.,
                         'weights':self.state.get('weights') or {},'last_signal':self.state.get('last_signal'),
                         'observations':int(self.state.get('observations') or 0),
                         'closed_bar_only':True,'cost_per_turnover':COST}
        except Exception as exc:
            self.last_error=str(exc)[:300];self.last_refresh=time.time()
        return self.status()

    async def refresh(self):
        async with self.refresh_lock:
            return await asyncio.to_thread(self._refresh_sync)

    def _parent_overlap_gate(self):
        from app.services.raven_smart_positioning_v61_shadow import raven_smart_positioning_v61_shadow
        ph={str(x.get('bar_ts')):float(x.get('net_pct') or 0) for x in (raven_smart_positioning_v61_shadow.state.get('history') or []) if x.get('bar_ts')}
        ch={str(x.get('bar_ts')):float(x.get('net_pct') or 0) for x in (self.state.get('history') or []) if x.get('bar_ts')}
        common=sorted(set(ph)&set(ch)); a=[ch[t]-ph[t] for t in common]
        n=len(a); pos=sum(x>0 for x in a)/n if n else 0.0
        med=float(np.median(a)) if a else 0.0; total=float(sum(a)) if a else 0.0
        return {'required_bars':12,'completed':n,'positive_alpha_ratio':round(pos,6),'median_alpha_pp':round(med,6),'sum_alpha_pp':round(total,6),'ready':bool(n>=12 and pos>=.58 and med>0 and total>0)}

    def _overlap_gate(self):
        try: parent=json.loads(PARENT_STATE.read_text(encoding='utf-8'))
        except Exception: parent={}
        ph={str(x.get('bar_ts')):x for x in (parent.get('history') or [])}
        ch={str(x.get('bar_ts')):x for x in (self.state.get('history') or [])}
        common=sorted(set(ph)&set(ch)); a=[]
        for t in common:
            a.append(float(ch[t].get('net_pct') or 0)-float(ph[t].get('net_pct') or 0))
        n=len(a); pos=(sum(x>0 for x in a)/n) if n else 0.0
        med=float(np.median(a)) if a else 0.0; total=float(sum(a))
        return {'required_common_bars':12,'common_bars':n,'positive_alpha_ratio':pos,'median_alpha_pp':med,'total_alpha_pp':total,'ready':bool(n>=12 and pos>=.58 and med>0 and total>0)}

    def status(self):
        eq=float(self.state.get('equity') or 100.);obs=int(self.state.get('observations') or 0);gate=self._overlap_gate()
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW',
                'strategy':'smartpos_g2_drop_toppos','future_only':True,'closed_bar_only':True,
                'promotion_eligible':False,'live_enabled':False,'grid':False,'martingale':False,'dca':False,
                'locked_config':{'horizon_bars':HORIZON,'ridge_alpha':ALPHA,'cost_per_turnover':COST,'gross':1.0},
                'equity':round(eq,6),'return_pct':round((eq/100.-1.)*100.,4),
                'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0.),4),
                'future_gate':self._parent_overlap_gate(),
                'observation_count':obs,'future_gate':gate,'phase':'WARMUP' if obs<24 else 'FUTURE_VALIDATION',
                'latest':self.latest,'last_refresh':self.last_refresh,'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='raven-smartpos-g2-drop-toppos-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None;self._save()
    async def _loop(self):
        while True:
            await self.refresh()
            await asyncio.sleep(max(300.0,self.interval))

raven_smartpos_g2_drop_toppos_shadow=RavenSmartposG2DropTopposShadow()

