from __future__ import annotations
import asyncio, json, math, time
from pathlib import Path
import httpx
import numpy as np
import pandas as pd
from app.http_shared import SHARED_SSL_CONTEXT

ROOT=Path(__file__).parents[2]
CACHE=ROOT/'data'/'raven_cache'
STATE=ROOT/'data'/'raven_paper_state.json'
SYMBOLS=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','LINKUSDT','AVAXUSDT','DOGEUSDT','ADAUSDT']
COST_SIDE=0.0011
SHORT_FUND_8H=0.00005
LONG_MIN=3
BEAR_MIN=4
SHORT_FRAC=0.50

class RavenEngine:
    def __init__(self):
        self.enabled=True; self.interval=300.0; self.task=None
        self.last_refresh=None; self.last_error=None; self.latest={}
        self.state=self._load_state()

    def _load_state(self):
        base={'equity':100.0,'weights':{},'last_prices':{},'last_mark_ts':None,
              'last_rebalance_slot':None,'realized_costs':0.0,'funding_costs':0.0,
              'rebalance_count':0,'history':[]}
        try:
            raw=json.loads(STATE.read_text(encoding='utf-8')); base.update(raw)
        except Exception: pass
        return base
    def _save(self):
        STATE.parent.mkdir(parents=True,exist_ok=True)
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')

    async def _latest_1h(self,symbol):
        params={'symbol':symbol,'granularity':'1h','limit':200,'endTime':int(time.time()*1000)}
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT,timeout=15,follow_redirects=True) as c:
            r=await c.get('https://api.bitget.com/api/v2/spot/market/history-candles',params=params)
            r.raise_for_status(); d=r.json()
        rows=d.get('data') or []
        if not rows:return pd.DataFrame()
        df=pd.DataFrame(rows,columns=['ts','open','high','low','close','base_vol','quote_vol','usdt_vol'])
        for col in df.columns[1:]:df[col]=pd.to_numeric(df[col],errors='coerce')
        df['ts']=pd.to_datetime(df['ts'].astype('int64'),unit='ms',utc=True)
        return df.dropna().sort_values('ts').reset_index(drop=True)

    async def _frame(self,symbol):
        path=CACHE/f'{symbol}_1h_720d.csv'
        hist=pd.read_csv(path) if path.exists() else pd.DataFrame()
        if not hist.empty: hist['ts']=pd.to_datetime(hist['ts'],utc=True)
        try:
            fresh=await self._latest_1h(symbol)
        except Exception:
            fresh=pd.DataFrame()
        if hist.empty and fresh.empty:raise RuntimeError(f'RAVEN_NO_DATA:{symbol}')
        df=pd.concat([hist,fresh],ignore_index=True) if not hist.empty else fresh
        df=df.drop_duplicates('ts',keep='last').sort_values('ts').reset_index(drop=True)
        if len(df)>20000:df=df.iloc[-20000:].reset_index(drop=True)
        return df

    @staticmethod
    def _resample(df):
        x=df.set_index('ts').sort_index()
        y=x.resample('8h').agg({'open':'first','high':'max','low':'min','close':'last','base_vol':'sum'}).dropna()
        now=pd.Timestamp.now(tz='UTC')
        y=y[(y.index+pd.Timedelta(hours=8))<=now]
        return y
    @staticmethod
    def _features(frames):
        idx=frames['BTCUSDT'].index
        for s in SYMBOLS: idx=idx.intersection(frames[s].index)
        close=pd.DataFrame({s:frames[s].loc[idx,'close'] for s in SYMBOLS})
        ret=close.pct_change().fillna(0)
        bull={}; bear={}; score={}; vol={}
        for s in SYMBOLS:
            c=close[s]; r=ret[s]
            f1=c.ewm(span=12,adjust=False).mean(); s1=c.ewm(span=72,adjust=False).mean(); m1=c.pct_change(42)
            f2=c.ewm(span=36,adjust=False).mean(); s2=c.ewm(span=180,adjust=False).mean(); m2=c.pct_change(84)
            v=r.rolling(30).std().replace(0,np.nan)
            bull[s]=((f1>s1)&(m1>.01)&(f2>s2)&(m2>.02)).fillna(False)
            bear[s]=((f1<s1)&(m1<-.01)&(f2<s2)&(m2<-.02)).fillna(False)
            score[s]=((.45*m1+.55*m2)/(v*np.sqrt(84))).replace([np.inf,-np.inf],np.nan).fillna(-999)
            vol[s]=v
        return idx,close,ret,bull,bear,score,vol

    @staticmethod
    def _target(idx,close,bull,bear,score,vol):
        if len(idx)<190:return {},'WARMING',0,0,None,1.0,1.0
        i=-1; up=sum(bool(bull[s].iloc[i]) for s in SYMBOLS); dn=sum(bool(bear[s].iloc[i]) for s in SYMBOLS)
        target_bar=.45/math.sqrt(365*3); weights={}
        btc_vol=vol['BTCUSDT']; med=btc_vol.rolling(180).median()
        vr=float(btc_vol.iloc[i]/max(float(med.iloc[i]),1e-9)) if np.isfinite(med.iloc[i]) else 1.0
        risk_scale=1.0 if vr<=1.25 else (.6 if vr<=1.75 else .3)
        shock=float(close['BTCUSDT'].iloc[i]/close['BTCUSDT'].iloc[i-3]-1.0)
        if bool(bull['BTCUSDT'].iloc[i]) and up>=LONG_MIN and shock>-0.04:
            cand=sorted([(float(score[s].iloc[i]),s) for s in SYMBOLS if bool(bull[s].iloc[i])],reverse=True)[:2]
            bs=min(1.0,max(.45,up/6.0))
            for _,s in cand:
                vv=max(float(vol[s].iloc[i]),1e-6); weights[s]=risk_scale*bs*min(1.0,target_bar/vv)/2.0
            regime='BULL'
        elif bool(bear['BTCUSDT'].iloc[i]) and dn>=BEAR_MIN:
            vv=max(float(vol['BTCUSDT'].iloc[i]),1e-6); bs=min(1.0,max(.5,dn/6.0))
            weights['BTCUSDT']=-risk_scale*SHORT_FRAC*min(1.0,target_bar/vv)*bs; regime='BEAR'
        else:regime='CASH'
        return weights,regime,up,dn,idx[-1],risk_scale,vr
    def _mark_to_market(self,prices,now_ts):
        old_prices=self.state.get('last_prices') or {}; weights=self.state.get('weights') or {}
        equity=float(self.state.get('equity') or 100.0); pnl_pct=0.0
        if old_prices:
            for s,w in weights.items():
                op=float(old_prices.get(s) or 0); np_=float(prices.get(s) or 0)
                if op>0 and np_>0:pnl_pct+=float(w)*(np_/op-1.0)
            equity*=max(0.01,1.0+pnl_pct)
            last=float(self.state.get('last_mark_ts') or now_ts); hours=max(0.0,(now_ts-last)/3600.0)
            short_gross=sum(abs(float(w)) for w in weights.values() if float(w)<0)
            funding=equity*short_gross*SHORT_FUND_8H*(hours/8.0)
            equity-=funding; self.state['funding_costs']=float(self.state.get('funding_costs') or 0)+funding
        self.state['equity']=equity; self.state['last_prices']={k:float(v) for k,v in prices.items()}
        self.state['last_mark_ts']=now_ts
        return pnl_pct

    def _rebalance(self,target,slot,bar_ts):
        old={k:float(v) for k,v in (self.state.get('weights') or {}).items()}
        keys=set(old)|set(target); turnover=sum(abs(float(target.get(k,0))-float(old.get(k,0))) for k in keys)
        equity=float(self.state.get('equity') or 100.0); cost=equity*turnover*COST_SIDE; equity-=cost
        self.state['equity']=equity; self.state['weights']={k:float(v) for k,v in target.items() if abs(float(v))>1e-9}
        self.state['last_rebalance_slot']=int(slot); self.state['realized_costs']=float(self.state.get('realized_costs') or 0)+cost
        self.state['rebalance_count']=int(self.state.get('rebalance_count') or 0)+1
        event={'ts':time.time(),'bar_ts':str(bar_ts),'equity':round(equity,6),'turnover':round(turnover,6),
               'cost':round(cost,8),'weights':self.state['weights']}
        hist=list(self.state.get('history') or []); hist.append(event); self.state['history']=hist[-200:]
        return event

    async def refresh(self):
        if not self.enabled:return self.status()
        try:
            raw=[]
            for s in SYMBOLS:
                raw.append(await self._frame(s)); await asyncio.sleep(.15)
            fresh={s:df for s,df in zip(SYMBOLS,raw)}; frames={s:self._resample(df) for s,df in fresh.items()}
            if any(len(frames[s])<190 for s in SYMBOLS):raise RuntimeError('RAVEN_HISTORY_WARMUP_INCOMPLETE')
            idx,close,ret,bull,bear,score,vol=self._features(frames)
            target,regime,up,dn,bar_ts,risk_scale,vol_ratio=self._target(idx,close,bull,bear,score,vol)
            prices={s:float(fresh[s].iloc[-1].close) for s in SYMBOLS}; now_ts=time.time()
            mtm=self._mark_to_market(prices,now_ts); slot=int(pd.Timestamp(bar_ts).timestamp()//86400)
            rebalance=None
            prev_risk=float(self.state.get('last_risk_scale',1.0)); prev_regime=self.state.get('last_regime')
            emergency_derisk=bool(risk_scale < prev_risk-0.05 or (prev_regime and regime!=prev_regime))
            if self.state.get('last_rebalance_slot')!=slot or emergency_derisk:
                rebalance=self._rebalance(target,slot,bar_ts)
            self.state['last_risk_scale']=risk_scale; self.state['last_regime']=regime
            self._save(); self.last_refresh=now_ts; self.last_error=None
            self.latest={'mode':'PAPER_ONLY','strategy':'AION_RAVEN_GOVERNOR_v8_LOCKED','regime':regime,
                         'breadth_bull':up,'breadth_bear':dn,'risk_scale':round(risk_scale,3),'btc_vol_ratio':round(vol_ratio,3),'bar_ts':str(bar_ts),'target_weights':target,
                         'paper_equity':round(float(self.state['equity']),6),'paper_return_pct':round((float(self.state['equity'])/100-1)*100,4),
                         'mark_return_pct':round(mtm*100,5),'emergency_derisk':emergency_derisk,'rebalance':rebalance,'prices':prices}
        except Exception as exc:self.last_error=str(exc)[:300]
        return self.status()
    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_ONLY','interval_seconds':self.interval,
                'last_refresh':self.last_refresh,'last_error':self.last_error,'latest':self.latest,
                'paper':{'equity':round(float(self.state.get('equity') or 100),6),
                         'return_pct':round((float(self.state.get('equity') or 100)/100-1)*100,4),
                         'weights':self.state.get('weights') or {},'rebalance_count':self.state.get('rebalance_count',0),
                         'realized_costs':round(float(self.state.get('realized_costs') or 0),8),
                         'funding_costs':round(float(self.state.get('funding_costs') or 0),8),
                         'history':(self.state.get('history') or [])[-30:]},
                'locked_config':{'timeframe':'8h','assets':SYMBOLS,'long_min':LONG_MIN,'bear_min':BEAR_MIN,
                                 'short_frac':SHORT_FRAC,'rebalance':'daily','leverage':False,
                                 'research_holdout_return_pct':17.0856,'research_holdout_max_dd_pct':-7.95}}

    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh(); self.task=asyncio.create_task(self._loop(),name='raven-paper-engine')

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None

    async def _loop(self):
        while True:
            await asyncio.sleep(max(60.0,self.interval)); await self.refresh()

    def reset_paper(self):
        self.state={'equity':100.0,'weights':{},'last_prices':{},'last_mark_ts':None,
                    'last_rebalance_slot':None,'realized_costs':0.0,'funding_costs':0.0,
                    'rebalance_count':0,'history':[]}
        self._save(); return self.status()

raven_engine=RavenEngine()