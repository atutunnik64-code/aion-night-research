from __future__ import annotations
import asyncio,json,math,time
from pathlib import Path
import httpx,numpy as np,pandas as pd
from app.http_shared import SHARED_SSL_CONTEXT
ROOT=Path(__file__).parents[2];C1=ROOT/'data'/'raven_cache';C4=ROOT/'data'/'raven_unseen_cache'
STATE=ROOT/'data'/'raven_universal_paper_state.json'
ORIG=['ETHUSDT','SOLUSDT','XRPUSDT','LINKUSDT','AVAXUSDT','DOGEUSDT','ADAUSDT']
NEW=['BNBUSDT','LTCUSDT','BCHUSDT','DOTUSDT','TRXUSDT','NEARUSDT','APTUSDT']
SYMBOLS=['BTCUSDT']+ORIG+NEW; COST=.0011; FUND=.00005

class RavenUniversalShadow:
    def __init__(self):
        self.enabled=True;self.interval=900.0;self.task=None;self.last_refresh=None;self.last_error=None;self.latest={};self.state=self._load()
    def _load(self):
        x={'equity':100.0,'weights':{},'last_prices':{},'last_mark_ts':None,'last_rebalance_slot':None,'costs':0.0,'funding':0.0,'rebalance_count':0,'history':[]}
        try:x.update(json.loads(STATE.read_text(encoding='utf-8')))
        except Exception:pass
        return x
    def _save(self):
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    async def _latest4h(self,symbol):
        params={'symbol':symbol,'granularity':'4h','limit':80,'endTime':int(time.time()*1000)}
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT,timeout=15,follow_redirects=True) as c:
            r=await c.get('https://api.bitget.com/api/v2/spot/market/history-candles',params=params);r.raise_for_status();d=r.json()
        rows=d.get('data') or []
        if not rows:return pd.DataFrame()
        x=pd.DataFrame(rows,columns=['ts','open','high','low','close','base_vol','quote_vol','usdt_vol'])
        for col in x.columns[1:]:x[col]=pd.to_numeric(x[col],errors='coerce')
        x['ts']=pd.to_datetime(x.ts.astype('int64'),unit='ms',utc=True)
        return x.dropna().sort_values('ts').reset_index(drop=True)
    def _hist4h(self,symbol):
        p1=C1/f'{symbol}_1h_720d.csv';p4=C4/f'{symbol}_4h_720d.csv'
        if p1.exists():
            x=pd.read_csv(p1);x['ts']=pd.to_datetime(x.ts,utc=True);x=x.set_index('ts')
            y=x.resample('4h').agg({'open':'first','high':'max','low':'min','close':'last','base_vol':'sum'}).dropna().reset_index();return y
        x=pd.read_csv(p4);x['ts']=pd.to_datetime(x.ts,utc=True);return x[['ts','open','high','low','close','base_vol']]
    async def _frame4h(self,symbol):
        hist=self._hist4h(symbol)
        try:fresh=await self._latest4h(symbol)
        except Exception:fresh=pd.DataFrame()
        if fresh.empty:return hist
        cols=['ts','open','high','low','close','base_vol'];x=pd.concat([hist[cols],fresh[cols]],ignore_index=True)
        return x.drop_duplicates('ts',keep='last').sort_values('ts').reset_index(drop=True)
    @staticmethod
    def _resample8(x):
        y=x.set_index('ts').resample('8h').agg({'open':'first','high':'max','low':'min','close':'last','base_vol':'sum'}).dropna()
        now=pd.Timestamp.now(tz='UTC');return y[(y.index+pd.Timedelta(hours=8))<=now]
    @staticmethod
    def _features(frames):
        idx=frames['BTCUSDT'].index
        for s in SYMBOLS:idx=idx.intersection(frames[s].index)
        c=pd.DataFrame({s:frames[s].loc[idx,'close'] for s in SYMBOLS});r=c.pct_change().fillna(0)
        bull={};bear={};score={};vol={}
        for s in SYMBOLS:
            z=c[s];rr=r[s];f1=z.ewm(span=12,adjust=False).mean();s1=z.ewm(span=72,adjust=False).mean();m1=z.pct_change(42)
            f2=z.ewm(span=36,adjust=False).mean();s2=z.ewm(span=180,adjust=False).mean();m2=z.pct_change(84);v=rr.rolling(30).std().replace(0,np.nan)
            bull[s]=((f1>s1)&(m1>.01)&(f2>s2)&(m2>.02)).fillna(False);bear[s]=((f1<s1)&(m1<-.01)&(f2<s2)&(m2<-.02)).fillna(False)
            score[s]=((.45*m1+.55*m2)/(v*np.sqrt(84))).replace([np.inf,-np.inf],np.nan).fillna(-999);vol[s]=v
        return idx,c,r,bull,bear,score,vol
    @staticmethod
    def _target(idx,c,bull,bear,score,vol):
        i=-1;n=len(SYMBOLS);up=sum(bool(bull[s].iloc[i]) for s in SYMBOLS);dn=sum(bool(bear[s].iloc[i]) for s in SYMBOLS)
        uf=up/n;df=dn/n;btc_vol=vol['BTCUSDT'];med=btc_vol.rolling(180).median();vr=float(btc_vol.iloc[i]/max(float(med.iloc[i]),1e-9))
        rs=1.0 if vr<=1.25 else (.6 if vr<=1.75 else .3);shock=float(c.BTCUSDT.iloc[i]/c.BTCUSDT.iloc[i-3]-1);target=.45/math.sqrt(365*3);w={}
        if bool(bull['BTCUSDT'].iloc[i]) and uf>=.375 and shock>-.04:
            cand=sorted([(float(score[s].iloc[i]),s) for s in SYMBOLS if bool(bull[s].iloc[i])],reverse=True)[:2];bs=min(1.0,max(.45,uf/.75))
            for _,s in cand:w[s]=rs*bs*min(1.0,target/max(float(vol[s].iloc[i]),1e-6))/2
            regime='BULL'
        elif bool(bear['BTCUSDT'].iloc[i]) and df>=.50:
            bs=min(1.0,max(.5,df/.75));w['BTCUSDT']=-.5*rs*bs*min(1.0,target/max(float(vol['BTCUSDT'].iloc[i]),1e-6));regime='BEAR'
        else:regime='CASH'
        return w,regime,up,dn,idx[-1],rs,vr
    def _mark(self,prices,now):
        old=self.state.get('last_prices') or {};w=self.state.get('weights') or {};eq=float(self.state.get('equity') or 100);pnl=0.0
        if old:
            for s,x in w.items():
                op=float(old.get(s) or 0);np_=float(prices.get(s) or 0)
                if op>0 and np_>0:pnl+=float(x)*(np_/op-1)
            eq*=max(.01,1+pnl);hours=max(0.0,(now-float(self.state.get('last_mark_ts') or now))/3600)
            short=sum(abs(float(x)) for x in w.values() if float(x)<0);fund=eq*short*FUND*(hours/8);eq-=fund
            self.state['funding']=float(self.state.get('funding') or 0)+fund
        self.state['equity']=eq;self.state['last_prices']={k:float(v) for k,v in prices.items()};self.state['last_mark_ts']=now;return pnl
    def _rebalance(self,target,slot,bar_ts):
        old={k:float(v) for k,v in (self.state.get('weights') or {}).items()};keys=set(old)|set(target)
        turn=sum(abs(float(target.get(k,0))-float(old.get(k,0))) for k in keys);eq=float(self.state.get('equity') or 100);cost=eq*turn*COST;eq-=cost
        self.state['equity']=eq;self.state['weights']={k:float(v) for k,v in target.items() if abs(float(v))>1e-9};self.state['last_rebalance_slot']=int(slot)
        self.state['costs']=float(self.state.get('costs') or 0)+cost;self.state['rebalance_count']=int(self.state.get('rebalance_count') or 0)+1
        e={'ts':time.time(),'bar_ts':str(bar_ts),'equity':round(eq,6),'turnover':round(turn,6),'cost':round(cost,8),'weights':self.state['weights']}
        h=list(self.state.get('history') or []);h.append(e);self.state['history']=h[-200:];return e
    async def refresh(self):
        if not self.enabled:return self.status()
        try:
            raw=[]
            for s in SYMBOLS:
                raw.append(await self._frame4h(s));await asyncio.sleep(.12)
            fresh={s:x for s,x in zip(SYMBOLS,raw)};frames={s:self._resample8(x) for s,x in fresh.items()}
            if any(len(frames[s])<370 for s in SYMBOLS):raise RuntimeError('UNIVERSAL_HISTORY_WARMUP_INCOMPLETE')
            idx,c,r,bull,bear,score,vol=self._features(frames);target,regime,up,dn,bar_ts,rs,vr=self._target(idx,c,bull,bear,score,vol)
            prices={s:float(fresh[s].iloc[-1].close) for s in SYMBOLS};now=time.time();mtm=self._mark(prices,now);slot=int(pd.Timestamp(bar_ts).timestamp()//86400)
            prev_rs=float(self.state.get('last_risk_scale',1.0));prev_regime=self.state.get('last_regime');emergency=bool(rs<prev_rs-.05 or (prev_regime and regime!=prev_regime))
            reb=None
            if self.state.get('last_rebalance_slot')!=slot or emergency:reb=self._rebalance(target,slot,bar_ts)
            self.state['last_risk_scale']=rs;self.state['last_regime']=regime;self._save();self.last_refresh=now;self.last_error=None
            self.latest={'mode':'PAPER_SHADOW','strategy':'AION_RAVEN_UNIVERSAL_v14_LOCKED','regime':regime,'breadth_bull':up,'breadth_bear':dn,
                         'risk_scale':round(rs,3),'btc_vol_ratio':round(vr,3),'bar_ts':str(bar_ts),'target_weights':target,
                         'paper_equity':round(float(self.state['equity']),6),'paper_return_pct':round((float(self.state['equity'])/100-1)*100,4),
                         'mark_return_pct':round(mtm*100,5),'emergency_derisk':emergency,'rebalance':reb}
        except Exception as exc:self.last_error=str(exc)[:300]
        return self.status()
    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW','interval_seconds':self.interval,
                'last_refresh':self.last_refresh,'last_error':self.last_error,'latest':self.latest,
                'paper':{'equity':round(float(self.state.get('equity') or 100),6),'return_pct':round((float(self.state.get('equity') or 100)/100-1)*100,4),
                         'weights':self.state.get('weights') or {},'rebalance_count':self.state.get('rebalance_count',0),
                         'realized_costs':round(float(self.state.get('costs') or 0),8),'funding_costs':round(float(self.state.get('funding') or 0),8),
                         'history':(self.state.get('history') or [])[-30:]},
                'locked_config':{'timeframe':'8h','assets':SYMBOLS,'breadth_long_ratio':.375,'breadth_bear_ratio':.50,
                                 'research_recent_180d_pct':8.524,'research_recent_max_dd_pct':-6.787}}
    async def start(self):
        if self.task and not self.task.done():return
        asyncio.create_task(self.refresh(),name='initial-refresh-'+self.__class__.__name__);self.task=asyncio.create_task(self._loop(),name='raven-universal-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(max(300.0,self.interval));await self.refresh()

raven_universal_shadow=RavenUniversalShadow()
