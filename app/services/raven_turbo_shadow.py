from __future__ import annotations
import asyncio,json,math,time
from pathlib import Path
import httpx,numpy as np,pandas as pd
from app.http_shared import SHARED_SSL_CONTEXT

ROOT=Path(__file__).parents[2]
CACHE=ROOT/'data'/'raven_turbo_highbeta_cache'
STATE=ROOT/'data'/'raven_turbo_paper_state.json'
SYMBOLS=['BTCUSDT','ETHUSDT','SOLUSDT','SUIUSDT','NEARUSDT','APTUSDT','INJUSDT','TIAUSDT','SEIUSDT','WIFUSDT','PEPEUSDT','FLOKIUSDT','ARBUSDT','OPUSDT','AAVEUSDT','RUNEUSDT','JUPUSDT']
COST=.0012; FUND=.00005
DEFAULT_CFG={'long_mult':3.0,'short_mult':2.0,'bull_frac':.35,'bear_frac':.70,'btc_long_m':.02,'btc_short_m':-.02,'slots':1,'vol1':1.25,'vol2':1.75,'cut1':.6,'cut2':.3,'gross_cap':3.0}
CHAMPION_RUNTIME=ROOT/'data'/'raven_champion_runtime.json'
CHAMPION_ARCHIVE=ROOT/'data'/'raven_champion_paper_archive.json'

class RavenTurboShadow:
    def __init__(self):
        self.enabled=True;self.interval=900.0;self.task=None;self.last_refresh=None;self.last_error=None;self.latest={};self.state=self._load()
    def _load(self):
        x={'equity':100.0,'weights':{},'last_prices':{},'last_mark_ts':None,'last_rebalance_slot':None,'costs':0.0,'funding':0.0,'rebalance_count':0,'history':[],'observation_count':0,'equity_history':[]}
        try:x.update(json.loads(STATE.read_text(encoding='utf-8')))
        except Exception:pass
        return x
    def _save(self):
        STATE.parent.mkdir(parents=True,exist_ok=True);STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')

    def _champion(self):
        base={'champion_name':'v24','config':dict(DEFAULT_CFG),'source':'BOOTSTRAP_V24'}
        try:
            x=json.loads(CHAMPION_RUNTIME.read_text(encoding='utf-8'))
            if isinstance(x,dict): base.update(x)
        except Exception: pass
        cfg=base.get('config') or {}
        req=set(DEFAULT_CFG)
        if not req.issubset(cfg): base['config']=dict(DEFAULT_CFG);base['champion_name']='v24'
        return base

    def _switch_champion_if_needed(self,champ):
        name=str(champ.get('champion_name') or 'v24');cfg=dict(champ.get('config') or DEFAULT_CFG)
        old=self.state.get('champion_name')
        if old is None:
            self.state['champion_name']=name;self.state['champion_config']=cfg;return
        if old==name: return
        rows=[]
        try: rows=json.loads(CHAMPION_ARCHIVE.read_text(encoding='utf-8'))
        except Exception: pass
        if not isinstance(rows,list): rows=[]
        rows.append({'archived_at':time.time(),'champion_name':old,**self.state})
        CHAMPION_ARCHIVE.write_text(json.dumps(rows[-20:],ensure_ascii=False,indent=2),encoding='utf-8')
        self.state={'champion_name':name,'champion_config':cfg,'equity':100.0,'weights':{},'last_prices':{},'last_mark_ts':None,'last_rebalance_slot':None,'costs':0.0,'funding':0.0,'rebalance_count':0,'history':[],'observation_count':0,'equity_history':[]}

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
        p=CACHE/f'{symbol}_4h_720d.csv'
        x=pd.read_csv(p);x['ts']=pd.to_datetime(x.ts,utc=True)
        return x[['ts','open','high','low','close','base_vol']]
    async def _frame4h(self,symbol):
        hist=self._hist4h(symbol);fresh=pd.DataFrame();last_err=None
        for delay in (0.0,0.5,1.5,3.0):
            if delay:await asyncio.sleep(delay)
            try:
                fresh=await self._latest4h(symbol)
                if not fresh.empty:break
            except Exception as exc:last_err=exc
        if fresh.empty:
            last_ts=pd.to_datetime(hist['ts'].max(),utc=True)
            age_h=max(0.0,(pd.Timestamp.now(tz='UTC')-last_ts).total_seconds()/3600.0)
            if age_h>12.0:raise RuntimeError(f'TURBO_FRESH_STALE:{symbol}:{age_h:.1f}h:{last_err}')
            return hist
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
        vol=r.rolling(30).std().replace(0,np.nan);m21=c.pct_change(21);m63=c.pct_change(63)
        e12=c.ewm(span=12,adjust=False).mean();e72=c.ewm(span=72,adjust=False).mean();e36=c.ewm(span=36,adjust=False).mean();e180=c.ewm(span=180,adjust=False).mean()
        bull=((e12>e72)&(m21>.01)&(e36>e180)&(m63>.02)).fillna(False)
        bear=((e12<e72)&(m21<-.01)&(e36<e180)&(m63<-.02)).fillna(False)
        score=((.35*m21+.65*m63)/(vol*np.sqrt(63))).replace([np.inf,-np.inf],np.nan).fillna(-999)
        return idx,c,r,bull,bear,score,vol
    @staticmethod
    def _target(idx,c,bull,bear,score,vol,cfg):
        i=-1;n=len(SYMBOLS);up=int(bull.iloc[i].sum());dn=int(bear.iloc[i].sum());uf=up/n;df=dn/n
        btc_v=vol.BTCUSDT;med=btc_v.rolling(180).median();vr=float(btc_v.iloc[i]/max(float(med.iloc[i]),1e-9)) if np.isfinite(med.iloc[i]) else 1.0
        rs=1.0 if vr<=cfg['vol1'] else (cfg['cut1'] if vr<=cfg['vol2'] else cfg['cut2'])
        btc_m=float(c.BTCUSDT.iloc[i]/c.BTCUSDT.iloc[i-9]-1.0);target=.45/math.sqrt(365*3);w={};regime='CASH'
        if bool(bull.BTCUSDT.iloc[i]) and uf>=cfg['bull_frac'] and btc_m>=cfg['btc_long_m']:
            cand=sorted([(float(score[s].iloc[i]),s) for s in SYMBOLS if s not in ('BTCUSDT','ETHUSDT') and bool(bull[s].iloc[i])],reverse=True)[:1]
            for _,s in cand:
                w[s]=rs*cfg['long_mult']*min(1.0,target/max(float(vol[s].iloc[i]),1e-6))
            regime='BULL_TURBO' if w else 'CASH'
        elif bool(bear.BTCUSDT.iloc[i]) and df>=cfg['bear_frac'] and btc_m<=cfg['btc_short_m']:
            for s,m in [('BTCUSDT',.65),('ETHUSDT',.35)]:
                w[s]=-rs*cfg['short_mult']*m*min(1.0,target/max(float(vol[s].iloc[i]),1e-6))
            regime='BEAR_TURBO'
        gross=sum(abs(float(x)) for x in w.values())
        if gross>cfg['gross_cap']:
            scale=cfg['gross_cap']/gross;w={s:float(x)*scale for s,x in w.items()}
        return w,regime,up,dn,idx[-1],rs,vr,btc_m

    def _mark(self,prices,now):
        old=self.state.get('last_prices') or {};w=self.state.get('weights') or {};eq=float(self.state.get('equity') or 100.0);pnl=0.0
        if old:
            for s,x in w.items():
                op=float(old.get(s) or 0);np_=float(prices.get(s) or 0)
                if op>0 and np_>0:pnl+=float(x)*(np_/op-1.0)
            eq*=max(.01,1.0+pnl)
            hours=max(0.0,(now-float(self.state.get('last_mark_ts') or now))/3600.0)
            short=sum(abs(float(x)) for x in w.values() if float(x)<0)
            fund=eq*short*FUND*(hours/8.0);eq-=fund
            self.state['funding']=float(self.state.get('funding') or 0)+fund
        self.state['equity']=eq;self.state['last_prices']={k:float(v) for k,v in prices.items()};self.state['last_mark_ts']=now
        return pnl

    def _rebalance(self,target,slot,bar_ts):
        old={k:float(v) for k,v in (self.state.get('weights') or {}).items()};keys=set(old)|set(target)
        turn=sum(abs(float(target.get(k,0))-float(old.get(k,0))) for k in keys);eq=float(self.state.get('equity') or 100.0)
        cost=eq*turn*COST;eq-=cost
        self.state['equity']=eq;self.state['weights']={k:float(v) for k,v in target.items() if abs(float(v))>1e-9}
        self.state['last_rebalance_slot']=int(slot);self.state['costs']=float(self.state.get('costs') or 0)+cost
        self.state['rebalance_count']=int(self.state.get('rebalance_count') or 0)+1
        e={'ts':time.time(),'bar_ts':str(bar_ts),'equity':round(eq,6),'turnover':round(turn,6),'cost':round(cost,8),'weights':self.state['weights']}
        h=list(self.state.get('history') or []);h.append(e);self.state['history']=h[-300:]
        return e

    async def refresh(self):
        if not self.enabled:return self.status()
        try:
            champ=self._champion();self._switch_champion_if_needed(champ);cfg=dict(champ.get('config') or DEFAULT_CFG);champion_name=str(champ.get('champion_name') or 'v24')
            raw=[]
            for s in SYMBOLS:
                raw.append(await self._frame4h(s));await asyncio.sleep(.10)
            fresh={s:x for s,x in zip(SYMBOLS,raw)};frames={s:self._resample8(x) for s,x in fresh.items()}
            if any(len(frames[s])<370 for s in SYMBOLS):raise RuntimeError('TURBO_HISTORY_WARMUP_INCOMPLETE')
            idx,c,r,bull,bear,score,vol=self._features(frames)
            target,regime,up,dn,bar_ts,rs,vr,btc_m=self._target(idx,c,bull,bear,score,vol,cfg)
            fixed24,fixed24_regime,_,_,_,_,_,fixed24_bm=self._target(idx,c,bull,bear,score,vol,DEFAULT_CFG)
            alts=[s for s in SYMBOLS if s not in ('BTCUSDT','ETHUSDT')]
            m21=c.pct_change(21);m63=c.pct_change(63);rawscore=((.35*m21+.65*m63)/(vol*np.sqrt(63))).replace([np.inf,-np.inf],np.nan)
            xs=rawscore[alts].max(axis=1)-rawscore[alts].min(axis=1);qt=xs.rolling(189,min_periods=94).quantile(.90).shift(1)
            row=rawscore[alts].iloc[-1].dropna();hi=row.idxmax() if len(row) else None;lo=row.idxmin() if len(row) else None
            v28_snapshot={'fixed_v24_target_weights':fixed24,'fixed_v24_regime':fixed24_regime,'btc_momentum_72h':float(fixed24_bm),
                          'dispersion':float(xs.iloc[-1]) if np.isfinite(xs.iloc[-1]) else None,'threshold_q90_189':float(qt.iloc[-1]) if np.isfinite(qt.iloc[-1]) else None,
                          'hi_symbol':hi,'lo_symbol':lo,'hi_vol':float(vol[hi].iloc[-1]) if hi else None,'lo_vol':float(vol[lo].iloc[-1]) if lo else None,
                          'closed_prices':{s:float(c[s].iloc[-1]) for s in SYMBOLS},'bar_ts':str(bar_ts)}
            v70_target=dict(target)
            if regime=='BULL_TURBO':
                cand=sorted([(float(score[s].iloc[-1]),s) for s in SYMBOLS if s not in ('BTCUSDT','ETHUSDT') and bool(bull[s].iloc[-1])],reverse=True)
                if cand:
                    top=max(abs(cand[0][0]),1e-9);k=min(3,1+sum(1 for x in cand[1:3] if x[0]>=0.75*top))
                    unit=.45/math.sqrt(365*3);v70_target={}
                    for _,s in cand[:k]:v70_target[s]=rs*cfg['long_mult']*min(1.0,unit/max(float(vol[s].iloc[-1]),1e-6))/k
                    gross=sum(abs(float(x)) for x in v70_target.values())
                    if gross>cfg['gross_cap']:
                        scale=cfg['gross_cap']/gross;v70_target={s:float(x)*scale for s,x in v70_target.items()}
            btc_m21=float(c.BTCUSDT.iloc[-1]/c.BTCUSDT.iloc[-22]-1.0) if len(c)>=22 else 0.0
            prices={s:float(fresh[s].iloc[-1].close) for s in SYMBOLS};now=time.time();mtm=self._mark(prices,now)
            slot=int(pd.Timestamp(bar_ts).timestamp()//86400);prev_rs=float(self.state.get('last_risk_scale',1.0));prev_regime=self.state.get('last_regime')
            emergency=bool(rs<prev_rs-.05 or (prev_regime and regime!=prev_regime));reb=None
            if self.state.get('last_rebalance_slot')!=slot or emergency:reb=self._rebalance(target,slot,bar_ts)
            self.state['last_risk_scale']=rs;self.state['last_regime']=regime
            self.state['observation_count']=int(self.state.get('observation_count') or 0)+1
            obs={'ts':now,'equity':round(float(self.state['equity']),6),'return_pct':round((float(self.state['equity'])/100-1)*100,4),
                 'regime':regime,'gross_exposure':round(sum(abs(float(x)) for x in target.values()),4),'weights':self.state.get('weights') or {}}
            eh=list(self.state.get('equity_history') or []);eh.append(obs);self.state['equity_history']=eh[-1000:]
            self._save();self.last_refresh=now;self.last_error=None
            self.latest={'mode':'PAPER_SHADOW','strategy':champion_name,'regime':regime,
                         'breadth_bull':up,'breadth_bear':dn,'risk_scale':round(rs,3),'btc_vol_ratio':round(vr,3),
                         'btc_momentum_72h_pct':round(btc_m*100,4),'btc_momentum_168h_pct':round(btc_m21*100,4),
                         'bar_ts':str(bar_ts),'target_weights':target,
                         'v70_diversified_target_weights':v70_target,'v28_snapshot':v28_snapshot,
                         'gross_exposure':round(sum(abs(float(x)) for x in target.values()),4),
                         'paper_equity':round(float(self.state['equity']),6),'paper_return_pct':round((float(self.state['equity'])/100-1)*100,4),
                         'mark_return_pct':round(mtm*100,5),'emergency_derisk':emergency,'rebalance':reb}
        except Exception as exc:self.last_error=str(exc)[:300]
        return self.status()

    def status(self):
        champ=self._champion()
        paper={'equity':round(float(self.state.get('equity') or 100),6),
               'return_pct':round((float(self.state.get('equity') or 100)/100-1)*100,4),
               'weights':self.state.get('weights') or {},'rebalance_count':self.state.get('rebalance_count',0),
               'realized_costs':round(float(self.state.get('costs') or 0),8),'funding_costs':round(float(self.state.get('funding') or 0),8),
               'history':(self.state.get('history') or [])[-40:],'equity_history':(self.state.get('equity_history') or [])[-100:]}
        obs=int(self.state.get('observation_count') or 0)
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW','interval_seconds':self.interval,
                'last_refresh':self.last_refresh,'last_error':self.last_error,'latest':self.latest,'paper':paper,'observation_count':obs,
                'phase':'WARMUP' if obs<24 else ('LEARNING' if obs<96 else 'ADAPTIVE_READY'),
                'current_champion':champ.get('champion_name','v24'),
                'locked_config':{'timeframe':'8h','rebalance':'24h','assets':SYMBOLS,'config':champ.get('config') or DEFAULT_CFG,
                                 'source':champ.get('source'),'live_execution_ready':False}}

    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='raven-turbo-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        while True:
            await self.refresh()
            await asyncio.sleep(max(300.0,self.interval))

    def reset_paper(self):
        self.state={'equity':100.0,'weights':{},'last_prices':{},'last_mark_ts':None,
                    'last_rebalance_slot':None,'costs':0.0,'funding':0.0,'rebalance_count':0,'history':[],'observation_count':0,'equity_history':[]}
        self._save();self.latest={};self.last_error=None
        return self.status()

raven_turbo_shadow=RavenTurboShadow()
