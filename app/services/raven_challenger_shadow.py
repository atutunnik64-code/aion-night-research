from __future__ import annotations
import asyncio, json, math, time
from pathlib import Path
import httpx, numpy as np, pandas as pd
from app.http_shared import SHARED_SSL_CONTEXT
from app.services.raven_evolution_manager import raven_evolution_manager
from app.services.raven_turbo_shadow import raven_turbo_shadow
from app.services.raven_funding_feature import raven_funding_feature

ROOT=Path(__file__).parents[2]
CACHE=ROOT/'data'/'raven_turbo_highbeta_cache'
STATE=ROOT/'data'/'raven_challenger_shadow_state.json'
ARCHIVE=ROOT/'data'/'raven_challenger_shadow_archive.json'
SYMBOLS=['BTCUSDT','ETHUSDT','SOLUSDT','SUIUSDT','NEARUSDT','APTUSDT','INJUSDT','TIAUSDT','SEIUSDT','WIFUSDT','PEPEUSDT','FLOKIUSDT','ARBUSDT','OPUSDT','AAVEUSDT','RUNEUSDT','JUPUSDT']
COST=.0012; FUND=.00005

class RavenChallengerShadow:
    def __init__(self):
        self.enabled=True; self.interval=1800.0; self.task=None
        self.last_refresh=None; self.last_error=None; self.latest={}
        self.state=self._load()

    def _blank(self,name=None):
        return {'nominee_name':name,'started_at':time.time(),'equity':100.0,'weights':{},'last_prices':{},
                'last_mark_ts':None,'last_rebalance_slot':None,'costs':0.0,'funding':0.0,
                'rebalance_count':0,'observation_count':0,'active_observation_count':0,'equity_history':[],'history':[],
                'benchmark_start_equity':None,'benchmark_current_equity':None,'benchmark_equity_history':[],'benchmark_champion_name':None}

    def _load(self):
        try:return json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:return self._blank()
    def _save(self):
        STATE.parent.mkdir(parents=True,exist_ok=True)
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')

    def _archive_current(self):
        if not self.state.get('nominee_name'): return
        rows=[]
        try: rows=json.loads(ARCHIVE.read_text(encoding='utf-8'))
        except Exception: pass
        if not isinstance(rows,list): rows=[]
        rows.append({'archived_at':time.time(),**self.state})
        ARCHIVE.write_text(json.dumps(rows[-20:],ensure_ascii=False,indent=2),encoding='utf-8')

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
        x=pd.read_csv(CACHE/f'{symbol}_4h_720d.csv');x['ts']=pd.to_datetime(x.ts,utc=True)
        return x[['ts','open','high','low','close','base_vol']]
    async def _frame4h(self,symbol):
        hist=self._hist4h(symbol);fresh=pd.DataFrame();last_exc=None
        for attempt,delay in enumerate((.5,1.0,2.0,4.0),1):
            try:
                fresh=await self._latest4h(symbol)
                if not fresh.empty:break
            except Exception as exc:last_exc=exc
            await asyncio.sleep(delay)
        if fresh.empty:
            age=(pd.Timestamp.now(tz='UTC')-hist.ts.max()).total_seconds()/3600.0
            if age>12:raise RuntimeError(f'CHALLENGER_STALE_CACHE:{symbol}:{age:.1f}h:{last_exc}')
            return hist
        cols=['ts','open','high','low','close','base_vol'];x=pd.concat([hist[cols],fresh[cols]],ignore_index=True)
        return x.drop_duplicates('ts',keep='last').sort_values('ts').reset_index(drop=True)

    @staticmethod
    def _resample8(x):
        y=x.set_index('ts').resample('8h').agg({'open':'first','high':'max','low':'min','close':'last','base_vol':'sum'}).dropna()
        now=pd.Timestamp.now(tz='UTC')
        return y[(y.index+pd.Timedelta(hours=8))<=now]

    @staticmethod
    def _features(frames):
        idx=frames['BTCUSDT'].index
        for s in SYMBOLS:idx=idx.intersection(frames[s].index)
        c=pd.DataFrame({s:frames[s].loc[idx,'close'] for s in SYMBOLS});r=c.pct_change().fillna(0)
        vol=r.rolling(30).std().replace(0,np.nan);m21=c.pct_change(21);m63=c.pct_change(63)
        e12=c.ewm(span=12,adjust=False).mean();e72=c.ewm(span=72,adjust=False).mean()
        e36=c.ewm(span=36,adjust=False).mean();e180=c.ewm(span=180,adjust=False).mean()
        bull=((e12>e72)&(m21>.01)&(e36>e180)&(m63>.02)).fillna(False)
        bear=((e12<e72)&(m21<-.01)&(e36<e180)&(m63<-.02)).fillna(False)
        score=((.35*m21+.65*m63)/(vol*np.sqrt(63))).replace([np.inf,-np.inf],np.nan).fillna(-999)
        return idx,c,bull,bear,score,vol
    @staticmethod
    def _target(idx,c,bull,bear,score,vol,cfg,funding_feature=None):
        req={'long_mult','short_mult','bull_frac','bear_frac','btc_long_m','btc_short_m','vol1','vol2','cut1','cut2','gross_cap'}
        if not req.issubset(cfg):raise RuntimeError('CHALLENGER_CONFIG_INCOMPLETE')
        i=-1;n=len(SYMBOLS);up=int(bull.iloc[i].sum());dn=int(bear.iloc[i].sum());uf=up/n;df=dn/n
        btc_v=vol.BTCUSDT;med=btc_v.rolling(180).median()
        vr=float(btc_v.iloc[i]/max(float(med.iloc[i]),1e-9)) if np.isfinite(med.iloc[i]) else 1.0
        rs=1.0 if vr<=float(cfg['vol1']) else (float(cfg['cut1']) if vr<=float(cfg['vol2']) else float(cfg['cut2']))
        btc_m=float(c.BTCUSDT.iloc[i]/c.BTCUSDT.iloc[i-9]-1.0);target=.45/math.sqrt(365*3);w={};regime='CASH'
        if bool(bull.BTCUSDT.iloc[i]) and uf>=float(cfg['bull_frac']) and btc_m>=float(cfg['btc_long_m']):
            cand=sorted([(float(score[s].iloc[i]),s) for s in SYMBOLS if s not in ('BTCUSDT','ETHUSDT') and bool(bull[s].iloc[i])],reverse=True)[:1]
            for _,s in cand:w[s]=rs*float(cfg['long_mult'])*min(1.0,target/max(float(vol[s].iloc[i]),1e-6))
            regime='BULL_CHALLENGER' if w else 'CASH'
        elif bool(bear.BTCUSDT.iloc[i]) and df>=float(cfg['bear_frac']) and btc_m<=float(cfg['btc_short_m']):
            for s,m in [('BTCUSDT',.65),('ETHUSDT',.35)]:
                w[s]=-rs*float(cfg['short_mult'])*m*min(1.0,target/max(float(vol[s].iloc[i]),1e-6))
            regime='BEAR_CHALLENGER'
        funding_meta={'required':False,'ok':True,'applied':False,'dispersion':0.0,'lag_hours':None}
        fund_keys={'fund_long_q','fund_short_q','fund_mismatch_scale','fund_disp_gate'}
        if fund_keys.issubset(cfg):
            funding_meta['required']=True
            ff=funding_feature or {}
            funding_meta.update({'ok':bool(ff.get('ok')),'dispersion':float(ff.get('dispersion') or 0.0),'lag_hours':ff.get('lag_hours'),'coverage':ff.get('coverage'),'sources':ff.get('sources')})
            if not ff.get('ok'):
                w={};regime='FUNDING_DATA_UNAVAILABLE'
            elif float(ff.get('dispersion') or 0.0)>=float(cfg['fund_disp_gate']):
                ranks=ff.get('ranks') or {};mismatch=max(0.0,min(1.0,float(cfg['fund_mismatch_scale'])))
                before=sum(abs(float(x)) for x in w.values())
                for sym in list(w):
                    q=ranks.get(sym)
                    if q is None:continue
                    aligned=(float(w[sym])>0 and float(q)>=float(cfg['fund_long_q'])) or (float(w[sym])<0 and float(q)<=float(cfg['fund_short_q']))
                    if not aligned:w[sym]=float(w[sym])*mismatch
                    if abs(float(w[sym]))<=1e-12:w.pop(sym,None)
                after=sum(abs(float(x)) for x in w.values());funding_meta['applied']=bool(after<before-1e-12);funding_meta['gross_before']=before;funding_meta['gross_after']=after
                if funding_meta['applied']:regime=regime+'_FUNDING_FILTERED'
        gross=sum(abs(float(x)) for x in w.values());cap=float(cfg['gross_cap'])
        if gross>cap:
            scale=cap/gross;w={s:float(x)*scale for s,x in w.items()}
        return w,regime,up,dn,idx[-1],rs,vr,btc_m,funding_meta
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
        h=list(self.state.get('history') or []);h.append(e);self.state['history']=h[-200:]
        return e

    @staticmethod
    def _drawdown(history):
        if not history:return 0.0
        vals=[float(x.get('equity') or 100.0) for x in history]
        peak=vals[0];worst=0.0
        for v in vals:
            peak=max(peak,v)
            if peak>0:worst=min(worst,v/peak-1.0)
        return worst*100.0

    async def _frames(self):
        sem=asyncio.Semaphore(2)
        async def one(symbol):
            async with sem:
                raw=await self._frame4h(symbol)
                return symbol,self._resample8(raw),raw
        rows=await asyncio.gather(*(one(s) for s in SYMBOLS))
        frames={s:f for s,f,_ in rows};fresh={s:r for s,_,r in rows}
        return frames,fresh

    async def refresh(self):
        try:
            evo=raven_evolution_manager.status()
            nominee=evo.get('shadow_nominee') or {}
            name=nominee.get('name')
            if not name:
                self.latest={'ok':True,'mode':'PAPER_SHADOW','reason':'NO_SHADOW_NOMINEE'}
                self.last_error=None;self.last_refresh=time.time()
                return self.status()
            cfg=nominee.get('chosen_cfg') or {}
            if self.state.get('nominee_name')!=name:
                self._archive_current();self.state=self._blank(name)
                bs=raven_turbo_shadow.status();bench=float((bs.get('paper') or {}).get('equity') or 100.0)
                self.state['benchmark_start_equity']=bench;self.state['benchmark_current_equity']=bench;self.state['benchmark_champion_name']=bs.get('current_champion') or 'v24'
                self.state['chosen_cfg']=cfg;self.state['source_file']=nominee.get('source_file')
            frames,fresh=await self._frames()
            idx,c,bull,bear,score,vol=self._features(frames)
            if len(idx)<200:raise RuntimeError('CHALLENGER_HISTORY_TOO_SHORT')
            prices={s:float(fresh[s].iloc[-1].close) for s in SYMBOLS}
            now=time.time();self._mark(prices,now)
            funding_feature=None
            fund_keys={'fund_long_q','fund_short_q','fund_mismatch_scale','fund_disp_gate'}
            if fund_keys.issubset(cfg):
                funding_feature=await raven_funding_feature.snapshot(idx[-1],lag_bars=2)
            target,regime,up,dn,bar_ts,rs,vr,btc_m,funding_meta=self._target(idx,c,bull,bear,score,vol,cfg,funding_feature)
            slot=int(pd.Timestamp(bar_ts).timestamp()//86400)
            rebalance=None
            funding_fail_derisk=bool(funding_meta.get('required') and not funding_meta.get('ok') and (self.state.get('weights') or {}))
            if self.state.get('last_rebalance_slot')!=slot or funding_fail_derisk:
                rebalance=self._rebalance(target,slot,bar_ts)
            self.state['observation_count']=int(self.state.get('observation_count') or 0)+1
            if target:self.state['active_observation_count']=int(self.state.get('active_observation_count') or 0)+1
            hist=list(self.state.get('equity_history') or [])
            hist.append({'ts':now,'equity':round(float(self.state['equity']),6),'regime':regime})
            self.state['equity_history']=hist[-1000:]
            bench=float((raven_turbo_shadow.status().get('paper') or {}).get('equity') or 100.0)
            self.state['benchmark_current_equity']=bench
            bh=list(self.state.get('benchmark_equity_history') or []);bh.append({'ts':now,'equity':round(bench,6)});self.state['benchmark_equity_history']=bh[-1000:]
            b0=float(self.state.get('benchmark_start_equity') or bench or 100.0)
            bench_ret=(bench/b0-1.0)*100.0 if b0>0 else 0.0
            challenger_ret=float(self.state.get('equity') or 100.0)-100.0
            self.latest={'ok':True,'mode':'PAPER_SHADOW','nominee':name,'bar_ts':str(bar_ts),
                         'regime':regime,'breadth_up':up,'breadth_down':dn,
                         'risk_scale':round(rs,4),'vol_ratio':round(vr,4),
                         'btc_momentum_72h_pct':round(btc_m*100,4),'weights':self.state.get('weights') or {},
                         'funding_filter':funding_meta,
                         'gross_exposure':round(sum(abs(float(x)) for x in (self.state.get('weights') or {}).values()),4),
                         'benchmark_v24_return_pct':round(bench_ret,4),'relative_edge_pct':round(challenger_ret-bench_ret,4),
                         'rebalance':rebalance}
            self.last_error=None;self.last_refresh=now;self._save()
            review=raven_evolution_manager.review_shadow(self.status());self.latest['shadow_review']=review
        except Exception as exc:
            self.last_error=str(exc)[:500];self.last_refresh=time.time()
        return self.status()

    def status(self):
        eq=float(self.state.get('equity') or 100.0);hist=self.state.get('equity_history') or []
        obs=int(self.state.get('observation_count') or 0)
        b0=float(self.state.get('benchmark_start_equity') or 100.0);bc=float(self.state.get('benchmark_current_equity') or b0)
        bret=(bc/b0-1.0)*100.0 if b0>0 else 0.0;cret=eq-100.0;edge=cret-bret
        bh=self.state.get('benchmark_equity_history') or [];bdd=self._drawdown(bh);started=float(self.state.get('started_at') or time.time());age_h=max(0.0,(time.time()-started)/3600)
        return {'ok':self.last_error is None,'mode':'PAPER_SHADOW','enabled':self.enabled,
                'nominee_name':self.state.get('nominee_name'),'equity':round(eq,6),
                'return_pct':round(cret,4),'max_dd_pct':round(self._drawdown(hist),4),
                'benchmark_champion_name':self.state.get('benchmark_champion_name') or 'v24',
                'benchmark_return_pct':round(bret,4),'benchmark_v24_return_pct':round(bret,4),
                'benchmark_max_dd_pct':round(bdd,4),'relative_edge_pct':round(edge,4),
                'comparison_age_hours':round(age_h,2),
                'comparison_phase':'WARMUP' if obs<24 else ('LEARNING' if obs<96 or age_h<48 else 'REVIEW_READY'),
                'comparison_ready':bool(obs>=96 and age_h>=48),'outperforming_v24':bool(obs>=96 and age_h>=48 and edge>0),
                'active_observation_count':int(self.state.get('active_observation_count') or 0),
                'observation_count':int(self.state.get('observation_count') or 0),
                'rebalance_count':int(self.state.get('rebalance_count') or 0),
                'costs':round(float(self.state.get('costs') or 0),8),
                'funding':round(float(self.state.get('funding') or 0),8),
                'last_refresh':self.last_refresh,'last_error':self.last_error,
                'latest':self.latest,'live_promotion_enabled':False}

    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='raven-challenger-shadow')

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None;self._save()

    async def _loop(self):
        await asyncio.sleep(45.0)
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(max(300.0,self.interval))

raven_challenger_shadow=RavenChallengerShadow()
