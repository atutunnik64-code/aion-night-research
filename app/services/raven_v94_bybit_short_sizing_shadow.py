from __future__ import annotations
import asyncio,json,time,urllib.parse,urllib.request
from pathlib import Path
import pandas as pd
from app.services.raven_turbo_shadow import raven_turbo_shadow,COST,FUND
from app.services.raven_v70_regime_shadow import raven_v70_regime_shadow

ROOT=Path(__file__).parents[2]
HIST=ROOT/'data'/'derivatives_history'/'bybit_account_ratio_4h_BTCUSDT.csv'
STATE=ROOT/'data'/'raven_v94_bybit_short_sizing_shadow.json'
BYBIT='https://api.bybit.com/v5/market/account-ratio'
TRAIL=180

class RavenV94BybitShortSizingShadow:
    def __init__(self):
        self.enabled=True;self.interval=300.0;self.task=None
        self.last_error=None;self.last_refresh=None;self.latest={}
        self.refresh_lock=asyncio.Lock();self.state=self._load()
        self.hist=self._load_hist()
    @staticmethod
    def _blank():
        return {'equity':100.0,'peak':100.0,'max_dd_pct':0.0,'weights':{},
                'last_prices':{},'last_mark_ts':None,'last_bar_ts':None,'costs':0.0,
                'funding':0.0,'observation_count':0,'bybit_rows':[],'history':[],
                'in_short_episode':False,'short_episode_start_bar':None,
                'short_episode_start_equity':None,'short_episode_start_v70_equity':None,
                'completed_short_episodes':0,'short_episodes':[],'started_at':time.time()}
    def _load(self):
        x=self._blank()
        try:x.update(json.loads(STATE.read_text(encoding='utf-8')))
        except Exception:pass
        return x
    def _save(self):
        STATE.parent.mkdir(parents=True,exist_ok=True)
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _load_hist():
        d=pd.read_csv(HIST);d['ts']=pd.to_datetime(d.timestamp,unit='ms',utc=True)
        d['ratio']=pd.to_numeric(d.longShortRatio,errors='coerce')
        return d[['ts','ratio']].dropna().sort_values('ts')
    @staticmethod
    def _fetch_rows():
        q=urllib.parse.urlencode({'category':'linear','symbol':'BTCUSDT','period':'4h','limit':500})
        with urllib.request.urlopen(BYBIT+'?'+q,timeout=10) as r:x=json.load(r)
        if int(x.get('retCode',-1))!=0:raise RuntimeError('BYBIT_RATIO_'+str(x.get('retMsg')))
        out=[]
        for z in x.get('result',{}).get('list') or []:
            ts=int(z['timestamp']);buy=float(z['buyRatio']);sell=float(z['sellRatio'])
            out.append({'timestamp':ts,'ratio':buy/max(sell,1e-12)})
        return out
    def _merge_live(self,rows):
        old={int(x['timestamp']):float(x['ratio']) for x in (self.state.get('bybit_rows') or [])}
        for x in rows:old[int(x['timestamp'])]=float(x['ratio'])
        keys=sorted(old)[-600:]
        self.state['bybit_rows']=[{'timestamp':k,'ratio':old[k]} for k in keys]
    def _ratio_scale(self,bar_ts):
        live=pd.DataFrame(self.state.get('bybit_rows') or [])
        if len(live):
            live['ts']=pd.to_datetime(live.timestamp,unit='ms',utc=True)
            live=live[['ts','ratio']]
            d=pd.concat([self.hist,live],ignore_index=True).drop_duplicates('ts',keep='last').sort_values('ts')
        else:d=self.hist
        bar=pd.Timestamp(bar_ts)
        raw=d[(d.ts>=bar)&(d.ts<bar+pd.Timedelta(hours=8))]
        if raw.empty or raw.ts.max()<bar+pd.Timedelta(hours=4):return None
        s=d.set_index('ts').ratio.sort_index().resample('8h').last()
        if bar not in s.index or not pd.notna(s.loc[bar]):return None
        pos=s.index.get_loc(bar);hist=s.iloc[max(0,pos-TRAIL):pos+1].dropna()
        if len(hist)<120:return None
        cur=float(s.loc[bar]);pct=float((hist<=cur).mean())
        return {'ratio':cur,'percentile':pct,'scale':0.875+0.25*pct,'samples':int(len(hist)),
                'raw_latest':str(raw.ts.max())}
    def _mark(self,prices,now):
        old=self.state.get('last_prices') or {};w=self.state.get('weights') or {}
        eq=float(self.state.get('equity') or 100.0)
        if old:
            pnl=0.0
            for s,x in w.items():
                a=float(old.get(s) or 0);b=float(prices.get(s) or 0)
                if a>0 and b>0:pnl+=float(x)*(b/a-1.0)
            eq*=max(.01,1.0+pnl)
            hours=max(0.0,(now-float(self.state.get('last_mark_ts') or now))/3600.0)
            gross=sum(abs(float(x)) for x in w.values())
            fund=eq*gross*FUND*(hours/8.0);eq-=fund
            self.state['funding']=float(self.state.get('funding') or 0)+fund
        self.state['equity']=eq;self.state['last_prices']={k:float(v) for k,v in prices.items()}
        self.state['last_mark_ts']=now;self.state['peak']=max(float(self.state.get('peak') or eq),eq)
        self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0.0),(eq/self.state['peak']-1.0)*100.0)
    def _rebalance(self,target,bar_ts):
        old={k:float(v) for k,v in (self.state.get('weights') or {}).items()};keys=set(old)|set(target)
        turn=sum(abs(float(target.get(k,0))-float(old.get(k,0))) for k in keys)
        eq=float(self.state.get('equity') or 100.0);cost=eq*turn*COST;eq-=cost
        self.state['equity']=eq;self.state['costs']=float(self.state.get('costs') or 0)+cost
        self.state['weights']={k:float(v) for k,v in target.items() if abs(float(v))>1e-9}
        self.state['last_bar_ts']=str(bar_ts)
        row={'ts':time.time(),'bar_ts':str(bar_ts),'equity':round(eq,6),'turnover':round(turn,6),
             'cost':round(cost,8),'weights':self.state['weights']}
        h=list(self.state.get('history') or []);h.append(row);self.state['history']=h[-300:]
        return row
    @staticmethod
    def _scaled_target(base,info):
        target={k:float(v) for k,v in (base or {}).items()};is_short=sum(target.values())<-1e-12
        if not is_short:return target,False,1.0
        if info is None:return None,True,None
        sc=float(info['scale']);return {k:v*sc for k,v in target.items()},True,sc

    def _update_episode(self,is_short,bar_ts,v70_equity):
        active=bool(self.state.get('in_short_episode'))
        if is_short and not active:
            self.state['in_short_episode']=True;self.state['short_episode_start_bar']=str(bar_ts)
            self.state['short_episode_start_equity']=float(self.state.get('equity') or 100.0)
            self.state['short_episode_start_v70_equity']=float(v70_equity)
        elif (not is_short) and active:
            a=float(self.state.get('short_episode_start_equity') or 100.0)
            b=float(self.state.get('short_episode_start_v70_equity') or 100.0)
            eq=float(self.state.get('equity') or 100.0);v=float(v70_equity)
            cr=(eq/max(a,1e-9)-1.0)*100.0;br=(v/max(b,1e-9)-1.0)*100.0
            row={'start':self.state.get('short_episode_start_bar'),'end':str(bar_ts),
                 'candidate_pct':cr,'v70_pct':br,'alpha_pp':cr-br}
            h=list(self.state.get('short_episodes') or []);h.append(row);self.state['short_episodes']=h[-100:]
            self.state['completed_short_episodes']=int(self.state.get('completed_short_episodes') or 0)+1
            self.state['in_short_episode']=False;self.state['short_episode_start_bar']=None
            self.state['short_episode_start_equity']=None;self.state['short_episode_start_v70_equity']=None

    async def refresh(self):
        async with self.refresh_lock:
            try:
                v70=raven_v70_regime_shadow.status();x=v70.get('latest') or {}
                prices=dict(raven_turbo_shadow.state.get('last_prices') or {})
                if not x.get('bar_ts') or not prices:
                    self.latest={'strategy':'v94_bybit_short_sizing','waiting_for':'V70_SNAPSHOT'}
                    self.last_error=None;self.last_refresh=time.time();self._save();return self.status()
                bar_ts=str(x['bar_ts']);new_bar=bar_ts!=str(self.state.get('last_bar_ts') or '')
                if new_bar or not (self.state.get('bybit_rows') or []):
                    rows=await asyncio.to_thread(self._fetch_rows);self._merge_live(rows)
                now=time.time();self._mark(prices,now);base=dict(x.get('target_weights') or {})
                info=self._ratio_scale(bar_ts);target,is_short,scale=self._scaled_target(base,info)
                if target is None:
                    self.latest={'strategy':'v94_bybit_short_sizing','bar_ts':bar_ts,
                                 'waiting_for':'CLOSED_BYBIT_RATIO','is_short':True}
                    self.last_error=None;self.last_refresh=now;self._save();return self.status()
                reb=None
                if not self.state.get('last_bar_ts'):
                    reb=self._rebalance(target,bar_ts)
                    self._update_episode(is_short,bar_ts,float(v70.get('equity') or 100.0))
                elif new_bar:
                    reb=self._rebalance(target,bar_ts)
                    self.state['observation_count']=int(self.state.get('observation_count') or 0)+1
                    self._update_episode(is_short,bar_ts,float(v70.get('equity') or 100.0))
                self._save();self.last_error=None;self.last_refresh=now
                eq=float(self.state.get('equity') or 100.0)
                self.latest={'strategy':'v94_bybit_short_sizing','bar_ts':bar_ts,
                             'source_v70_equity':float(v70.get('equity') or 100.0),'source_regime':x.get('source_regime'),
                             'is_short':is_short,'short_scale':scale,'bybit':info,'target_weights':target,
                             'paper_equity':eq,'paper_return_pct':(eq/100.0-1.0)*100.0,'rebalance':reb}
            except Exception as exc:
                self.last_error=str(exc)[:300];self.last_refresh=time.time()
            return self.status()
    def status(self):
        eq=float(self.state.get('equity') or 100.0);obs=int(self.state.get('observation_count') or 0)
        eps=list(self.state.get('short_episodes') or []);alphas=[float(x.get('alpha_pp') or 0) for x in eps]
        positive_ratio=(sum(x>0 for x in alphas)/len(alphas)) if alphas else 0.0
        median_alpha=float(pd.Series(alphas).median()) if alphas else 0.0
        future_gate={'required_completed_short_episodes':10,'completed_short_episodes':len(eps),
                     'positive_alpha_ratio':positive_ratio,'median_alpha_pp':median_alpha,
                     'ready':len(eps)>=10 and positive_ratio>=0.6 and median_alpha>0}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW',
                'strategy':'v94_bybit_short_sizing','future_only':True,'promotion_eligible':False,
                'live_enabled':False,'grid':False,'martingale':False,'dca':False,
                'locked_rule':'v70_short_x_(0.875+0.25*BTC_bybit_ls_trailing60d_percentile)',
                'trail_bars':TRAIL,'equity':round(eq,6),'return_pct':round((eq/100.0-1.0)*100.0,4),
                'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0.0),4),
                'observation_count':obs,'phase':'WARMUP' if obs<24 else 'FUTURE_VALIDATION',
                'future_short_gate':future_gate,'latest':self.latest,
                'last_refresh':self.last_refresh,'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='raven-v94-bybit-short-sizing-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None;self._save()
    async def _loop(self):
        while True:
            await self.refresh()
            waiting=(self.latest or {}).get('waiting_for')
            delay=5.0 if waiting=='V70_SNAPSHOT' else (60.0 if waiting=='CLOSED_BYBIT_RATIO' else max(300.0,self.interval))
            await asyncio.sleep(delay)

raven_v94_bybit_short_sizing_shadow=RavenV94BybitShortSizingShadow()
