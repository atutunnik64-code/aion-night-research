from __future__ import annotations
import asyncio,json,time,urllib.parse,urllib.request
from pathlib import Path
import numpy as np
import pandas as pd

ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'aftershock_v2_shadow.json'
SYMS=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT']
COST=.0012; ALLOC=.10; THRESH=.8636581067191152
SCALER={'shock_mult':(1.5474588691947753,.5285804733490173),'oi_mult':(1.8735164443892334,.9485616025174779),'abs_flow':(.21576759954734712,.13326350459184044)}
KLINE='https://fapi.binance.com/fapi/v1/klines'; DATA='https://fapi.binance.com/futures/data/'

def _get(url,params):
    q=urllib.parse.urlencode(params); req=urllib.request.Request(url+'?'+q,headers={'User-Agent':'AION-Crypto-Radar/0.11.58'})
    with urllib.request.urlopen(req,timeout=20) as r:return json.loads(r.read().decode('utf-8'))

def _blank():
    return {'strategy':'aftershock_v2','version':'v2.0','start_ts':time.time(),'last_bars':{},'pending':[],'resolved':[],
            'events':[],'equity':100.0,'peak':100.0,'max_dd_pct':0.0,'observation_count':0,'event_count':0,'trade_count':0}

class AftershockV2Shadow:
    def __init__(self): self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_refresh=None;self.state=self._load()
    def _load(self):
        try:return json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:return _blank()
    def _save(self): STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _frame(self,sym):
        k=_get(KLINE,{'symbol':sym,'interval':'1h','limit':220})
        oi=_get(DATA+'openInterestHist',{'symbol':sym,'period':'1h','limit':220})
        tk=_get(DATA+'takerlongshortRatio',{'symbol':sym,'period':'1h','limit':220})
        gl=_get(DATA+'globalLongShortAccountRatio',{'symbol':sym,'period':'1h','limit':220})
        prem=_get('https://fapi.binance.com/fapi/v1/premiumIndex',{'symbol':sym})
        p=pd.DataFrame([{'bucket':int(x[0]//3600000),'bar_ts':int(x[0]),'close':float(x[4]),'quote_volume':float(x[7])} for x in k])
        o=pd.DataFrame([{'bucket':int(x['timestamp']//3600000),'oi_val':float(x['sumOpenInterestValue'])} for x in oi])
        t=pd.DataFrame([{'bucket':int(x['timestamp']//3600000),'buySellRatio':float(x['buySellRatio'])} for x in tk])
        g=pd.DataFrame([{'bucket':int(x['timestamp']//3600000),'global_ls':float(x['longShortRatio'])} for x in gl])
        z=p.merge(o,on='bucket').merge(t,on='bucket').merge(g,on='bucket').sort_values('bucket').drop_duplicates('bucket')
        now_bucket=int(time.time()//3600); z=z[z.bucket<now_bucket].copy()
        z['ret']=z.close.pct_change();z['oi']=z.oi_val.pct_change();z['flow']=np.log(z.buySellRatio.clip(lower=1e-9));lv=np.log(z.quote_volume.clip(lower=1))
        z['vz']=(lv-lv.rolling(72).mean())/lv.rolling(72).std();z['r95']=z.ret.abs().rolling(168,min_periods=72).quantile(.95).shift(1);z['oi10']=z.oi.rolling(168,min_periods=72).quantile(.10).shift(1)
        return z.reset_index(drop=True),float(prem.get('lastFundingRate') or 0)

    @staticmethod
    def _score(r,funding):
        sg=float(np.sign(r.ret)); shock=abs(float(r.ret))/max(float(r.r95),1e-9); oi=(-float(r.oi))/max(-float(r.oi10),1e-9); af=abs(float(r.flow))
        z1=(shock-SCALER['shock_mult'][0])/SCALER['shock_mult'][1];z2=(oi-SCALER['oi_mult'][0])/SCALER['oi_mult'][1];z3=(af-SCALER['abs_flow'][0])/SCALER['abs_flow'][1]
        aligned=float(np.sign(r.flow)==sg and af>=.05);fopp=float(np.sign(funding)==-sg);copp=float(np.sign(np.log(max(float(r.global_ls),1e-9)))==-sg)
        score=z1+z2+.5*z3+.5*aligned+.35*fopp+.35*copp
        return score,sg,{'shock_mult':shock,'oi_mult':oi,'abs_flow':af,'aligned_flow':bool(aligned),'funding_opposite':bool(fopp),'crowd_opposite':bool(copp)}
    def _process_symbol(self,sym,z,funding):
        if z.empty:return
        latest=int(z.bar_ts.iloc[-1]); last=self.state['last_bars'].get(sym)
        if last is None:
            self.state['last_bars'][sym]=latest;return
        new=z[z.bar_ts>int(last)]
        for _,r in new.iterrows():
            self.state['observation_count']+=1
            if pd.notna(r.r95) and pd.notna(r.oi10) and pd.notna(r.vz):
                base=abs(float(r.ret))>=float(r.r95) and float(r.oi)<=float(r.oi10) and float(r.vz)>=1.0
                if base:
                    score,sg,feat=self._score(r,funding);self.state['event_count']+=1
                    ev={'symbol':sym,'bar_ts':int(r.bar_ts),'score':float(score),'high_cascade':bool(score>=THRESH),**feat}
                    self.state['events'].append(ev);self.state['events']=self.state['events'][-100:]
                    if score>=THRESH:
                        rid=f"{sym}:{int(r.bar_ts)}"
                        if not any(x['id']==rid for x in self.state['pending']) and not any(x['id']==rid for x in self.state['resolved']):
                            self.state['pending'].append({'id':rid,'symbol':sym,'bar_ts':int(r.bar_ts),'due_ts':int(r.bar_ts+12*3600000),
                                'entry':float(r.close),'side':float(-sg),'score':float(score),'cost':COST,'allocation_fraction':ALLOC})
                            self.state['trade_count']+=1
        self.state['last_bars'][sym]=latest

    def _resolve(self,frames):
        keep=[]
        for x in self.state['pending']:
            z=frames.get(x['symbol']); q=z[z.bar_ts>=x['due_ts']] if z is not None else pd.DataFrame()
            if q.empty:keep.append(x);continue
            exit_px=float(q.iloc[0].close);gross=float(x['side'])*(exit_px/float(x['entry'])-1);net=gross-COST
            eq=float(self.state['equity'])*(1+ALLOC*net);self.state['equity']=eq;self.state['peak']=max(float(self.state['peak']),eq)
            dd=(eq/float(self.state['peak'])-1)*100;self.state['max_dd_pct']=min(float(self.state['max_dd_pct']),dd)
            self.state['resolved'].append({**x,'exit':exit_px,'gross_return_pct':gross*100,'net_return_pct':net*100,'resolved_ts':time.time(),'paper_equity':eq})
            self.state['resolved']=self.state['resolved'][-200:]
        self.state['pending']=keep
    def _refresh_sync(self):
        frames={}
        for sym in SYMS:
            z,f=self._frame(sym);frames[sym]=z;self._process_symbol(sym,z,f)
        self._resolve(frames);self._save();return self.state
    async def refresh(self):
        try:self.state=await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status()
    def status(self):
        r=self.state.get('resolved') or [];wins=sum(1 for x in r if float(x.get('net_return_pct') or 0)>0);obs=int(self.state.get('observation_count') or 0);trades=int(self.state.get('trade_count') or 0);ret=float(self.state.get('equity') or 100)-100;dd=float(self.state.get('max_dd_pct') or 0)
        gate={'required_observations':24,'required_resolved_events':8,'observations':obs,'trades':trades,'resolved_events':len(r),'return_pct':ret,'max_dd_pct':dd,'ready_for_review':bool(obs>=24 and len(r)>=8 and ret>0 and dd>-8.0)}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW','strategy':'aftershock_v2','future_only':True,
                'promotion_eligible':False,'live_enabled':False,'grid':False,'martingale':False,'dca':False,
                'locked_config':{'cascade_threshold':THRESH,'hold_hours':12,'cost_round_trip':COST,'allocation_fraction':ALLOC,'scaler':SCALER},
                'equity':float(self.state.get('equity') or 100),'return_pct':float(self.state.get('equity') or 100)-100,
                'max_dd_pct':dd,'observation_count':obs,'future_gate':gate,
                'event_count':int(self.state.get('event_count') or 0),'trade_count':int(self.state.get('trade_count') or 0),
                'pending_count':len(self.state.get('pending') or []),'resolved_count':len(r),'wins':wins,
                'win_rate':(wins/len(r) if r else None),'recent_resolved':r[-10:],'last_refresh':self.last_refresh,'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='aftershock-v2-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        await asyncio.sleep(50)
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(60 if self.last_error else self.interval)

aftershock_v2_shadow=AftershockV2Shadow()
