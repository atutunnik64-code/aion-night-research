from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.options_surface_shadow import options_surface_shadow

ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
CTX=DATA/'liquidation_context_snapshots_v1.jsonl';STATE=DATA/'options_funding_regime_mismatch_shadow_v1.json'
ASSETS={'BTC':'BTCUSDT','ETH':'ETHUSDT'}
SKEW_MIN=2.0;MIN_ABS_FUNDING=.00010;MIN_ABS_OI_30M_PCT=.50
OI_LOOKBACK=1800;HOLD_SEC=14400;COOLDOWN=14400;COST=.0012;ALLOC=.05
MIN_HOURS=72.;MIN_RESOLVED=20;MIN_PER_ASSET=5

def _ctx():
    if not CTX.exists():return []
    out=[]
    for line in CTX.read_text(encoding='utf-8').splitlines()[-30000:]:
        try:out.append(json.loads(line))
        except Exception:pass
    return sorted(out,key=lambda x:float(x.get('ts') or 0))

def _blank():
    now=time.time();return {'version':'OPTIONS_FUNDING_REGIME_MISMATCH_V1','rule_frozen_at':now,
        'last_surface_ts':0.0,'last_event_ts':{},'pending':[],'resolved':[],
        'options_equity':100.0,'options_peak':100.0,'options_max_dd_pct':0.0,
        'perp_equity':100.0,'perp_peak':100.0,'perp_max_dd_pct':0.0}
def _latest(rows,sym,ts):
    z=[x for x in rows if x.get('symbol')==sym and float(x.get('ts') or 0)<=ts]
    return z[-1] if z else None

def _old(rows,sym,ts):
    z=[x for x in rows if x.get('symbol')==sym and float(x.get('ts') or 0)<=ts-OI_LOOKBACK]
    return z[-1] if z else None

def _nearest_exp(currency):
    exps=((options_surface_shadow.status().get('currencies') or {}).get(currency) or {}).get('expiries') or []
    valid=[x for x in exps if x.get('skew_down_minus_up') is not None]
    return valid[0] if valid else None

class OptionsFundingRegimeMismatchShadow:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state=_blank()
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _detect(self,rows):
        raw=options_surface_shadow.latest or {};surf=(raw.get('last_snapshot') or {});surface_ts=float(surf.get('ts') or 0)
        if surface_ts<=float(self.state.get('last_surface_ts') or 0):return 0
        self.state['last_surface_ts']=surface_ts;new=0
        for asset,sym in ASSETS.items():
            exp=_nearest_exp(asset)
            if not exp:continue
            skew=float(exp.get('skew_down_minus_up') or 0)
            if abs(skew)<SKEW_MIN:continue
            now=_latest(rows,sym,surface_ts);old=_old(rows,sym,surface_ts)
            if not now or not old:continue
            funding=float(now.get('funding_rate') or 0);entry=float(now.get('mark_price') or 0)
            if abs(funding)<MIN_ABS_FUNDING or entry<=0:continue
            oi0=float(old.get('open_interest') or 0);oi1=float(now.get('open_interest') or 0)
            if oi0<=0 or oi1<=0:continue
            oi_pct=(oi1/oi0-1.0)*100
            if abs(oi_pct)<MIN_ABS_OI_30M_PCT:continue
            options_side=-1.0 if skew>0 else 1.0
            perp_side=1.0 if funding>0 else -1.0
            if options_side==perp_side:continue
            prev=float((self.state.get('last_event_ts') or {}).get(asset) or 0)
            if surface_ts-prev<COOLDOWN:continue
            self.state.setdefault('pending',[]).append({'id':f'{asset}:{int(surface_ts)}','asset':asset,'symbol':sym,
                'ts':surface_ts,'due_ts':surface_ts+HOLD_SEC,'expiry':exp.get('expiry'),'skew_vol_points':skew,
                'funding_rate':funding,'oi_30m_pct':oi_pct,'entry':entry,'options_side':options_side,'perp_side':perp_side,
                'next_funding_time':int(now.get('next_funding_time') or 0),'round_trip_cost':COST,'allocation_fraction':ALLOC})
            self.state.setdefault('last_event_ts',{})[asset]=surface_ts;new+=1
        return new
    @staticmethod
    def _exit(rows,sym,due):
        for x in rows:
            if x.get('symbol')==sym and float(x.get('ts') or 0)>=due:return x
        return None
    def _resolve(self,rows):
        keep=[]
        for p in self.state.get('pending') or []:
            ex=self._exit(rows,p['symbol'],float(p['due_ts']))
            if not ex:keep.append(p);continue
            exit_px=float(ex.get('mark_price') or 0)
            if exit_px<=0:keep.append(p);continue
            ret=exit_px/float(p['entry'])-1.0
            crossed=int(p.get('next_funding_time') or 0)>0 and float(p['ts'])*1000<int(p['next_funding_time'])<=float(ex.get('ts') or 0)*1000
            out={**p,'exit':exit_px,'exit_ts':float(ex.get('ts') or 0),'funding_crossed':crossed}
            for name in ('options','perp'):
                side=float(p[name+'_side']);fund=side*float(p.get('funding_rate') or 0) if crossed else 0.0
                net=side*ret-COST-fund;out[name+'_net_return_pct']=net*100
                eqk=name+'_equity';peakk=name+'_peak';ddk=name+'_max_dd_pct'
                eq=float(self.state.get(eqk) or 100)*(1+ALLOC*net);peak=max(float(self.state.get(peakk) or 100),eq)
                self.state[eqk]=eq;self.state[peakk]=peak;self.state[ddk]=min(float(self.state.get(ddk) or 0),(eq/peak-1)*100)
                out[name+'_paper_equity']=eq
            self.state.setdefault('resolved',[]).append(out);self.state['resolved']=self.state['resolved'][-500:]
        self.state['pending']=keep
    def _refresh_sync(self):
        rows=_ctx();new=self._detect(rows);self._resolve(rows);self.state['last_new_events']=new;self._save();return rows
    async def refresh(self):
        try:rows=await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:rows=[];self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status(rows)
    def status(self,rows=None):
        rows=_ctx() if rows is None else rows;r=self.state.get('resolved') or []
        start=float(self.state.get('rule_frozen_at') or time.time());end=max([float(x.get('ts') or 0) for x in rows],default=start)
        hours=max(0.0,(end-start)/3600);per={a:sum(1 for x in r if x.get('asset')==a) for a in ASSETS}
        ow=sum(1 for x in r if float(x.get('options_net_return_pct') or 0)>0);pw=sum(1 for x in r if float(x.get('perp_net_return_pct') or 0)>0)
        gate={'collection_hours':hours,'required_hours':MIN_HOURS,'resolved_events':len(r),'required_resolved_events':MIN_RESOLVED,
              'resolved_by_asset':per,'required_per_asset':MIN_PER_ASSET,
              'ready_for_review':bool(hours>=MIN_HOURS and len(r)>=MIN_RESOLVED and all(per[a]>=MIN_PER_ASSET for a in per))}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_SHADOW','strategy':'OPTIONS_FUNDING_REGIME_MISMATCH_V1',
            'live_enabled':False,'paper_only':True,'rule_frozen_at':self.state.get('rule_frozen_at'),
            'locked_rule':{'skew_min_vol_points':SKEW_MIN,'min_abs_funding':MIN_ABS_FUNDING,'oi_lookback_minutes':30,
                'min_abs_oi_change_pct':MIN_ABS_OI_30M_PCT,'hold_hours':HOLD_SEC/3600,'cooldown_hours':COOLDOWN/3600,
                'round_trip_cost':COST,'allocation_fraction':ALLOC,'assets':list(ASSETS),'hypotheses':['FOLLOW_OPTIONS_SKEW','FOLLOW_PERP_CROWD']},
            'pending_count':len(self.state.get('pending') or []),'resolved_count':len(r),'future_gate':gate,
            'options':{'equity':self.state.get('options_equity',100.0),'return_pct':float(self.state.get('options_equity',100.0))-100,
                'max_dd_pct':self.state.get('options_max_dd_pct',0.0),'wins':ow,'win_rate':ow/len(r) if r else None},
            'perp':{'equity':self.state.get('perp_equity',100.0),'return_pct':float(self.state.get('perp_equity',100.0))-100,
                'max_dd_pct':self.state.get('perp_max_dd_pct',0.0),'wins':pw,'win_rate':pw/len(r) if r else None},
            'promotion_eligible':False,'recent_resolved':r[-12:],'last_refresh':self.last_refresh,'last_error':self.last_error,
            'policy':{'no_grid':True,'no_martingale':True,'no_dca':True,'no_historical_selection':True,'no_parameter_tuning':True,'live_requires_separate_review':True}}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='options-funding-regime-mismatch-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(300)
            if self.enabled:await self.refresh()

options_funding_regime_mismatch_shadow=OptionsFundingRegimeMismatchShadow()
