from __future__ import annotations
import asyncio,json,time
from pathlib import Path

ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
BIN=DATA/'liquidation_context_snapshots_v1.jsonl';BY=DATA/'crossvenue_bybit_liq_context_v1.jsonl'
STATE=DATA/'crossvenue_oi_migration_shadow_v2.json'
CORE=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT','SUIUSDT','AAVEUSDT']
LOOKBACK=1800;MIN_SHARE_SHIFT=.005;MIN_TOTAL_OI_GROWTH_PCT=1.0;MIN_ABS_INFLOW_FUNDING=.00005
HOLD=14400;COOLDOWN=14400;COST=.0012;ALLOC=.05
MIN_HOURS=72.;MIN_RESOLVED=30;MIN_SYMBOLS=6;MIN_PER_VENUE=5

def _lines(path,limit=40000):
    if not path.exists():return []
    out=[]
    for line in path.read_text(encoding='utf-8').splitlines()[-limit:]:
        try:out.append(json.loads(line))
        except Exception:pass
    return sorted(out,key=lambda x:float(x.get('ts') or 0))

def _blank():
    now=time.time();return {'version':'CROSSVENUE_OI_MIGRATION_V2','rule_frozen_at':now,'last_processed_ts':now,
        'last_event_ts':{},'pending':[],'resolved':[],'follow_equity':100.0,'follow_peak':100.0,'follow_max_dd_pct':0.0,
        'fade_equity':100.0,'fade_peak':100.0,'fade_max_dd_pct':0.0}

def _latest(rows,sym,ts):
    z=[x for x in rows if x.get('symbol')==sym and float(x.get('ts') or 0)<=ts]
    return z[-1] if z else None

def _old(rows,sym,ts):
    z=[x for x in rows if x.get('symbol')==sym and float(x.get('ts') or 0)<=ts-LOOKBACK]
    return z[-1] if z else None

def _oi_usd(row):
    oi=float(row.get('open_interest') or 0);px=float(row.get('mark_price') or 0)
    return oi*px if oi>0 and px>0 else 0.0

def _exit(rows,sym,due):
    for x in rows:
        if x.get('symbol')==sym and float(x.get('ts') or 0)>=due:return x
    return None

class CrossVenueOIMigrationShadowV2:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state=_blank()
    def _save(self):
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _detect(self,br,yr):
        new=0
        last=float(self.state.get('last_processed_ts') or 0)
        ts_candidates=sorted({float(x.get('ts') or 0) for x in br if float(x.get('ts') or 0)>last})
        for ts in ts_candidates:
            for sym in CORE:
                bn=_latest(br,sym,ts);bo=_old(br,sym,ts);yn=_latest(yr,sym,ts);yo=_old(yr,sym,ts)
                if not all((bn,bo,yn,yo)):continue
                bn_oi,bo_oi,yn_oi,yo_oi=map(_oi_usd,(bn,bo,yn,yo))
                cur=bn_oi+yn_oi;old=bo_oi+yo_oi
                if cur<=0 or old<=0:continue
                share_now=bn_oi/cur;share_old=bo_oi/old;shift=share_now-share_old
                growth=(cur/old-1.0)*100
                if abs(shift)<MIN_SHARE_SHIFT or growth<MIN_TOTAL_OI_GROWTH_PCT:continue
                inflow='Binance' if shift>0 else 'Bybit';src=bn if inflow=='Binance' else yn
                funding=float(src.get('funding_rate') or 0)
                if abs(funding)<MIN_ABS_INFLOW_FUNDING:continue
                prev=float((self.state.get('last_event_ts') or {}).get(sym) or 0)
                if ts-prev<COOLDOWN:continue
                entry=float(bn.get('mark_price') or 0)
                if entry<=0:continue
                crowd=1.0 if funding>0 else -1.0
                self.state.setdefault('pending',[]).append({
                    'id':f'{sym}:{int(ts)}','symbol':sym,'ts':ts,'due_ts':ts+HOLD,
                    'inflow_venue':inflow,'binance_oi_usd':bn_oi,'bybit_oi_usd':yn_oi,
                    'binance_share':share_now,'binance_share_shift_30m':shift,
                    'total_oi_growth_30m_pct':growth,'inflow_funding':funding,'entry':entry,
                    'follow_side':crowd,'fade_side':-crowd,'binance_funding':float(bn.get('funding_rate') or 0),
                    'next_funding_time':int(bn.get('next_funding_time') or 0),
                    'round_trip_cost':COST,'allocation_fraction':ALLOC})
                self.state.setdefault('last_event_ts',{})[sym]=ts;new+=1
            self.state['last_processed_ts']=ts
        return new

    def _resolve(self,br):
        keep=[]
        for p in self.state.get('pending') or []:
            ex=_exit(br,p['symbol'],float(p['due_ts']))
            if not ex:keep.append(p);continue
            exit_px=float(ex.get('mark_price') or 0)
            if exit_px<=0:keep.append(p);continue
            r=exit_px/float(p['entry'])-1.0
            crossed=int(p.get('next_funding_time') or 0)>0 and float(p['ts'])*1000<int(p['next_funding_time'])<=float(ex.get('ts') or 0)*1000
            out={**p,'exit':exit_px,'exit_ts':float(ex.get('ts') or 0),'funding_crossed':crossed}
            for name in ('follow','fade'):
                side=float(p[name+'_side'])
                fund=side*float(p.get('binance_funding') or 0) if crossed else 0.0
                net=side*r-COST-fund;out[name+'_net_return_pct']=net*100
                eqk=name+'_equity';peakk=name+'_peak';ddk=name+'_max_dd_pct'
                eq=float(self.state.get(eqk) or 100)*(1+ALLOC*net)
                peak=max(float(self.state.get(peakk) or 100),eq)
                self.state[eqk]=eq;self.state[peakk]=peak
                self.state[ddk]=min(float(self.state.get(ddk) or 0),(eq/peak-1)*100)
                out[name+'_paper_equity']=eq
            self.state.setdefault('resolved',[]).append(out)
            self.state['resolved']=self.state['resolved'][-1000:]
        self.state['pending']=keep

    def _refresh_sync(self):
        br=_lines(BIN);yr=_lines(BY)
        self.state['last_new_events']=self._detect(br,yr)
        self._resolve(br);self._save();return br,yr

    async def refresh(self):
        try:br,yr=await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:br=[];yr=[];self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status(br,yr)
    def status(self,br=None,yr=None):
        br=_lines(BIN) if br is None else br;yr=_lines(BY) if yr is None else yr
        r=self.state.get('resolved') or [];start=float(self.state.get('rule_frozen_at') or time.time())
        ends=[float(x.get('ts') or 0) for x in br+yr]
        hours=max(0.0,((max(ends) if ends else start)-start)/3600)
        syms=sorted({x.get('symbol') for x in r if x.get('symbol')})
        venues={v:sum(1 for x in r if x.get('inflow_venue')==v) for v in ('Binance','Bybit')}
        fw=sum(1 for x in r if float(x.get('follow_net_return_pct') or 0)>0)
        dw=sum(1 for x in r if float(x.get('fade_net_return_pct') or 0)>0)
        gate={'collection_hours':hours,'required_hours':MIN_HOURS,'resolved_events':len(r),
              'required_resolved_events':MIN_RESOLVED,'resolved_symbols':len(syms),'required_symbols':MIN_SYMBOLS,
              'inflow_venues':venues,'required_per_inflow_venue':MIN_PER_VENUE,
              'ready_for_review':bool(hours>=MIN_HOURS and len(r)>=MIN_RESOLVED and len(syms)>=MIN_SYMBOLS and all(venues[v]>=MIN_PER_VENUE for v in venues))}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_SHADOW',
            'strategy':'CROSSVENUE_OI_MIGRATION_V2','live_enabled':False,'paper_only':True,
            'rule_frozen_at':self.state.get('rule_frozen_at'),
            'locked_rule':{'oi_lookback_minutes':30,'min_venue_share_shift':MIN_SHARE_SHIFT,
                'min_total_oi_growth_pct':MIN_TOTAL_OI_GROWTH_PCT,'min_abs_inflow_funding':MIN_ABS_INFLOW_FUNDING,
                'hold_hours':HOLD/3600,'cooldown_hours':COOLDOWN/3600,'round_trip_cost':COST,
                'allocation_fraction':ALLOC,'hypotheses':['FOLLOW_INFLOW_CROWD','FADE_INFLOW_CROWD']},
            'pending_count':len(self.state.get('pending') or []),'resolved_count':len(r),'future_gate':gate,
            'follow':{'equity':self.state.get('follow_equity',100.0),
                'return_pct':float(self.state.get('follow_equity',100.0))-100,
                'max_dd_pct':self.state.get('follow_max_dd_pct',0.0),'wins':fw,'win_rate':fw/len(r) if r else None},
            'fade':{'equity':self.state.get('fade_equity',100.0),
                'return_pct':float(self.state.get('fade_equity',100.0))-100,
                'max_dd_pct':self.state.get('fade_max_dd_pct',0.0),'wins':dw,'win_rate':dw/len(r) if r else None},
            'promotion_eligible':False,'recent_resolved':r[-12:],'last_refresh':self.last_refresh,
            'last_error':self.last_error,'policy':{'no_grid':True,'no_martingale':True,'no_dca':True,
                'no_historical_selection':True,'no_parameter_tuning':True,'live_requires_separate_review':True}}

    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='crossvenue-oi-migration-shadow-v2')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(300)

crossvenue_oi_migration_shadow_v2=CrossVenueOIMigrationShadowV2()



