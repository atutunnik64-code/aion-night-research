from __future__ import annotations
import asyncio,json,time
from pathlib import Path

ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
SNAPS=DATA/'liquidation_context_snapshots_v1.jsonl'
STATE=DATA/'funding_oi_state_transition_shadow_v1.json'
CORE=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT','SUIUSDT','AAVEUSDT']
MIN_ABS_FUNDING=.00010;MIN_ABS_OI_30M_PCT=.50
LOOKBACK_SEC=1800;HOLD_SEC=14400;COOLDOWN_SEC=14400
ROUND_TRIP_COST=.0012;ALLOC=.05;MIN_HOURS=72.;MIN_RESOLVED=30;MIN_SYMBOLS=6
STRESS_COST_MULTS=(2.0,3.0)

def _lines():
    if not SNAPS.exists():return []
    raw=SNAPS.read_text(encoding='utf-8').splitlines()[-50000:];out=[]
    for line in raw:
        try:
            x=json.loads(line)
            if str(x.get('symbol') or '') in CORE:out.append(x)
        except Exception:pass
    return sorted(out,key=lambda x:float(x.get('ts') or 0))

def _blank():
    now=time.time();return {'version':'FUNDING_OI_STATE_TRANSITION_V1','rule_frozen_at':now,
        'last_processed_ts':now,'last_event_ts':{},'pending':[],'resolved':[],
        'follow_equity':100.0,'follow_peak':100.0,'follow_max_dd_pct':0.0,
        'fade_equity':100.0,'fade_peak':100.0,'fade_max_dd_pct':0.0}

def _stress_return_pct(rows,name,cost_mult):
    eq=100.0
    for x in rows:
        entry=float(x.get('entry') or 0);exit_px=float(x.get('exit') or 0)
        if entry<=0 or exit_px<=0:continue
        side=float(x.get(name+'_side') or 0);gross=side*(exit_px/entry-1.0)
        funding=side*float(x.get('funding_rate') or 0) if x.get('funding_crossed') else 0.0
        cost=float(x.get('round_trip_cost') or ROUND_TRIP_COST)*float(cost_mult)
        eq*=1.0+ALLOC*(gross-cost-funding)
    return eq-100.0

class FundingOiStateTransitionShadow:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state=_blank()
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _old(rows,target):
        for x in reversed(rows):
            if float(x.get('ts') or 0)<=target:return x
        return None
    def _detect(self,rows):
        by={s:[] for s in CORE}
        for x in rows:
            s=str(x.get('symbol') or '')
            if s in by:by[s].append(x)
        last=float(self.state.get('last_processed_ts') or self.state.get('rule_frozen_at') or 0);new=0;max_ts=last
        for s,sr in by.items():
            for x in sr:
                ts=float(x.get('ts') or 0);max_ts=max(max_ts,ts)
                if ts<=last or ts<float(self.state.get('rule_frozen_at') or 0):continue
                fund=float(x.get('funding_rate') or 0);entry=float(x.get('mark_price') or 0)
                if abs(fund)<MIN_ABS_FUNDING or entry<=0:continue
                old=self._old(sr,ts-LOOKBACK_SEC)
                if not old:continue
                oi0=float(old.get('open_interest') or 0);oi1=float(x.get('open_interest') or 0)
                if oi0<=0 or oi1<=0:continue
                oi_pct=(oi1/oi0-1.0)*100
                if abs(oi_pct)<MIN_ABS_OI_30M_PCT:continue
                prev=float((self.state.get('last_event_ts') or {}).get(s) or 0)
                if ts-prev<COOLDOWN_SEC:continue
                crowd_side=1.0 if fund>0 else -1.0
                self.state.setdefault('pending',[]).append({'id':f'{s}:{int(ts)}','symbol':s,'ts':ts,
                    'event_type':'OI_BUILD' if oi_pct>0 else 'OI_UNWIND','oi_30m_pct':oi_pct,'funding_rate':fund,
                    'follow_side':crowd_side,'fade_side':-crowd_side,'entry':entry,'due_ts':ts+HOLD_SEC,
                    'next_funding_time':int(x.get('next_funding_time') or 0),'round_trip_cost':ROUND_TRIP_COST})
                self.state.setdefault('last_event_ts',{})[s]=ts;new+=1
        self.state['last_processed_ts']=max_ts
        return new
    @staticmethod
    def _exit_row(rows,sym,due):
        for x in rows:
            if x.get('symbol')==sym and float(x.get('ts') or 0)>=due:return x
        return None
    def _resolve(self,rows):
        keep=[]
        for p in self.state.get('pending') or []:
            ex=self._exit_row(rows,p['symbol'],float(p['due_ts']))
            if not ex:keep.append(p);continue
            exit_px=float(ex.get('mark_price') or 0)
            if exit_px<=0:keep.append(p);continue
            crossed=int(p.get('next_funding_time') or 0)>0 and float(p['ts'])*1000<int(p['next_funding_time'])<=float(ex.get('ts') or 0)*1000
            ret=exit_px/float(p['entry'])-1.0;fund=float(p.get('funding_rate') or 0)
            out={**p,'exit':exit_px,'exit_ts':float(ex.get('ts') or 0),'funding_crossed':crossed}
            for name in ('follow','fade'):
                side=float(p[name+'_side']);f=side*fund if crossed else 0.0
                net=side*ret-ROUND_TRIP_COST-f;out[name+'_net_return_pct']=net*100
                eqk=name+'_equity';peakk=name+'_peak';ddk=name+'_max_dd_pct'
                eq=float(self.state.get(eqk) or 100)*(1+ALLOC*net);peak=max(float(self.state.get(peakk) or 100),eq)
                self.state[eqk]=eq;self.state[peakk]=peak;self.state[ddk]=min(float(self.state.get(ddk) or 0),(eq/peak-1)*100)
                out[name+'_paper_equity']=eq
            self.state.setdefault('resolved',[]).append(out);self.state['resolved']=self.state['resolved'][-1000:]
        self.state['pending']=keep
    def _refresh_sync(self):
        rows=_lines();new=self._detect(rows);self._resolve(rows);self.state['last_new_events']=new;self._save();return rows
    async def refresh(self):
        try:rows=await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:rows=[];self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status(rows)
    def status(self,rows=None):
        rows=_lines() if rows is None else rows;r=self.state.get('resolved') or []
        start=float(self.state.get('rule_frozen_at') or time.time());end=max([float(x.get('ts') or 0) for x in rows],default=start)
        hours=max(0.0,(end-start)/3600);syms=sorted({x.get('symbol') for x in r if x.get('symbol')})
        follow_wins=sum(1 for x in r if float(x.get('follow_net_return_pct') or 0)>0);fade_wins=sum(1 for x in r if float(x.get('fade_net_return_pct') or 0)>0)
        follow_stress={f'{int(m)}x':_stress_return_pct(r,'follow',m) for m in STRESS_COST_MULTS}
        fade_stress={f'{int(m)}x':_stress_return_pct(r,'fade',m) for m in STRESS_COST_MULTS}
        sample_ready=bool(hours>=MIN_HOURS and len(r)>=MIN_RESOLVED and len(syms)>=MIN_SYMBOLS)
        hypothesis_ready={'follow':bool(sample_ready and follow_stress['2x']>0 and follow_stress['3x']>0),
                          'fade':bool(sample_ready and fade_stress['2x']>0 and fade_stress['3x']>0)}
        gate={'collection_hours':hours,'required_hours':MIN_HOURS,'resolved_events':len(r),'required_resolved_events':MIN_RESOLVED,
              'resolved_symbols':len(syms),'required_symbols':MIN_SYMBOLS,'sample_ready':sample_ready,
              'follow_stress_return_pct':follow_stress,'fade_stress_return_pct':fade_stress,
              'hypothesis_ready':hypothesis_ready,'selection_policy':'NO_AUTOMATIC_WINNER',
              'ready_for_review':bool(hypothesis_ready['follow'] or hypothesis_ready['fade'])}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_SHADOW','strategy':'FUNDING_OI_STATE_TRANSITION_V1',
            'live_enabled':False,'paper_only':True,'rule_frozen_at':self.state.get('rule_frozen_at'),
            'locked_rule':{'min_abs_funding':MIN_ABS_FUNDING,'oi_lookback_minutes':30,'min_abs_oi_change_pct':MIN_ABS_OI_30M_PCT,
                           'hold_hours':HOLD_SEC/3600,'cooldown_hours':COOLDOWN_SEC/3600,'round_trip_cost':ROUND_TRIP_COST,
                           'allocation_fraction':ALLOC,'hypotheses':['FOLLOW_FUNDING_SIDE','FADE_FUNDING_SIDE']},
            'pending_count':len(self.state.get('pending') or []),'resolved_count':len(r),'resolved_symbols':syms,'future_gate':gate,
            'follow':{'equity':self.state.get('follow_equity',100.0),'return_pct':float(self.state.get('follow_equity',100.0))-100,
                      'max_dd_pct':self.state.get('follow_max_dd_pct',0.0),'wins':follow_wins,'win_rate':follow_wins/len(r) if r else None,
                      'cost_stress_return_pct':follow_stress},
            'fade':{'equity':self.state.get('fade_equity',100.0),'return_pct':float(self.state.get('fade_equity',100.0))-100,
                    'max_dd_pct':self.state.get('fade_max_dd_pct',0.0),'wins':fade_wins,'win_rate':fade_wins/len(r) if r else None,
                    'cost_stress_return_pct':fade_stress},
            'promotion_eligible':False,'recent_resolved':r[-12:],'last_refresh':self.last_refresh,'last_error':self.last_error,
            'policy':{'no_grid':True,'no_martingale':True,'no_dca':True,'no_backfill_selection':True,'no_parameter_tuning':True,'live_requires_separate_review':True}}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='funding-oi-state-transition-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(60)
            if self.enabled:await self.refresh()

funding_oi_state_transition_shadow=FundingOiStateTransitionShadow()
