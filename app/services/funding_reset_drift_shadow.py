from __future__ import annotations
import asyncio,json,time
from pathlib import Path

ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
SNAPS=DATA/'liquidation_context_snapshots_v1.jsonl'
STATE=DATA/'funding_reset_drift_shadow_v1.json'
SYMS=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT','SUIUSDT','AAVEUSDT']
MIN_ABS_FUNDING=.00010;HOLD=3600;COST=.0012;ALLOC=.05
MIN_HOURS=72.;MIN_RESOLVED=30;MIN_SYMBOLS=6;MIN_PER_SIGN=5
STRESS=(2.0,3.0)

def _rows():
    if not SNAPS.exists():return []
    out=[]
    for line in SNAPS.read_text(encoding='utf-8').splitlines()[-50000:]:
        try:
            x=json.loads(line)
            if str(x.get('symbol') or '') in SYMS:out.append(x)
        except Exception:pass
    return sorted(out,key=lambda x:float(x.get('ts') or 0))
def _blank():
    now=time.time();return {'version':'FUNDING_RESET_DRIFT_V1','rule_frozen_at':now,'last_processed_ts':now,
        'last_event_nft':{},'pending':[],'resolved':[],'revert_equity':100.0,'revert_peak':100.0,'revert_max_dd_pct':0.0,
        'continue_equity':100.0,'continue_peak':100.0,'continue_max_dd_pct':0.0}

def _stress(rows,name,mult):
    eq=100.0
    for x in rows:
        base=float(x.get(name+'_net_return_pct') or 0)/100.0
        cost=float(x.get('round_trip_cost') or COST)
        eq*=1.0+float(x.get('allocation_fraction') or ALLOC)*(base-(float(mult)-1.0)*cost)
    return eq-100.0

def _exit(rows,sym,due):
    for x in rows:
        if x.get('symbol')==sym and float(x.get('ts') or 0)>=due:return x
    return None

class FundingResetDriftShadow:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state=_blank()
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _detect(self,rows):
        by={s:[] for s in SYMS}
        for x in rows:by[str(x.get('symbol') or '')].append(x) if str(x.get('symbol') or '') in by else None
        last=float(self.state.get('last_processed_ts') or 0);new=0;max_ts=last
        for sym,sr in by.items():
            prev=None
            for x in sr:
                ts=float(x.get('ts') or 0);max_ts=max(max_ts,ts)
                if ts<=last:
                    prev=x;continue
                if prev is not None:
                    old_nft=int(prev.get('next_funding_time') or 0);new_nft=int(x.get('next_funding_time') or 0)
                    crossed=old_nft>0 and new_nft>old_nft and ts*1000>=old_nft
                    funding=float(prev.get('funding_rate') or 0);entry=float(x.get('mark_price') or 0)
                    seen=int((self.state.get('last_event_nft') or {}).get(sym) or 0)
                    if crossed and old_nft>seen and abs(funding)>=MIN_ABS_FUNDING and entry>0:
                        crowd=1.0 if funding>0 else -1.0
                        self.state.setdefault('pending',[]).append({'id':f'{sym}:{old_nft}','symbol':sym,'ts':ts,'funding_time_ms':old_nft,
                            'pre_funding_rate':funding,'entry':entry,'due_ts':ts+HOLD,'revert_side':-crowd,'continue_side':crowd,
                            'round_trip_cost':COST,'allocation_fraction':ALLOC})
                        self.state.setdefault('last_event_nft',{})[sym]=old_nft;new+=1
                prev=x
        self.state['last_processed_ts']=max_ts;return new
    def _resolve(self,rows):
        keep=[]
        for p in self.state.get('pending') or []:
            ex=_exit(rows,p['symbol'],float(p['due_ts']))
            if not ex:keep.append(p);continue
            px=float(ex.get('mark_price') or 0)
            if px<=0:keep.append(p);continue
            ret=px/float(p['entry'])-1.0;out={**p,'exit':px,'exit_ts':float(ex.get('ts') or 0)}
            for name in ('revert','continue'):
                side=float(p[name+'_side']);net=side*ret-COST;out[name+'_net_return_pct']=net*100
                eqk=name+'_equity';peakk=name+'_peak';ddk=name+'_max_dd_pct'
                eq=float(self.state.get(eqk) or 100)*(1+ALLOC*net);peak=max(float(self.state.get(peakk) or 100),eq)
                self.state[eqk]=eq;self.state[peakk]=peak;self.state[ddk]=min(float(self.state.get(ddk) or 0),(eq/peak-1)*100)
                out[name+'_paper_equity']=eq
            self.state.setdefault('resolved',[]).append(out);self.state['resolved']=self.state['resolved'][-1000:]
        self.state['pending']=keep
    def _refresh_sync(self):
        rows=_rows();self.state['last_new_events']=self._detect(rows);self._resolve(rows);self._save();return rows
    async def refresh(self):
        try:rows=await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:rows=[];self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status(rows)
    def status(self,rows=None):
        rows=_rows() if rows is None else rows;r=self.state.get('resolved') or []
        freeze=float(self.state.get('rule_frozen_at') or time.time());end=max([float(x.get('ts') or 0) for x in rows],default=freeze)
        hours=max(0.0,(end-freeze)/3600);syms=sorted({x.get('symbol') for x in r if x.get('symbol')})
        signs={s:sum(1 for x in r if (1 if float(x.get('pre_funding_rate') or 0)>0 else -1)==s) for s in (-1,1)}
        rw=sum(1 for x in r if float(x.get('revert_net_return_pct') or 0)>0);cw=sum(1 for x in r if float(x.get('continue_net_return_pct') or 0)>0)
        rst={f'{int(m)}x':_stress(r,'revert',m) for m in STRESS};cst={f'{int(m)}x':_stress(r,'continue',m) for m in STRESS}
        sample=bool(hours>=MIN_HOURS and len(r)>=MIN_RESOLVED and len(syms)>=MIN_SYMBOLS and all(signs[s]>=MIN_PER_SIGN for s in signs))
        ready={'revert':bool(sample and rst['2x']>0 and rst['3x']>0),'continue':bool(sample and cst['2x']>0 and cst['3x']>0)}
        gate={'collection_hours':hours,'required_hours':MIN_HOURS,'resolved_events':len(r),'required_resolved_events':MIN_RESOLVED,
              'resolved_symbols':len(syms),'required_symbols':MIN_SYMBOLS,'funding_sign_events':{'NEGATIVE':signs[-1],'POSITIVE':signs[1]},
              'required_per_sign':MIN_PER_SIGN,'sample_ready':sample,'revert_stress_return_pct':rst,'continue_stress_return_pct':cst,
              'hypothesis_ready':ready,'selection_policy':'NO_AUTOMATIC_WINNER','ready_for_review':bool(ready['revert'] or ready['continue'])}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_SHADOW','strategy':'FUNDING_RESET_DRIFT_V1',
            'live_enabled':False,'paper_only':True,'rule_frozen_at':self.state.get('rule_frozen_at'),'pending_count':len(self.state.get('pending') or []),'resolved_count':len(r),
            'future_gate':gate,'revert':{'equity':self.state.get('revert_equity',100.0),'return_pct':float(self.state.get('revert_equity',100.0))-100,'wins':rw,'win_rate':rw/len(r) if r else None,'cost_stress_return_pct':rst},
            'continue':{'equity':self.state.get('continue_equity',100.0),'return_pct':float(self.state.get('continue_equity',100.0))-100,'wins':cw,'win_rate':cw/len(r) if r else None,'cost_stress_return_pct':cst},
            'promotion_eligible':False,'last_refresh':self.last_refresh,'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='funding-reset-drift-shadow')
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

funding_reset_drift_shadow=FundingResetDriftShadow()
