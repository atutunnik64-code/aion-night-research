from __future__ import annotations
import asyncio,json,time
from pathlib import Path
ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
SPREAD_STATE=DATA/'moex_spread_research_v1.json'; STATE=DATA/'moex_spread_paper_v1.json'
ENTRY_Z=2.0; EXIT_Z=0.5; STOP_Z=4.0; HOLD_HOURS=8.0; ASSUMED_RT_COST_BPS=10.0; INTERVAL=60.0

def _load(p,default):
    try:return json.loads(p.read_text(encoding='utf-8-sig'))
    except Exception:return default

def _src():
    s=_load(SPREAD_STATE,{});return (s.get('last') or {})

class MoexSpreadPaperV1:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None
        self.state=_load(STATE,{'version':'MOEX_SPREAD_PAPER_V1','started_at':time.time(),'pending':[],'resolved':[],'last_source_ts':None,'equity':100.0,'peak':100.0,'max_dd_pct':0.0,'last_event_ts':{}})
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _eq(self,net):
        eq=float(self.state.get('equity') or 100)+net;peak=max(float(self.state.get('peak') or 100),eq);dd=(eq/peak-1)*100 if peak else 0
        self.state['equity']=eq;self.state['peak']=peak;self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0),dd)
    def refresh(self):
        try:
            src=_src();ts=float(src.get('ts') or 0)
            if not ts or ts==self.state.get('last_source_ts'):return self.status()
            self.state['last_source_ts']=ts;now=time.time();assets={str(x.get('asset')):x for x in (src.get('assets') or [])}
            keep=[];resolved=self.state.setdefault('resolved',[])
            for p in self.state.get('pending') or []:
                a=assets.get(p['asset'])
                if not a or a.get('zscore') is None:
                    keep.append(p);continue
                z=float(a['zscore']);curve=float(a.get('curve_pct') or 0);done=abs(z)<=EXIT_Z or abs(z)>=STOP_Z or now>=float(p['due_ts'])
                if not done:
                    keep.append(p);continue
                raw=float(p['side'])*(curve-float(p['entry_curve_pct']));net=raw-ASSUMED_RT_COST_BPS/100.0
                out={**p,'exit_curve_pct':curve,'exit_z':z,'resolved_at':now,'gross_return_pct':raw,'net_return_pct':net};resolved.append(out);self._eq(net)
            self.state['pending']=keep;self.state['resolved']=resolved[-5000:]
            new=0
            if src.get('gate_ready'):
                active={p['asset'] for p in self.state.get('pending') or []}
                for a in src.get('assets') or []:
                    name=str(a.get('asset') or '');z=a.get('zscore')
                    if not name or z is None or abs(float(z))<ENTRY_Z or name in active:continue
                    prev=float((self.state.get('last_event_ts') or {}).get(name) or 0)
                    if now-prev<3600:continue
                    side=-1 if float(z)>0 else 1
                    p={'id':f'{name}:{int(now)}','asset':name,'front':a.get('front'),'next':a.get('next'),'opened_at':now,'due_ts':now+HOLD_HOURS*3600,
                       'entry_curve_pct':float(a.get('curve_pct') or 0),'entry_z':float(z),'side':side,'round_trip_cost_bps':ASSUMED_RT_COST_BPS}
                    self.state.setdefault('pending',[]).append(p);self.state.setdefault('last_event_ts',{})[name]=now;active.add(name);new+=1
            self.state['last_new_events']=new;self._save();self.last_error=None;return self.status()
        except Exception as exc:self.last_error=str(exc)[:500];return self.status()
    def status(self):
        src=_src();gate=bool(src.get('gate_ready'));pending=self.state.get('pending') or [];resolved=self.state.get('resolved') or []
        return {'ok':self.last_error is None,'strategy':'MOEX_SPREAD_PAPER_V1','mode':'FUTURE_ONLY_PAPER','paper_only':True,'live_enabled':False,
                'gate_ready':gate,'pending':len(pending),'resolved':len(resolved),'wins':sum(1 for x in resolved if float(x.get('net_return_pct') or 0)>0),'last_new_events':self.state.get('last_new_events',0),
                'equity':round(float(self.state.get('equity') or 100),5),'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0),5),
                'locked_rule':{'entry_abs_z':ENTRY_Z,'exit_abs_z':EXIT_Z,'stop_abs_z':STOP_Z,'hold_hours':HOLD_HOURS},
                'cost_model':{'assumed_round_trip_bps':ASSUMED_RT_COST_BPS,'status':'PLACEHOLDER_UNTIL_BROKER_FEES_CONFIGURED'},
                'promotion_blocked':True,'blocking_reason':'BROKER_SPECIFIC_MOEX_FEES_NOT_CONFIGURED_AND_BASELINE_GATE_REQUIRED',
                'policy':{'no_entry_before_gate':True,'two_leg_spread_only':True,'no_live_orders':True,'no_grid':True,'no_martingale':True,'no_dca':True},'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        self.refresh();self.task=asyncio.create_task(self._loop(),name='moex-spread-paper-v1')
    async def stop(self):
        if self.task and not self.task.done():self.task.cancel();await asyncio.gather(self.task,return_exceptions=True)
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(INTERVAL)
            if self.enabled:self.refresh()

moex_spread_paper_v1=MoexSpreadPaperV1()
