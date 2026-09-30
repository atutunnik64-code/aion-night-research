from __future__ import annotations
import json,time
from pathlib import Path
ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
SPREAD_STATE=DATA/'moex_spread_research_v1.json'; STATE=DATA/'moex_spread_paper_v1.json'
ENTRY_Z=2.0; EXIT_Z=0.5; STOP_Z=4.0; HOLD_HOURS=8.0; ASSUMED_RT_COST_BPS=10.0

def _load(p,default):
    try:return json.loads(p.read_text(encoding='utf-8'))
    except Exception:return default

def _src():
    s=_load(SPREAD_STATE,{}); return (s.get('last') or {})

class MoexSpreadPaperV1:
    def __init__(self):
        self.state=_load(STATE,{'version':'MOEX_SPREAD_PAPER_V1','started_at':time.time(),'pending':[],'resolved':[],'last_source_ts':None}); self.last_error=None
    def _save(self): STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def refresh(self):
        try:
            src=_src(); ts=float(src.get('ts') or 0)
            if not ts or ts==self.state.get('last_source_ts'): return self.status()
            self.state['last_source_ts']=ts
            if not src.get('gate_ready'):
                self._save(); return self.status()
            self._save(); return self.status()
        except Exception as exc:self.last_error=str(exc)[:500]; return self.status()
    def status(self):
        src=_src(); gate=bool(src.get('gate_ready')); pending=self.state.get('pending') or []; resolved=self.state.get('resolved') or []
        return {'ok':self.last_error is None,'strategy':'MOEX_SPREAD_PAPER_V1','mode':'FUTURE_ONLY_PAPER','paper_only':True,'live_enabled':False,
                'gate_ready':gate,'pending':len(pending),'resolved':len(resolved),'wins':sum(1 for x in resolved if float(x.get('net_return_pct') or 0)>0),
                'locked_rule':{'entry_abs_z':ENTRY_Z,'exit_abs_z':EXIT_Z,'stop_abs_z':STOP_Z,'hold_hours':HOLD_HOURS},
                'cost_model':{'assumed_round_trip_bps':ASSUMED_RT_COST_BPS,'status':'PLACEHOLDER_UNTIL_BROKER_FEES_CONFIGURED'},
                'promotion_blocked':True,'blocking_reason':'BROKER_SPECIFIC_MOEX_FEES_NOT_CONFIGURED_AND_BASELINE_GATE_REQUIRED',
                'policy':{'no_entry_before_gate':True,'two_leg_spread_only':True,'no_live_orders':True,'no_grid':True,'no_martingale':True,'no_dca':True},'last_error':self.last_error}

moex_spread_paper_v1=MoexSpreadPaperV1()
