from __future__ import annotations
import json,time
from pathlib import Path
ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
STATE=DATA/'funding_dislocation_paper_v1.json'
HOLD_HOURS=8.0; COST=0.0012; ALLOC=0.05

def _load():
    try:return json.loads(STATE.read_text(encoding='utf-8'))
    except Exception:return {'version':'FUNDING_DISLOCATION_PAPER_V1','started_at':time.time(),'pending':[],'resolved':[],'active_episodes':{},'equity':100.0,'equity_2x':100.0,'equity_3x':100.0,'peak':100.0,'max_dd_pct':0.0}
def _v(snap,sym,venue):return (((snap.get('symbols') or {}).get(sym) or {}).get('venues') or {}).get(venue) or {}
def _fts(x):
    x=float(x or 0); return x/1000.0 if x>1e11 else x

class FundingDislocationPaperV1:
    def __init__(self):self.state=_load(); self.last_error=None
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _open(self,snap,q):
        ts=float(snap.get('ts') or 0); sym=q['symbol']; lv=_v(snap,sym,q['long']); sv=_v(snap,sym,q['short'])
        if not ts or not lv.get('ask') or not sv.get('bid'):return None
        eid=f"{sym}|{q['long']}|{q['short']}|{int(ts)}"
        return {'id':eid,'symbol':sym,'long':q['long'],'short':q['short'],'entry_ts':ts,'due_ts':ts+HOLD_HOURS*3600,'entry_long':float(lv['ask']),'entry_short':float(sv['bid']),'long_funding_rate':float(lv.get('funding_rate') or 0),'short_funding_rate':float(sv.get('funding_rate') or 0),'long_next_funding':_fts(lv.get('next_funding_time') or lv.get('funding_time')),'short_next_funding':_fts(sv.get('next_funding_time') or sv.get('funding_time')),'basis_bps':q.get('basis_bps'),'spread_per_hour':q.get('spread_per_hour'),'persistence':q.get('persistence')}
    def refresh(self,snap,qualified):
        try:
            ts=float(snap.get('ts') or 0); pending=list(self.state.get('pending') or []); resolved=list(self.state.get('resolved') or [])
            still=[]
            for e in pending:
                if ts<float(e['due_ts']):still.append(e);continue
                lv=_v(snap,e['symbol'],e['long']); sv=_v(snap,e['symbol'],e['short'])
                if not lv.get('bid') or not sv.get('ask'):still.append(e);continue
                lr=(float(lv['bid'])-e['entry_long'])/e['entry_long']; sr=(e['entry_short']-float(sv['ask']))/e['entry_short']
                lf=-e['long_funding_rate'] if e['long_next_funding'] and e['long_next_funding']<=ts else 0.0
                sf= e['short_funding_rate'] if e['short_next_funding'] and e['short_next_funding']<=ts else 0.0
                gross=.5*(lr+sr+lf+sf)
                e.update({'exit_ts':ts,'exit_long':float(lv['bid']),'exit_short':float(sv['ask']),'long_price_return':lr,'short_price_return':sr,'long_funding_cashflow':lf,'short_funding_cashflow':sf,'gross_return':gross,'net_return':gross-COST,'net_2x_cost':gross-2*COST,'net_3x_cost':gross-3*COST})
                resolved.append(e)
                for k,f in [('equity','net_return'),('equity_2x','net_2x_cost'),('equity_3x','net_3x_cost')]:self.state[k]=float(self.state.get(k) or 100.0)*(1+ALLOC*float(e[f]))
                self.state['peak']=max(float(self.state.get('peak') or 100.0),float(self.state['equity'])); self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0.0),(float(self.state['equity'])/float(self.state['peak'])-1)*100)
            self.state['pending']=still; self.state['resolved']=resolved[-500:]
            active=dict(self.state.get('active_episodes') or {}); nowkeys=set()
            for q in qualified or []:
                key='|'.join((q['symbol'],q['long'],q['short'])); nowkeys.add(key)
                if key not in active:
                    e=self._open(snap,q)
                    if e:self.state['pending'].append(e); active[key]=e['id']
            for k in list(active):
                if k not in nowkeys:active.pop(k,None)
            self.state['active_episodes']=active; self.state['last_refresh']=time.time(); self._save(); self.last_error=None
        except Exception as e:self.last_error=str(e)[:500]
        return self.status()
    def status(self):
        r=list(self.state.get('resolved') or []); n=len(r); wins=sum(1 for x in r if float(x.get('net_return') or 0)>0)
        return {'ok':self.last_error is None,'strategy':'FUNDING_DISLOCATION_PAPER_V1','mode':'FUTURE_ONLY_PAPER','live_enabled':False,'paper_only':True,'hold_hours':HOLD_HOURS,'allocation_fraction':ALLOC,'round_trip_cost':COST,'pending':len(self.state.get('pending') or []),'resolved':n,'wins':wins,'win_rate':wins/n if n else None,'equity':self.state.get('equity'),'return_pct':float(self.state.get('equity') or 100)-100,'max_dd_pct':self.state.get('max_dd_pct'),'equity_2x':self.state.get('equity_2x'),'equity_3x':self.state.get('equity_3x'),'recent_resolved':r[-20:],'last_error':self.last_error,'policy':{'entry_after_persistence_only':True,'bid_ask_execution':True,'funding_entry_rate_estimate':True,'no_grid':True,'no_martingale':True,'no_dca':True,'no_live_orders':True}}
funding_dislocation_paper_v1=FundingDislocationPaperV1()
