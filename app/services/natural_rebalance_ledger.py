from __future__ import annotations
import json,time
from collections import defaultdict,deque
from app.services.execution_engine import execution_engine
from app.services.live_executor import live_executor

class NaturalRebalanceLedger:
    def __init__(self):
        self.last_error=None; self.last_refresh=None; self.latest={}; self.residual=[]
    @staticmethod
    def _parse(r):
        try:d=json.loads(r['payload'] or '{}')
        except Exception:return None
        row=d.get('row') or {}; steps=row.get('steps') or d.get('legs') or []
        buy=next((x for x in steps if str(x.get('side') or '').lower()=='buy'),{})
        sell=next((x for x in steps if str(x.get('side') or '').lower()=='sell'),{})
        bv=str(row.get('buy_venue') or buy.get('venue') or ''); sv=str(row.get('sell_venue') or sell.get('venue') or '')
        base=str(row.get('base') or buy.get('to_asset') or sell.get('from_asset') or buy.get('base') or '').upper()
        if not bv or not sv or not base or bv==sv:return None
        states=d.get('order_states') or []; qty=float(d.get('matched_qty') or 0)
        if qty<=0 and states:
            vals=[float(x.get('filled_qty') or 0) for x in states if isinstance(x,dict)]
            if vals:qty=min(vals)
        if qty<=0:qty=float(row.get('base_bought') or 0)
        if qty<=0:return None
        return {'id':int(r['id']),'ts':float(r['ts']),'strategy':r['strategy'],'base':base,'buy':bv,'sell':sv,
                'buy_symbol':str(buy.get('symbol') or ''),'sell_symbol':str(sell.get('symbol') or ''),
                'buy_fee':float(buy.get('fee_rate') or 0),'sell_fee':float(sell.get('fee_rate') or 0),
                'qty':qty,'pnl':float(r['realized_profit'] or 0),'remaining':qty,'remaining_pnl':float(r['realized_profit'] or 0)}
    def refresh(self,start_id=0):
        try:
            q="SELECT id,ts,strategy,status,realized_profit,payload FROM executions WHERE id>? AND environment='DEMO' AND strategy IN ('CLASSIC','LATENCY') AND status='FILLED_BOTH' ORDER BY id"
            rows=execution_engine.db.execute(q,(int(start_id),)).fetchall(); parsed=[x for x in (self._parse(r) for r in rows) if x]
            queues=defaultdict(lambda:{'ab':deque(),'ba':deque()}); matches=[]
            live=live_executor.status(); quarantine=set(live.get('live_quarantined_venues') or [])
            allowed=set(live.get('verified_ready') or [])-quarantine
            for x in parsed:
                a,b=sorted((x['buy'],x['sell'])); key=(x['base'],a,b); side='ab' if (x['buy'],x['sell'])==(a,b) else 'ba'; opp='ba' if side=='ab' else 'ab'
                oq=queues[key][opp]
                while x['remaining']>1e-12 and oq:
                    y=oq[0]; m=min(x['remaining'],y['remaining']); fx=m/x['remaining']; fy=m/y['remaining']
                    xp=x['remaining_pnl']*fx; yp=y['remaining_pnl']*fy; pnl=xp+yp
                    deploy=bool({a,b}.issubset(allowed)); matches.append({'base':x['base'],'venues':[a,b],'qty':m,'pnl':pnl,'deployable':deploy,'execution_ids':[y['id'],x['id']]})
                    x['remaining']-=m; x['remaining_pnl']-=xp; y['remaining']-=m; y['remaining_pnl']-=yp
                    if y['remaining']<=1e-12:oq.popleft()
                if x['remaining']>1e-12:queues[key][side].append(x)
            residual=[]
            for group in queues.values():
                for side in ('ab','ba'):
                    residual.extend(list(group[side]))
            self.residual=residual
            natural=sum(z['pnl'] for z in matches); deploy=sum(z['pnl'] for z in matches if z['deployable'])
            self.latest={'mode':'DEMO_FILL_NETTING','matched_pair_count':len(matches),'natural_closed_loop_pnl':round(natural,8),
                         'deployable_natural_pnl':round(deploy,8),'deployable_match_count':sum(1 for z in matches if z['deployable']),
                         'residual_fill_count':len(residual),'residual_signal_pnl':round(sum(x['remaining_pnl'] for x in residual),8),
                         'allowed_venues':sorted(allowed),'quarantined_venues':sorted(quarantine),'matches':matches[-30:]}
            self.last_refresh=time.time(); self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:300]
        return self.status()
    def status(self):
        return {'ok':self.last_error is None,'mode':'DEMO_FILL_NETTING','last_refresh':self.last_refresh,'last_error':self.last_error,'latest':self.latest}

natural_rebalance_ledger=NaturalRebalanceLedger()