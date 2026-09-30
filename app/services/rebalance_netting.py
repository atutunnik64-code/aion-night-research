from __future__ import annotations
import time

class RebalanceNettingPlanner:
    def __init__(self):
        self.last=None

    @staticmethod
    def _amount(row):
        for key in ("amount","quote_in","start_amount","notional"):
            try:
                v=float(row.get(key) or 0)
                if v>0:return v
            except Exception:pass
        return 0.0

    @staticmethod
    def _rebalance_cost(row):
        for key in ("rebalance_cost_quote","rebalance_cost","full_rebalance_cost_quote"):
            try:
                v=float(row.get(key) or 0)
                if v>0:return v
            except Exception:pass
        plan=row.get("batch_ready_plan") or {}
        try:return max(0.0,float(plan.get("rebalance_cost_per_cycle") or 0))
        except Exception:return 0.0

    def plan(self,rows):
        rows=list(rows or []); pairs=[]; used=set()
        for i,a in enumerate(rows):
            if i in used:continue
            ast=a.get("steps") or []
            abuy=next((x for x in ast if x.get("side")=="buy"),{})
            asell=next((x for x in ast if x.get("side")=="sell"),{})
            av=str(a.get("buy_venue") or abuy.get("venue") or ""); bv=str(a.get("sell_venue") or asell.get("venue") or "")
            if not av or not bv or av==bv:continue
            aa=self._amount(a)
            if aa<=0:continue
            best=None
            for j,b in enumerate(rows):
                if j==i or j in used:continue
                bst=b.get("steps") or []
                bbuy=next((x for x in bst if x.get("side")=="buy"),{})
                bsell=next((x for x in bst if x.get("side")=="sell"),{})
                bbuyv=str(b.get("buy_venue") or bbuy.get("venue") or ""); bsellv=str(b.get("sell_venue") or bsell.get("venue") or "")
                if bbuyv!=bv or bsellv!=av:continue
                ba=self._amount(b)
                if ba<=0:continue
                nettable=min(aa,ba); ratio=nettable/max(aa,ba)
                score=(ratio,float(b.get("net_pct") or b.get("clean_net_pct") or 0))
                if best is None or score>best[0]:best=(score,j,b,ba,nettable)
            if best is None:continue
            _,j,b,ba,nettable=best; used.update({i,j})
            ca=self._rebalance_cost(a); cb=self._rebalance_cost(b)
            avoid_ratio_a=min(1.0,nettable/aa); avoid_ratio_b=min(1.0,nettable/ba)
            saved=ca*avoid_ratio_a+cb*avoid_ratio_b
            pairs.append({
                "venue_a":av,"venue_b":bv,
                "route_a":a.get("signature") or f"{a.get('base') or a.get('pair')}:{av}->{bv}",
                "route_b":b.get("signature") or f"{b.get('base') or b.get('pair')}:{bv}->{av}",
                "flow_a_quote":round(aa,8),"flow_b_quote":round(ba,8),
                "nettable_quote":round(nettable,8),
                "netting_ratio_pct":round(nettable/max(aa,ba)*100.0,4),
                "estimated_rebalance_saved_quote":round(saved,8),
                "status":"QUOTE_FLOW_NETTABLE",
            })
        self.last={"updated_at":time.time(),"pair_count":len(pairs),"pairs":sorted(pairs,key=lambda x:(x["estimated_rebalance_saved_quote"],x["netting_ratio_pct"]),reverse=True)}
        return self.last

rebalance_netting=RebalanceNettingPlanner()
