from __future__ import annotations
import time

class StrategyPortfolio:
    def __init__(self):
        self.starting_capital=100.0
        self.max_idea_pct=25.0
        self.max_venue_pct=25.0
        self.daily_stop_pct=5.0
        self.min_locked_net_pct=0.05
        self.last_snapshot=None

    def limits(self):
        cap=self.starting_capital
        return {
            "starting_capital":cap,
            "max_idea_usdt":round(cap*self.max_idea_pct/100.0,2),
            "max_venue_exposure_usdt":round(cap*self.max_venue_pct/100.0,2),
            "daily_stop_usdt":round(cap*self.daily_stop_pct/100.0,2),
            "min_locked_net_pct":self.min_locked_net_pct,
            "leverage_allowed":False,
            "live_requires_explicit_enable":True,
        }

    def _metrics(self,row):
        plan=row.get("batch_ready_plan") or {}
        if plan:
            net=float(plan.get("net_pct_per_cycle") or 0)
            profit=float(plan.get("series_profit") or 0)
            buy_cap=float(plan.get("required_buy_quote_inventory") or 0)
            sell_qty=float(plan.get("required_sell_base_inventory") or 0)
            buy_step=next((x for x in (row.get("steps") or []) if x.get("side")=="buy"),{})
            px=float(buy_step.get("best_ask") or (1/float(buy_step.get("rate") or 1)))
            sell_cap=sell_qty*px
            required=buy_cap+sell_cap
            return net,profit,required
        if row.get("full_loop_confirmed") or row.get("full_net_pct") is not None:
            net=float(row.get("full_net_pct") or 0)
            profit=float(row.get("full_profit_amount") or 0)
            start=float(row.get("required_buy_quote") or row.get("start_amount") or 0)
            required=max(0.0,start*2.0)
            return net,profit,required
        net=float(row.get("net_pct") or row.get("clean_net_pct") or row.get("entry_basis_net_pct") or 0)
        profit=float(row.get("profit") or row.get("clean_profit") or row.get("net_profit") or 0)
        amount=float(row.get("amount") or row.get("quote_in") or row.get("start_amount") or 0)
        return net,profit,max(0.0,amount*2.0 if amount else 0.0)

    def rank(self,rows):
        out=[]
        for row in rows or []:
            try: net,profit,required=self._metrics(row)
            except Exception: continue
            if net<=0: continue
            repeat=float(row.get("repeatability_score") or row.get("aion_score") or 50)
            risk=float(row.get("risk_penalty") or 0)
            score=net*100.0 + min(100.0,repeat)*0.2 - risk
            fit=(required<=self.starting_capital+1e-9) if required>0 else False
            out.append({**row,"portfolio_score":round(score,4),"portfolio_net_pct":round(net,6),
                        "portfolio_profit":round(profit,8),"required_capital_quote":round(required,6),
                        "capital_fit":fit})
        return sorted(out,key=lambda x:(x["capital_fit"],x["portfolio_score"],x["portfolio_net_pct"]),reverse=True)

    def snapshot(self,classic=None,basis=None,triangles=None,stable=None,live_venues=None):
        live_venues=set(live_venues or [])
        raw_classic=self.rank(classic)
        # One asset + one SELL venue is one economic risk cluster, even if many BUY venues quote it.
        clusters={}
        for x in raw_classic:
            steps=x.get('steps') or []
            sell=next((z for z in steps if str(z.get('side') or '').lower()=='sell'),{})
            base=str(sell.get('from_asset') or x.get('base') or '?').upper()
            venue=str(sell.get('venue') or x.get('sell_venue') or '?')
            key=f'{base}@{venue}'
            clusters.setdefault(key,[]).append(x)
        classic_ranked=[]
        for key,items in clusters.items():
            best=items[0]
            classic_ranked.append({**best,'risk_cluster':key,'cluster_size':len(items),'alternative_routes':max(0,len(items)-1)})
        classic_ranked=sorted(classic_ranked,key=lambda x:(x.get('capital_fit',False),x.get('portfolio_score',0)),reverse=True)
        sell_counts={}
        for x in classic_ranked:
            venue=str(x.get('risk_cluster','?@?')).split('@',1)[-1]
            sell_counts[venue]=sell_counts.get(venue,0)+1
        cluster_total=max(1,len(classic_ranked))
        tradeable=[]; access_blocked=[]; capital_blocked=[]
        for x in classic_ranked:
            steps=x.get("steps") or []
            venues={str(z.get("venue") or "") for z in steps if z.get("venue")}
            access_ready=bool(venues and venues.issubset(live_venues))
            x={**x,"execution_venues":sorted(venues),"access_ready":access_ready}
            economic=x.get("capital_fit") and float(x.get("portfolio_net_pct") or 0)>=self.min_locked_net_pct
            if economic and access_ready:tradeable.append(x)
            elif economic:access_blocked.append(x)
            else:capital_blocked.append(x)
        research_groups={
            "basis_funding":self.rank(basis),
            "triangles":self.rank(triangles),
            "stablecoin":self.rank(stable),
        }
        research=[]
        for strategy,rows in research_groups.items():
            for row in rows[:25]: research.append({"strategy":strategy,"capital_status":"RESEARCH_ONLY",**row})
        research=self.rank(research)
        tradeable=[{"strategy":"classic","capital_status":"TRADEABLE_PIPELINE",**x} for x in tradeable]
        access_blocked=[{"strategy":"classic","capital_status":"ACCESS_BLOCKED",**x} for x in access_blocked]
        capital_blocked=[{"strategy":"classic","capital_status":"CAPITAL_BLOCKED",**x} for x in capital_blocked]
        self.last_snapshot={
            "updated_at":time.time(),"limits":self.limits(),
            "classic_raw_count":len(raw_classic),"classic_cluster_count":len(classic_ranked),
            "sell_venue_cluster_counts":dict(sorted(sell_counts.items(),key=lambda kv:kv[1],reverse=True)),
            "venue_concentration_warning":any(v/cluster_total>0.50 for v in sell_counts.values()),
            "tradeable_count":len(tradeable),"access_blocked_count":len(access_blocked),"capital_blocked_count":len(capital_blocked),"research_count":len(research),
            "live_venues":sorted(live_venues),
            "counts":{"classic_tradeable":len(tradeable),"classic_access_blocked":len(access_blocked),"classic_capital_blocked":len(capital_blocked),**{k:len(v) for k,v in research_groups.items()}},
            "tradeable":tradeable[:50],"access_blocked":access_blocked[:50],"capital_blocked":capital_blocked[:50],"research":research[:50],
            "top":tradeable[:25]+access_blocked[:25]+capital_blocked[:25]+research[:25],
        }
        return self.last_snapshot

strategy_portfolio=StrategyPortfolio()
