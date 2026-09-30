from __future__ import annotations

from app.services.route_finder import RouteFinder
from app.services.prefunded_cycles import PrefundedCycleFinder


def _amount_grid(max_amount: float) -> list[float]:
    seeds = [25000,10000,5000,2500,1000,500,250,100,50,25,10]
    values = [float(max_amount)] + [float(x) for x in seeds if x < max_amount]
    return sorted(set(v for v in values if v > 0), reverse=True)


def optimized_cycles_worker(edges, max_amount, cycle_assets, start_assets, max_hops=5, min_profit_pct=0.001):
    allowed=set(cycle_assets)
    filtered=[e for e in edges if e.src.asset in allowed and e.dst.asset in allowed]
    finder=RouteFinder(max_hops=int(max_hops), min_profit_pct=float(min_profit_pct))
    chosen={}
    for amount in _amount_grid(float(max_amount)):
        for route in finder.find_cycles(filtered, amount, set(start_assets)):
            sig='>'.join(route['nodes'])
            if sig not in chosen:
                route['optimized_amount']=amount
                chosen[sig]=route
    return sorted(chosen.values(), key=lambda x:(x['profit_amount'],x['profit_pct']), reverse=True)


def prefunded_find_worker(edges, requested, start_assets, max_hops=3, min_profit_pct=0.001):
    finder=PrefundedCycleFinder(max_hops=int(max_hops), min_profit_pct=float(min_profit_pct))
    return finder.find(edges, float(requested), set(start_assets))


def prefunded_confirm_worker(candidates, edges, requested, max_hops=3, min_profit_pct=0.001):
    finder=PrefundedCycleFinder(max_hops=int(max_hops), min_profit_pct=float(min_profit_pct))
    return finder.confirm(candidates, edges, float(requested))
