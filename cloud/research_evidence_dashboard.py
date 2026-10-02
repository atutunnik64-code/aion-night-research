from __future__ import annotations

import json
import math
import statistics
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUT = DATA / "research_evidence_dashboard_v1.json"


def load(name: str):
    p = DATA / name
    try:
        raw = p.read_text(encoding="utf-8-sig")
        if not raw.strip(): return None, "blank"
        return json.loads(raw), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:240]}"


def f(v, default=0.0):
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception: return default


def stats(values):
    xs = [f(x) for x in values if x is not None]
    if not xs:
        return {"count": 0, "sum": 0.0, "mean": None, "median": None, "min": None, "max": None, "positive": 0, "positive_rate": None}
    pos = sum(1 for x in xs if x > 0)
    return {"count": len(xs), "sum": round(sum(xs),8), "mean": round(sum(xs)/len(xs),8), "median": round(statistics.median(xs),8),
            "min": round(min(xs),8), "max": round(max(xs),8), "positive": pos, "positive_rate": round(pos/len(xs),6)}


def generic_counts(state):
    if not isinstance(state, dict): return {}
    out = {}
    for k in ("scan_count", "last_new_events", "last_scan_ts"):
        if k in state: out[k] = state.get(k)
    for k in ("pending", "resolved", "positions", "closed", "events", "verification_queue", "aborted"):
        if isinstance(state.get(k), (list, dict)): out[k + "_count"] = len(state.get(k) or [])
    return out


def build():
    now = time.time()
    dash = {"version":"RESEARCH_EVIDENCE_DASHBOARD_V2","generated_at":now,"paper_only":True,"live_enabled":False,
            "notes":["Metrics remain in each branch's native units; incompatible branches are never summed into one portfolio return.",
                     "Trade-return sums are not capital-normalized portfolio returns unless explicitly labelled equity_return_pct.",
                     "Research edge dollars are observed PAPER edge, not realized account profit."],"branches":{},"errors":{}}

    s, err = load("crossvenue_spot_arb_cloud_v1.json")
    if err: dash["errors"]["classic_spot_arb"] = err
    elif isinstance(s, dict):
        events=[x for x in (s.get("events") or []) if str(x.get("quality_guard") or "").startswith("PRICE_VOLUME_CONSENSUS")]
        dash["branches"]["classic_spot_arb"]={"metric_type":"research_edge_quote",**generic_counts(s),"research_event_count":len(events),
            "conservative_research_edge_quote":round(f(s.get("research_pnl_quote")),8),"execution_edge_quote":round(f(s.get("execution_edge_quote")),8),
            "latest_cycle":{"raw_positive_routes":s.get("last_raw_positive"),"after_fees_positive_routes":s.get("last_after_fees_positive"),
                            "depth_checks":s.get("last_depth_checks"),"depth_pass":s.get("last_depth_pass"),"new_events":s.get("last_new_events"),
                            "verification_candidates":s.get("last_verification_candidates")},
            "venue_symbol_counts":s.get("venue_symbol_counts") or {},"venue_errors":s.get("venue_errors") or {},"venue_hosts":s.get("venue_hosts") or {}}

    s, err = load("crossvenue_arb_verifier_v1.json")
    if err: dash["errors"]["arb_identity_verifier"] = err
    elif isinstance(s, dict):
        verified=list((s.get("verified") or {}).values());rejected=list((s.get("rejected") or {}).values());watch=list((s.get("watch") or {}).values())
        dash["branches"]["arb_identity_verifier"]={"metric_type":"identity_verified_paper_edge","scan_count":s.get("scan_count",0),
            "candidate_signature_count":s.get("candidate_signature_count"),"verified_count":len(verified),"rejected_count":len(rejected),"watch_count":len(watch),
            "verified_paper_edge_quote":round(sum(f(x.get("paper_edge_quote")) for x in verified),8),
            "verified_execution_net_pct":stats([x.get("execution_net_pct") for x in verified]),"last_error":s.get("last_error")}

    s, err = load("bybit_local_relay_v1.json")
    if err: dash["errors"]["bybit_local_relay"] = err
    elif isinstance(s, dict):
        h=s.get("source_health") or {}; sa=s.get("spot_arb") or {}; pf=s.get("perp_funding") or {}
        foi=s.get("funding_oi_shadow") or {}; ps=s.get("price_shock_shadow") or {}
        lr=s.get("liquidation_regime_shadow") or {}; vc=s.get("volatility_compression_shadow") or {}
        age=max(0.0,now-f(s.get("generated_at"),now))
        dash["branches"]["bybit_local_relay"]={"metric_type":"local_bybit_public_data_research","fresh":age<=900,"age_sec":round(age,3),
            "spot_symbols":int(h.get("bybit_spot_symbol_count") or 0),"guarded_common_assets":int(h.get("bybit_guarded_common_assets") or 0),
            "perp_symbols":int(h.get("bybit_perp_symbol_count") or 0),"ws_connected":bool(h.get("bybit_ws_connected")),
            "context_count":int(h.get("bybit_context_count") or 0),"liquidation_events":int(h.get("bybit_liquidation_events") or 0),
            "spot_scan_count":int(sa.get("scan_count") or 0),"recent_bybit_arb_events":int(sa.get("recent_bybit_event_count") or 0),
            "bybit_funding_candidates":int(pf.get("bybit_paper_candidate_count") or 0),
            "funding_oi_pending":int(foi.get("pending_count") or 0),"funding_oi_resolved":int(foi.get("resolved_count") or 0),
            "price_shock_pending":int(ps.get("pending_count") or 0),"price_shock_resolved":int(ps.get("resolved_count") or 0),
            "liquidation_regime_pending":int(lr.get("pending_count") or 0),"liquidation_regime_resolved":int(lr.get("resolved_count") or 0),
            "vol_compression_pending":int(vc.get("pending_count") or 0),"vol_compression_resolved":int(vc.get("resolved_count") or 0)}

    s, err = load("crossvenue_leadlag_episode_cloud_v1.json")
    if err: dash["errors"]["leadlag"] = err
    elif isinstance(s, dict):
        r=s.get("resolved") or []
        dash["branches"]["leadlag"]={"metric_type":"episode_edge",**generic_counts(s),"final_all_in_net_pct":stats([x.get("final_all_in_net_pct") for x in r]),
            "max_all_in_net_pct":stats([x.get("max_all_in_net_pct") for x in r]),"paper_positive_window_edge_quote_sum":round(sum(f(x.get("paper_edge_quote")) for x in r),8),
            "caught_up_count":sum(1 for x in r if x.get("resolution")=="CAUGHT_UP"),"timeout_count":sum(1 for x in r if x.get("resolution")=="TIMEOUT"),
            "error_count":s.get("error_count",0),"last_error":s.get("last_error")}

    s, err = load("crossvenue_orderbook_consensus_shadow_v1.json")
    if err: dash["errors"]["orderbook_consensus"] = err
    elif isinstance(s, dict):
        r=s.get("resolved") or []
        valid=[];stale=[]
        for x in r:
            lag=f(x.get("resolution_lag_sec"), f(x.get("exit_ts"))-f(x.get("due_ts")))
            (valid if 0<=lag<=180 else stale).append(x)
        eq=100.0;peak=100.0;dd=0.0
        for x in valid:
            eq*=1+0.05*(f(x.get("net_return_pct"))/100.0);peak=max(peak,eq);dd=min(dd,(eq/peak-1)*100 if peak else 0.0)
        dash["branches"]["orderbook_consensus"]={"metric_type":"capital_normalized_timing_valid_equity",**generic_counts(s),
            "timing_valid_resolved_count":len(valid),"legacy_stale_resolved_count":len(stale),"timing_valid_net_return_pct":stats([x.get("net_return_pct") for x in valid]),
            "equity_return_pct":round(eq-100.0,8),"max_dd_pct":round(dd,8)}

    s, err = load("crossvenue_momentum_divergence_shadow_v1.json")
    if err: dash["errors"]["momentum_divergence"] = err
    elif isinstance(s, dict):
        r=s.get("resolved") or [];syms=sorted({x.get("base") for x in r if x.get("base")});frozen=f(s.get("rule_frozen_at"),now)
        dash["branches"]["momentum_divergence"]={"metric_type":"capital_normalized_equity_5pct_alloc",**generic_counts(s),"collection_hours":round(max(0.0,(now-frozen)/3600.0),4),"resolved_symbols":len(syms),
            "continuation":{"equity":round(f(s.get("continuation_equity"),100),8),"equity_return_pct":round(f(s.get("continuation_equity"),100)-100,8),"max_dd_pct":round(f(s.get("continuation_max_dd_pct")),8),"trade_returns":stats([x.get("continuation_net_return_pct") for x in r])},
            "snapback":{"equity":round(f(s.get("snapback_equity"),100),8),"equity_return_pct":round(f(s.get("snapback_equity"),100)-100,8),"max_dd_pct":round(f(s.get("snapback_max_dd_pct")),8),"trade_returns":stats([x.get("snapback_net_return_pct") for x in r])}}

    s, err = load("perp_funding_spread_paper.json")
    if err: dash["errors"]["perp_funding_spread"] = err
    elif isinstance(s, dict):
        pos=s.get("positions") or [];closed=s.get("closed") or []
        dash["branches"]["perp_funding_spread"]={"metric_type":"quote_pnl",**generic_counts(s),"closed_pnl_quote":round(f(s.get("closed_pnl")),8),
            "open_funding_realized_quote":round(sum(f(x.get("funding_realized")) for x in pos),8),"open_funding_events":sum(int(x.get("funding_events") or 0) for x in pos),
            "closed_trade_pnl_quote":stats([x.get("net_pnl") for x in closed if x.get("net_pnl") is not None])}

    s, err = load("crossvenue_spot_perp_portfolio_v1.json")
    if err: dash["errors"]["spot_perp_portfolio_100"] = err
    elif isinstance(s, dict):
        eq=f(s.get("realized_equity_quote"),100.0);pos=s.get("positions") or {};resolved=s.get("resolved") or []
        dash["branches"]["spot_perp_portfolio_100"]={"metric_type":"capital_constrained_realized_quote_pnl",**generic_counts(s),
            "starting_capital_quote":f(s.get("starting_capital_quote"),100),"realized_equity_quote":round(eq,8),"realized_pnl_quote":round(f(s.get("realized_pnl_quote")),8),
            "realized_return_pct":round(eq-100.0,8),"open_positions":len(pos),"locked_capital_quote":len(pos)*50.0,"available_capital_quote":round(max(0.0,eq-len(pos)*50.0),8),
            "open_funding_accrued_quote":round(sum(f(x.get("funding_quote")) for x in pos.values()),8),"resolved_pnl_quote":stats([x.get("pnl_quote") for x in resolved])}

    s, err = load("classic_spot_arb_portfolio_v1.json")
    if err: dash["errors"]["classic_spot_arb_portfolio_100"] = err
    elif isinstance(s, dict):
        start=f(s.get("starting_capital_quote"),100.0);eq=f(s.get("realized_equity_quote"),start);trades=s.get("trades") or []
        dash["branches"]["classic_spot_arb_portfolio_100"]={"metric_type":"capital_normalized_realized_conservative_quote_pnl",
            "starting_capital_quote":round(start,8),"realized_equity_quote":round(eq,8),"realized_pnl_quote":round(f(s.get("realized_pnl_quote")),8),
            "realized_return_pct":round((eq/start-1.0)*100.0,8) if start>0 else None,"trade_count":len(trades),
            "bybit_trade_count":sum(1 for x in trades if x.get("includes_bybit")),"locked_capital_quote":round(f(s.get("locked_capital_quote")),8),
            "available_capital_quote":round(f(s.get("available_capital_quote")),8),
            "trade_pnl_quote":stats([x.get("realized_conservative_pnl_quote") for x in trades]),
            "skipped_capital":int(s.get("skipped_capital") or 0),"skipped_identity":int(s.get("skipped_identity") or 0)}

    s, err = load("perp_funding_spread_portfolio_v1.json")
    if err: dash["errors"]["perp_funding_spread_portfolio_100"] = err
    elif isinstance(s, dict):
        start=f(s.get("starting_capital_quote"),100.0);eq=f(s.get("realized_equity_quote"),start);pos=s.get("positions") or {};resolved=s.get("resolved") or []
        dash["branches"]["perp_funding_spread_portfolio_100"]={"metric_type":"future_only_capital_normalized_realized_quote_pnl",
            "starting_capital_quote":round(start,8),"realized_equity_quote":round(eq,8),"realized_pnl_quote":round(f(s.get("realized_pnl_quote")),8),
            "realized_return_pct":round((eq/start-1.0)*100.0,8) if start>0 else None,"open_positions":len(pos),"resolved_positions":len(resolved),
            "max_dd_pct":round(f(s.get("max_dd_pct")),8),"resolved_normalized_pnl":stats([x.get("normalized_pnl") for x in resolved]),
            "rule_frozen_at":s.get("rule_frozen_at")}

    s, err = load("moex_broad_portfolio_v1.json")
    if err: dash["errors"]["moex_broad_portfolio_100"] = err
    elif isinstance(s, dict):
        start=f(s.get("starting_capital"),100.0);books=s.get("books") or {}
        def moex_book(kind):
            b=books.get(kind) or {};eq=f(b.get("equity"),start);resolved=b.get("resolved") or []
            rub=[f(x.get("one_contract_net_rub_stress_estimate")) for x in resolved if x.get("one_contract_net_rub_stress_estimate") is not None]
            wins=sum(1 for x in resolved if f(x.get("normalized_pnl"))>0)
            return {"equity":round(eq,8),"pnl":round(eq-start,8),"return_pct":round((eq/start-1.0)*100.0,8) if start>0 else None,
                "open_positions":len(b.get("positions") or {}),"resolved_positions":len(resolved),"wins":wins,
                "win_rate":round(wins/len(resolved),6) if resolved else None,"max_dd_pct":round(f(b.get("max_dd_pct")),8),
                "normalized_pnl":stats([x.get("normalized_pnl") for x in resolved]),
                "one_contract_net_rub_stress":{"count":len(rub),"sum":round(sum(rub),8),"mean":round(sum(rub)/len(rub),8) if rub else None,
                    "recent":[{"secid":x.get("secid"),"value_rub":round(f(x.get("one_contract_net_rub_stress_estimate")),8)}
                              for x in resolved[-12:] if x.get("one_contract_net_rub_stress_estimate") is not None]}}
        dash["branches"]["moex_broad_portfolio_100"]={"metric_type":"future_only_capital_normalized_paper","starting_capital":round(start,8),
            "rule_frozen_at":s.get("rule_frozen_at"),"no_historical_backfill":True,"continuation":moex_book("continuation"),"reversal":moex_book("reversal")}

    s, err = load("moex_broad_futures_shadow_v1.json")
    if err: dash["errors"]["moex_broad"] = err
    elif isinstance(s, dict):
        r=s.get("resolved") or []
        dash["branches"]["moex_broad"]={"metric_type":"trade_return_pct_not_portfolio",**generic_counts(s),
            "resolved_return_pct":stats([x.get("net_return_pct") if x.get("net_return_pct") is not None else x.get("return_pct") for x in r]),
            "reported_equity_raw":s.get("equity"),"reported_equity_warning":"raw branch equity is not treated as portfolio return unless capital normalization is explicit"}

    for key,name in (("moex_calendar_matrix","moex_calendar_matrix_shadow_v1.json"),("moex_spread_paper","moex_spread_paper_v1.json"),
                     ("spot_perp_shadow","crossvenue_spot_perp_shadow_v2.json"),("funding_dislocation","funding_dislocation_paper_v1.json"),
                     ("funding_oi_bybit","funding_oi_bybit_shadow_v1.json"),("bybit_liquidation_regime","bybit_liquidation_regime_shadow_v1.json"),
                     ("bybit_price_shock","bybit_price_shock_shadow_v1.json"),("bybit_volatility_compression","bybit_volatility_compression_shadow_v1.json")):
        st,e=load(name)
        if e: dash["errors"][key]=e
        elif isinstance(st,dict):
            row={"metric_type":"native_state_summary",**generic_counts(st)}
            for fld in ("equity","equity_2x","equity_3x","follow_equity","fade_equity","continuation_equity","recovery_equity","reversal_equity","breakout_equity","fakeout_equity","closed_pnl","last_error","rule_frozen_at","eligible_pairs","raw_same_asset_pairs"):
                if fld in st: row[fld]=st.get(fld)
            dash["branches"][key]=row

    tmp=OUT.with_suffix(OUT.suffix+".tmp");tmp.write_text(json.dumps(dash,ensure_ascii=False,indent=2),encoding="utf-8");tmp.replace(OUT)
    print(f"EVIDENCE_DASHBOARD_OK branches={len(dash['branches'])} errors={len(dash['errors'])}")

if __name__ == "__main__": build()
