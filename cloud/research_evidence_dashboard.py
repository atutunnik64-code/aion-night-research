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
        if not raw.strip():
            return None, "blank"
        return json.loads(raw), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:240]}"


def f(v, default=0.0):
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def stats(values):
    xs = [f(x) for x in values if x is not None]
    if not xs:
        return {"count": 0, "sum": 0.0, "mean": None, "median": None, "min": None, "max": None, "positive": 0, "positive_rate": None}
    return {
        "count": len(xs),
        "sum": round(sum(xs), 8),
        "mean": round(sum(xs) / len(xs), 8),
        "median": round(statistics.median(xs), 8),
        "min": round(min(xs), 8),
        "max": round(max(xs), 8),
        "positive": sum(1 for x in xs if x > 0),
        "positive_rate": round(sum(1 for x in xs if x > 0) / len(xs), 6),
    }


def generic_counts(state):
    if not isinstance(state, dict):
        return {}
    out = {}
    for k in ("scan_count", "last_new_events", "last_scan_ts"):
        if k in state:
            out[k] = state.get(k)
    for k in ("pending", "resolved", "positions", "closed", "events", "verification_queue"):
        if isinstance(state.get(k), (list, dict)):
            out[k + "_count"] = len(state.get(k) or [])
    return out


def build():
    now = time.time()
    dash = {
        "version": "RESEARCH_EVIDENCE_DASHBOARD_V1",
        "generated_at": now,
        "paper_only": True,
        "live_enabled": False,
        "notes": [
            "Metrics remain in each branch's native units; incompatible branches are never summed into one portfolio return.",
            "Trade-return sums are not capital-normalized portfolio returns unless explicitly labelled equity_return_pct.",
            "Research edge dollars are observed PAPER edge, not realized account profit.",
        ],
        "branches": {},
        "errors": {},
    }

    s, err = load("crossvenue_spot_arb_cloud_v1.json")
    if err:
        dash["errors"]["classic_spot_arb"] = err
    elif isinstance(s, dict):
        events = [x for x in (s.get("events") or []) if str(x.get("quality_guard") or "").startswith("PRICE_VOLUME_CONSENSUS")]
        dash["branches"]["classic_spot_arb"] = {
            "metric_type": "research_edge_quote",
            **generic_counts(s),
            "research_event_count": len(events),
            "conservative_research_edge_quote": round(f(s.get("research_pnl_quote")), 8),
            "execution_edge_quote": round(f(s.get("execution_edge_quote")), 8),
            "latest_cycle": {
                "raw_positive_routes": s.get("last_raw_positive"),
                "after_fees_positive_routes": s.get("last_after_fees_positive"),
                "depth_checks": s.get("last_depth_checks"),
                "depth_pass": s.get("last_depth_pass"),
                "new_events": s.get("last_new_events"),
                "verification_candidates": s.get("last_verification_candidates"),
            },
            "venue_symbol_counts": s.get("venue_symbol_counts") or {},
            "venue_errors": s.get("venue_errors") or {},
            "venue_hosts": s.get("venue_hosts") or {},
        }

    s, err = load("crossvenue_leadlag_episode_cloud_v1.json")
    if err:
        dash["errors"]["leadlag"] = err
    elif isinstance(s, dict):
        r = s.get("resolved") or []
        dash["branches"]["leadlag"] = {
            "metric_type": "episode_edge",
            **generic_counts(s),
            "final_all_in_net_pct": stats([x.get("final_all_in_net_pct") for x in r]),
            "max_all_in_net_pct": stats([x.get("max_all_in_net_pct") for x in r]),
            "paper_positive_window_edge_quote_sum": round(sum(f(x.get("paper_edge_quote")) for x in r), 8),
            "caught_up_count": sum(1 for x in r if x.get("resolution") == "CAUGHT_UP"),
            "timeout_count": sum(1 for x in r if x.get("resolution") == "TIMEOUT"),
            "error_count": s.get("error_count", 0),
            "last_error": s.get("last_error"),
        }

    s, err = load("crossvenue_orderbook_consensus_shadow_v1.json")
    if err:
        dash["errors"]["orderbook_consensus"] = err
    elif isinstance(s, dict):
        r = s.get("resolved") or []
        dash["branches"]["orderbook_consensus"] = {
            "metric_type": "trade_return_pct_not_portfolio",
            **generic_counts(s),
            "net_return_pct": stats([x.get("net_return_pct") for x in r]),
        }

    s, err = load("crossvenue_momentum_divergence_shadow_v1.json")
    if err:
        dash["errors"]["momentum_divergence"] = err
    elif isinstance(s, dict):
        r = s.get("resolved") or []
        syms = sorted({x.get("base") for x in r if x.get("base")})
        frozen = f(s.get("rule_frozen_at"), now)
        dash["branches"]["momentum_divergence"] = {
            "metric_type": "capital_normalized_equity_5pct_alloc",
            **generic_counts(s),
            "collection_hours": round(max(0.0, (now - frozen) / 3600.0), 4),
            "resolved_symbols": len(syms),
            "continuation": {
                "equity": round(f(s.get("continuation_equity"), 100.0), 8),
                "equity_return_pct": round(f(s.get("continuation_equity"), 100.0) - 100.0, 8),
                "max_dd_pct": round(f(s.get("continuation_max_dd_pct")), 8),
                "trade_returns": stats([x.get("continuation_net_return_pct") for x in r]),
            },
            "snapback": {
                "equity": round(f(s.get("snapback_equity"), 100.0), 8),
                "equity_return_pct": round(f(s.get("snapback_equity"), 100.0) - 100.0, 8),
                "max_dd_pct": round(f(s.get("snapback_max_dd_pct")), 8),
                "trade_returns": stats([x.get("snapback_net_return_pct") for x in r]),
            },
        }

    s, err = load("perp_funding_spread_paper.json")
    if err:
        dash["errors"]["perp_funding_spread"] = err
    elif isinstance(s, dict):
        pos = s.get("positions") or []
        closed = s.get("closed") or []
        dash["branches"]["perp_funding_spread"] = {
            "metric_type": "quote_pnl",
            **generic_counts(s),
            "closed_pnl_quote": round(f(s.get("closed_pnl")), 8),
            "open_funding_realized_quote": round(sum(f(x.get("funding_realized")) for x in pos), 8),
            "open_funding_events": sum(int(x.get("funding_events") or 0) for x in pos),
            "closed_trade_pnl_quote": stats([x.get("net_pnl") for x in closed if x.get("net_pnl") is not None]),
        }

    s, err = load("moex_broad_futures_shadow_v1.json")
    if err:
        dash["errors"]["moex_broad"] = err
    elif isinstance(s, dict):
        r = s.get("resolved") or []
        dash["branches"]["moex_broad"] = {
            "metric_type": "trade_return_pct_not_portfolio",
            **generic_counts(s),
            "resolved_return_pct": stats([
                x.get("net_return_pct") if x.get("net_return_pct") is not None else x.get("return_pct")
                for x in r
            ]),
            "reported_equity_raw": s.get("equity"),
            "reported_equity_warning": "raw branch equity is not treated as portfolio return unless capital normalization is explicit",
        }

    for key, name in (
        ("moex_calendar_matrix", "moex_calendar_matrix_shadow_v1.json"),
        ("moex_spread_paper", "moex_spread_paper_v1.json"),
        ("spot_perp_shadow", "crossvenue_spot_perp_shadow_v2.json"),
        ("funding_dislocation", "funding_dislocation_paper_v1.json"),
        ("funding_oi_bybit", "funding_oi_bybit_shadow_v1.json"),
        ("bybit_liquidation_regime", "bybit_liquidation_regime_shadow_v1.json"),
        ("bybit_price_shock", "bybit_price_shock_shadow_v1.json"),
        ("bybit_volatility_compression", "bybit_volatility_compression_shadow_v1.json"),
    ):
        st, e = load(name)
        if e:
            dash["errors"][key] = e
        elif isinstance(st, dict):
            row = {"metric_type": "native_state_summary", **generic_counts(st)}
            for fld in (
                "equity", "equity_2x", "equity_3x", "follow_equity", "fade_equity",
                "continuation_equity", "recovery_equity", "reversal_equity", "breakout_equity",
                "fakeout_equity", "closed_pnl", "last_error", "rule_frozen_at",
            ):
                if fld in st:
                    row[fld] = st.get(fld)
            dash["branches"][key] = row

    tmp = OUT.with_suffix(OUT.suffix + ".tmp")
    tmp.write_text(json.dumps(dash, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(OUT)
    print(f"EVIDENCE_DASHBOARD_OK branches={len(dash['branches'])} errors={len(dash['errors'])}")


if __name__ == "__main__":
    build()
