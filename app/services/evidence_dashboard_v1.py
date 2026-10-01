from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

ROOT = Path(__file__).parents[2]
DATA = ROOT / "data"
STATE = DATA / "evidence_dashboard_v1.json"


class EvidenceDashboardV1:
    def __init__(self):
        self.enabled = True
        self.live_enabled = False
        self.task = None
        self.last_error = None
        self.last_refresh = None

    @staticmethod
    def _read(name: str) -> dict:
        p = DATA / name
        try:
            raw = p.read_text(encoding="utf-8-sig")
            if not raw.strip():
                return {"_health": "EMPTY"}
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else {"_health": "INVALID_TYPE"}
        except FileNotFoundError:
            return {"_health": "MISSING"}
        except Exception as exc:
            return {"_health": f"ERROR:{type(exc).__name__}:{str(exc)[:180]}"}

    @staticmethod
    def _f(v, default=0.0):
        try:
            return float(v)
        except Exception:
            return default

    def _build(self) -> dict:
        arb = self._read("crossvenue_spot_arb_cloud_v1.json")
        verifier = self._read("crossvenue_arb_verifier_v1.json")
        leadlag = self._read("crossvenue_leadlag_episode_cloud_v1.json")
        spot_perp = self._read("crossvenue_spot_perp_portfolio_v1.json")
        orderbook = self._read("crossvenue_orderbook_consensus_shadow_v1.json")
        funding = self._read("perp_funding_spread_paper.json")
        moex = self._read("moex_broad_futures_shadow_v1.json")
        calendar = self._read("moex_calendar_matrix_shadow_v1.json")

        ll_resolved = leadlag.get("resolved") or []
        ll_positive = [x for x in ll_resolved if self._f(x.get("max_all_in_net_pct")) > 0]
        ll_edge_quote = sum(self._f(x.get("paper_edge_quote")) for x in ll_resolved)

        ob_resolved = orderbook.get("resolved") or []
        ob_returns = [self._f(x.get("net_return_pct")) for x in ob_resolved]

        sp_start = self._f(spot_perp.get("starting_capital_quote"), 100.0)
        sp_equity = self._f(spot_perp.get("realized_equity_quote"), sp_start)
        sp_realized_pct = ((sp_equity / sp_start) - 1.0) * 100.0 if sp_start > 0 else None

        venue_counts = arb.get("venue_symbol_counts") or {}
        venue_errors = arb.get("venue_errors") or {}
        source_health = {
            "arb_bybit_symbols": int(venue_counts.get("Bybit") or 0),
            "arb_bybit_error": venue_errors.get("Bybit"),
            "leadlag_state": leadlag.get("_health", "OK"),
            "orderbook_state": orderbook.get("_health", "OK"),
            "moex_state": moex.get("_health", "OK"),
        }

        return {
            "version": "EVIDENCE_DASHBOARD_V1",
            "generated_at": time.time(),
            "paper_only": True,
            "live_enabled": False,
            "portfolio_metrics": {
                "spot_perp_100usdt": {
                    "starting_capital_quote": sp_start,
                    "realized_equity_quote": sp_equity,
                    "realized_pnl_quote": self._f(spot_perp.get("realized_pnl_quote")),
                    "realized_return_pct": sp_realized_pct,
                    "open_positions": len(spot_perp.get("positions") or {}),
                    "resolved_positions": len(spot_perp.get("resolved") or []),
                    "metric_type": "CAPITAL_NORMALIZED_REALIZED",
                },
                "perp_funding_spread": {
                    "closed_pnl_quote": self._f(funding.get("closed_pnl_quote")),
                    "open_positions": len(funding.get("positions") or []),
                    "closed_positions": len(funding.get("closed") or []),
                    "metric_type": "REALIZED_QUOTE_IF_CLOSED",
                },
            },
            "research_edge_metrics": {
                "classic_spot_arb": {
                    "scan_count": int(arb.get("scan_count") or 0),
                    "execution_edge_quote_sum": self._f(arb.get("execution_edge_quote")),
                    "last_raw_positive": int(arb.get("last_raw_positive") or 0),
                    "last_after_fees_positive": int(arb.get("last_after_fees_positive") or 0),
                    "last_depth_checks": int(arb.get("last_depth_checks") or 0),
                    "last_depth_pass": int(arb.get("last_depth_pass") or 0),
                    "verification_candidates": int(arb.get("last_verification_candidates") or 0),
                    "metric_type": "SUM_OF_PAPER_EDGE_EVENTS_NOT_PORTFOLIO_RETURN",
                },
                "leadlag": {
                    "scan_count": int(leadlag.get("scan_count") or 0),
                    "pending": len(leadlag.get("pending") or []),
                    "resolved": len(ll_resolved),
                    "positive_edge_episodes": len(ll_positive),
                    "paper_edge_quote_sum": ll_edge_quote,
                    "error_count": int(leadlag.get("error_count") or 0),
                    "last_error": leadlag.get("last_error"),
                    "metric_type": "EPISODE_EDGE_NOT_PORTFOLIO_RETURN",
                },
                "orderbook_consensus": {
                    "pending": len(orderbook.get("pending") or []),
                    "resolved": len(ob_resolved),
                    "sum_trade_return_pct": sum(ob_returns),
                    "mean_trade_return_pct": (sum(ob_returns) / len(ob_returns)) if ob_returns else None,
                    "positive_trade_rate": (sum(1 for x in ob_returns if x > 0) / len(ob_returns)) if ob_returns else None,
                    "metric_type": "TRADE_RETURN_SERIES_NOT_CAPITAL_NORMALIZED",
                },
                "arb_verifier": {
                    "verified_count": int(verifier.get("verified_count") or 0),
                    "rejected_count": int(verifier.get("rejected_count") or 0),
                    "pending_count": len(verifier.get("pending") or []),
                    "metric_type": "IDENTITY_AND_EXECUTION_VALIDATION",
                },
            },
            "moex_research": {
                "broad_shadow": {
                    "resolved": len(moex.get("resolved") or []),
                    "pending": len(moex.get("pending") or []),
                    "note": "Do not interpret legacy equity-like sums as portfolio return unless capital-normalized.",
                },
                "calendar_matrix": {
                    "sample_keys": len(calendar.get("history") or calendar.get("samples") or {}),
                    "metric_type": "BASELINE_AND_SIGNAL_RESEARCH",
                },
            },
            "source_health": source_health,
            "policy": {
                "no_grid": True,
                "no_martingale": True,
                "no_dca": True,
                "no_live_orders": True,
                "no_strategy_ranking": True,
                "separate_portfolio_from_research_edge": True,
            },
        }

    def _save(self, payload: dict):
        DATA.mkdir(parents=True, exist_ok=True)
        tmp = STATE.with_suffix(STATE.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(STATE)

    async def refresh(self):
        try:
            payload = self._build()
            self._save(payload)
            self.last_error = None
            self.last_refresh = payload["generated_at"]
        except Exception as exc:
            self.last_error = str(exc)[:500]
        return self.status()

    def status(self):
        return {
            "ok": self.last_error is None,
            "strategy": "EVIDENCE_DASHBOARD_V1",
            "paper_only": True,
            "live_enabled": False,
            "last_refresh": self.last_refresh,
            "last_error": self.last_error,
        }

    async def start(self):
        if self.task and not self.task.done():
            return
        await self.refresh()
        self.task = asyncio.create_task(self._loop(), name="evidence-dashboard-v1")

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        self.task = None
        await self.refresh()

    async def _loop(self):
        while True:
            await asyncio.sleep(60)
            if self.enabled:
                await self.refresh()


evidence_dashboard_v1 = EvidenceDashboardV1()
