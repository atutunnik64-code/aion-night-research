from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from app.services.perp_funding_spread_paper import perp_funding_spread_paper

ROOT = Path(__file__).parents[2]
DATA = ROOT / "data"
STATE = DATA / "perp_funding_spread_portfolio_v1.json"

STARTING_CAPITAL = 100.0
CAPITAL_PER_POSITION = 50.0
MAX_POSITIONS = 2


def _blank():
    now = time.time()
    return {
        "version": "PERP_FUNDING_SPREAD_PORTFOLIO_V1_100USDT",
        "rule_frozen_at": now,
        "starting_capital_quote": STARTING_CAPITAL,
        "realized_equity_quote": STARTING_CAPITAL,
        "realized_pnl_quote": 0.0,
        "peak_equity_quote": STARTING_CAPITAL,
        "max_dd_pct": 0.0,
        "positions": {},
        "resolved": [],
        "seen_source_signatures": [],
        "skipped_capital": 0,
    }


class PerpFundingSpreadPortfolioV1:
    def __init__(self):
        self.enabled = True
        self.live_enabled = False
        self.task = None
        self.last_error = None
        self.last_refresh = None
        try:
            raw = STATE.read_text(encoding="utf-8-sig")
            self.state = json.loads(raw) if raw.strip() else _blank()
        except Exception:
            self.state = _blank()
        self.state["version"] = "PERP_FUNDING_SPREAD_PORTFOLIO_V1_100USDT"

    @staticmethod
    def _f(v, default=0.0):
        try:
            return float(v)
        except Exception:
            return default

    def _save(self):
        DATA.mkdir(parents=True, exist_ok=True)
        tmp = STATE.with_suffix(STATE.suffix + ".tmp")
        tmp.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(STATE)

    def _open_new(self):
        frozen = self._f(self.state.get("rule_frozen_at"))
        seen = set(self.state.get("seen_source_signatures") or [])
        positions = self.state.setdefault("positions", {})
        opened = 0

        source_positions = list(perp_funding_spread_paper.state.get("positions") or [])
        source_positions.sort(key=lambda x: self._f(x.get("opened_at")))
        for p in source_positions:
            sig = str(p.get("signature") or "")
            if not sig or sig in seen:
                continue
            if self._f(p.get("opened_at")) < frozen:
                seen.add(sig)
                continue
            if len(positions) >= MAX_POSITIONS:
                self.state["skipped_capital"] = int(self.state.get("skipped_capital") or 0) + 1
                seen.add(sig)
                continue
            positions[sig] = {
                "signature": sig,
                "base": p.get("base"),
                "long_venue": p.get("long_venue"),
                "short_venue": p.get("short_venue"),
                "opened_at": self._f(p.get("opened_at")),
                "capital_required_quote": CAPITAL_PER_POSITION,
                "entry_edge_24h_pct": self._f(p.get("entry_edge_24h_pct")),
                "basis_review": bool(p.get("basis_review")),
                "funding_interval_long_hours": self._f(p.get("long_interval_hours")),
                "funding_interval_short_hours": self._f(p.get("short_interval_hours")),
            }
            seen.add(sig)
            opened += 1

        self.state["seen_source_signatures"] = list(seen)[-20000:]
        return opened

    def _resolve(self):
        closed_map = {
            str(x.get("signature") or ""): x
            for x in (perp_funding_spread_paper.state.get("closed") or [])
            if x.get("signature")
        }
        positions = dict(self.state.get("positions") or {})
        closed = 0
        for sig, pos in list(positions.items()):
            src = closed_map.get(sig)
            if not src:
                continue
            pnl = self._f(src.get("realized_pnl"))
            before = self._f(self.state.get("realized_equity_quote"), STARTING_CAPITAL)
            after = before + pnl
            peak = max(self._f(self.state.get("peak_equity_quote"), STARTING_CAPITAL), after)
            dd = (after / peak - 1.0) * 100.0 if peak > 0 else 0.0
            out = {
                **pos,
                "closed_at": self._f(src.get("closed_at")),
                "close_reason": src.get("close_reason"),
                "funding_realized_quote": self._f(src.get("funding_realized")),
                "funding_events": int(src.get("funding_events") or 0),
                "realized_pnl_quote": pnl,
                "equity_before_quote": before,
                "equity_after_quote": after,
            }
            self.state.setdefault("resolved", []).append(out)
            self.state["resolved"] = self.state["resolved"][-5000:]
            self.state["realized_equity_quote"] = after
            self.state["realized_pnl_quote"] = after - STARTING_CAPITAL
            self.state["peak_equity_quote"] = peak
            self.state["max_dd_pct"] = min(self._f(self.state.get("max_dd_pct")), dd)
            positions.pop(sig, None)
            closed += 1
        self.state["positions"] = positions
        return closed

    def _refresh_sync(self):
        opened = self._open_new()
        closed = self._resolve()
        self.state["last_new_positions"] = opened
        self.state["last_new_resolved"] = closed
        self.state["last_refresh"] = time.time()
        self._save()

    async def refresh(self):
        try:
            await asyncio.to_thread(self._refresh_sync)
            self.last_error = None
        except Exception as exc:
            self.last_error = str(exc)[:500]
            self.state["last_error"] = self.last_error
            self.state["last_error_ts"] = time.time()
            try:
                self._save()
            except Exception:
                pass
        self.last_refresh = time.time()
        return self.status()

    def status(self):
        eq = self._f(self.state.get("realized_equity_quote"), STARTING_CAPITAL)
        rr = self.state.get("resolved") or []
        return {
            "ok": self.last_error is None,
            "strategy": "PERP_FUNDING_SPREAD_PORTFOLIO_V1_100USDT",
            "mode": "FUTURE_ONLY_CAPITAL_NORMALIZED_PAPER",
            "paper_only": True,
            "live_enabled": False,
            "starting_capital_quote": STARTING_CAPITAL,
            "realized_equity_quote": eq,
            "realized_pnl_quote": eq - STARTING_CAPITAL,
            "realized_return_pct": (eq / STARTING_CAPITAL - 1.0) * 100.0,
            "open_positions": len(self.state.get("positions") or {}),
            "resolved_positions": len(rr),
            "wins": sum(1 for x in rr if self._f(x.get("realized_pnl_quote")) > 0),
            "max_dd_pct": self._f(self.state.get("max_dd_pct")),
            "skipped_capital": int(self.state.get("skipped_capital") or 0),
            "last_new_positions": self.state.get("last_new_positions", 0),
            "last_new_resolved": self.state.get("last_new_resolved", 0),
            "recent_resolved": rr[-20:],
            "last_error": self.last_error,
            "assumptions": {
                "capital_per_position_quote": CAPITAL_PER_POSITION,
                "max_positions": MAX_POSITIONS,
                "source_realized_pnl_used_only_after_source_close": True,
                "no_historical_backfill": True,
            },
            "policy": {
                "no_grid": True,
                "no_martingale": True,
                "no_dca": True,
                "no_live_orders": True,
            },
        }

    async def start(self):
        if self.task and not self.task.done():
            return
        await self.refresh()
        self.task = asyncio.create_task(self._loop(), name="perp-funding-spread-portfolio-v1")

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        self.task = None
        await self.refresh()

    async def _loop(self):
        while True:
            await asyncio.sleep(30)
            if self.enabled:
                await self.refresh()


perp_funding_spread_portfolio_v1 = PerpFundingSpreadPortfolioV1()
