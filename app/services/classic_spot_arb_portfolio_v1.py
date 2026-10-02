from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from app.services.crossvenue_spot_arb_cloud_v1 import crossvenue_spot_arb_cloud_v1

ROOT = Path(__file__).parents[2]
DATA = ROOT / "data"
STATE = DATA / "classic_spot_arb_portfolio_v1.json"

STARTING_CAPITAL = 100.0
LEG_NOTIONAL = 25.0
CAPITAL_PER_TRADE = LEG_NOTIONAL * 2.0
REBALANCE_LOCK_SEC = 300.0
ELIGIBLE_IDENTITY = {
    "HIGH_MAJOR",
    "MEDIUM_MAJOR",
    "HIGH_4VENUE",
    "MEDIUM_3VENUE",
    "CONTRACT_VERIFIED",
}


def _blank():
    now = time.time()
    return {
        "version": "CLASSIC_SPOT_ARB_PORTFOLIO_V1_100USDT",
        "rule_frozen_at": now,
        "starting_capital_quote": STARTING_CAPITAL,
        "realized_equity_quote": STARTING_CAPITAL,
        "realized_pnl_quote": 0.0,
        "peak_equity_quote": STARTING_CAPITAL,
        "max_dd_pct": 0.0,
        "last_processed_ts": now,
        "locks": [],
        "trades": [],
        "skipped_capital": 0,
        "skipped_identity": 0,
        "skipped_nonpositive_conservative": 0,
    }


class ClassicSpotArbPortfolioV1:
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
        self.state["version"] = "CLASSIC_SPOT_ARB_PORTFOLIO_V1_100USDT"

    def _save(self):
        DATA.mkdir(parents=True, exist_ok=True)
        tmp = STATE.with_suffix(STATE.suffix + ".tmp")
        tmp.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(STATE)

    @staticmethod
    def _f(v, default=0.0):
        try:
            return float(v)
        except Exception:
            return default

    def _release(self, ts: float):
        self.state["locks"] = [
            x for x in (self.state.get("locks") or [])
            if self._f(x.get("release_ts")) > ts
        ]

    def _available_capital(self):
        equity = self._f(self.state.get("realized_equity_quote"), STARTING_CAPITAL)
        locked = sum(self._f(x.get("capital_quote")) for x in (self.state.get("locks") or []))
        return max(0.0, equity - locked)

    def _record_trade(self, ev: dict):
        ts = self._f(ev.get("ts"))
        self._release(ts)
        conf = str(ev.get("identity_confidence") or "")
        if conf not in ELIGIBLE_IDENTITY:
            self.state["skipped_identity"] = int(self.state.get("skipped_identity") or 0) + 1
            return False

        conservative = self._f(ev.get("conservative_pnl_quote"))
        if conservative <= 0:
            self.state["skipped_nonpositive_conservative"] = int(
                self.state.get("skipped_nonpositive_conservative") or 0
            ) + 1
            return False

        if self._available_capital() + 1e-9 < CAPITAL_PER_TRADE:
            self.state["skipped_capital"] = int(self.state.get("skipped_capital") or 0) + 1
            return False

        equity_before = self._f(self.state.get("realized_equity_quote"), STARTING_CAPITAL)
        equity_after = equity_before + conservative
        peak = max(self._f(self.state.get("peak_equity_quote"), STARTING_CAPITAL), equity_after)
        dd = (equity_after / peak - 1.0) * 100.0 if peak > 0 else 0.0

        trade = {
            "ts": ts,
            "signature": ev.get("signature"),
            "base": ev.get("base"),
            "buy_venue": ev.get("buy_venue"),
            "sell_venue": ev.get("sell_venue"),
            "identity_confidence": conf,
            "notional_per_leg_quote": LEG_NOTIONAL,
            "capital_required_quote": CAPITAL_PER_TRADE,
            "execution_net_pct": self._f(ev.get("execution_net_pct")),
            "conservative_net_pct": self._f(ev.get("conservative_net_pct")),
            "realized_conservative_pnl_quote": conservative,
            "equity_before_quote": equity_before,
            "equity_after_quote": equity_after,
            "release_ts": ts + REBALANCE_LOCK_SEC,
            "includes_bybit": ev.get("buy_venue") == "Bybit" or ev.get("sell_venue") == "Bybit",
        }
        self.state.setdefault("trades", []).append(trade)
        self.state["trades"] = self.state["trades"][-3000:]
        self.state.setdefault("locks", []).append({
            "signature": ev.get("signature"),
            "opened_ts": ts,
            "release_ts": ts + REBALANCE_LOCK_SEC,
            "capital_quote": CAPITAL_PER_TRADE,
        })
        self.state["realized_equity_quote"] = equity_after
        self.state["realized_pnl_quote"] = equity_after - STARTING_CAPITAL
        self.state["peak_equity_quote"] = peak
        self.state["max_dd_pct"] = min(self._f(self.state.get("max_dd_pct")), dd)
        return True

    def _refresh_sync(self):
        events = list(crossvenue_spot_arb_cloud_v1.state.get("events") or [])
        last = self._f(self.state.get("last_processed_ts"), self._f(self.state.get("rule_frozen_at")))
        frozen = self._f(self.state.get("rule_frozen_at"))
        new_events = [
            x for x in events
            if self._f(x.get("ts")) > last and self._f(x.get("ts")) >= frozen
            and str(x.get("quality_guard") or "").startswith("PRICE_VOLUME_CONSENSUS")
        ]
        new_events.sort(key=lambda x: (self._f(x.get("ts")), -self._f(x.get("conservative_net_pct"))))

        taken = 0
        max_ts = last
        for ev in new_events:
            max_ts = max(max_ts, self._f(ev.get("ts")))
            if self._record_trade(ev):
                taken += 1

        self.state["last_processed_ts"] = max_ts
        self._release(time.time())
        self.state["last_new_source_events"] = len(new_events)
        self.state["last_new_trades"] = taken
        self.state["locked_capital_quote"] = sum(
            self._f(x.get("capital_quote")) for x in (self.state.get("locks") or [])
        )
        self.state["available_capital_quote"] = self._available_capital()
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
        trades = self.state.get("trades") or []
        eq = self._f(self.state.get("realized_equity_quote"), STARTING_CAPITAL)
        pnl = eq - STARTING_CAPITAL
        bybit = sum(1 for x in trades if x.get("includes_bybit"))
        return {
            "ok": self.last_error is None,
            "strategy": "CLASSIC_SPOT_ARB_PORTFOLIO_V1_100USDT",
            "mode": "CAPITAL_NORMALIZED_PAPER",
            "paper_only": True,
            "live_enabled": False,
            "starting_capital_quote": STARTING_CAPITAL,
            "realized_equity_quote": eq,
            "realized_pnl_quote": pnl,
            "realized_return_pct": (eq / STARTING_CAPITAL - 1.0) * 100.0,
            "trade_count": len(trades),
            "bybit_trade_count": bybit,
            "locked_capital_quote": self.state.get("locked_capital_quote", 0.0),
            "available_capital_quote": self.state.get("available_capital_quote", eq),
            "max_dd_pct": self.state.get("max_dd_pct", 0.0),
            "last_new_source_events": self.state.get("last_new_source_events", 0),
            "last_new_trades": self.state.get("last_new_trades", 0),
            "skipped_capital": self.state.get("skipped_capital", 0),
            "skipped_identity": self.state.get("skipped_identity", 0),
            "skipped_nonpositive_conservative": self.state.get("skipped_nonpositive_conservative", 0),
            "recent_trades": trades[-20:],
            "last_error": self.last_error,
            "assumptions": {
                "notional_per_leg_quote": LEG_NOTIONAL,
                "capital_per_trade_quote": CAPITAL_PER_TRADE,
                "rebalance_lock_sec": REBALANCE_LOCK_SEC,
                "pnl_source": "DEPTH_VWAP_AFTER_FEES_MINUS_SAFETY_AND_REBALANCE_RESERVE",
                "contract_verified_high_edge_reenters_source_stream": True,
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
        self.task = asyncio.create_task(self._loop(), name="classic-spot-arb-portfolio-v1")

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        self.task = None
        await self.refresh()

    async def _loop(self):
        while True:
            await asyncio.sleep(15)
            if self.enabled:
                await self.refresh()


classic_spot_arb_portfolio_v1 = ClassicSpotArbPortfolioV1()
