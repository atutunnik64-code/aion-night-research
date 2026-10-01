from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from app.services.basis_funding import basis_funding_scanner
from app.services.crossvenue_spot_perp_shadow_v2 import crossvenue_spot_perp_shadow_v2

ROOT = Path(__file__).parents[2]
STATE = ROOT / "data" / "crossvenue_spot_perp_portfolio_v1.json"
START_CAPITAL = 100.0
NOTIONAL_PER_LEG = 25.0
CAPITAL_PER_POSITION = 2.0 * NOTIONAL_PER_LEG
MAX_POSITIONS = int(START_CAPITAL // CAPITAL_PER_POSITION)
THRESHOLD_CONSERVATIVE_NET_PCT = 0.05
CONFIRM_SCANS = 3
HORIZON_SEC = 8 * 3600.0
MAX_RESOLUTION_LAG_SEC = 300.0


class CrossVenueSpotPerpPortfolioV1:
    def __init__(self):
        self.enabled = True
        self.live_enabled = False
        self.task = None
        self.last_error = None
        self.last_refresh = None
        try:
            self.state = json.loads(STATE.read_text(encoding="utf-8-sig"))
        except Exception:
            self.state = {
                "version": "CROSSVENUE_SPOT_PERP_PORTFOLIO_V1_100USDT",
                "rule_frozen_at": time.time(),
                "starting_capital_quote": START_CAPITAL,
                "realized_equity_quote": START_CAPITAL,
                "realized_pnl_quote": 0.0,
                "positions": {},
                "resolved": [],
                "aborted": [],
                "confirm": {},
                "scan_count": 0,
            }
        self.state["version"] = "CROSSVENUE_SPOT_PERP_PORTFOLIO_V1_100USDT"
        self.state.setdefault("positions", {})
        self.state.setdefault("resolved", [])
        self.state.setdefault("aborted", [])
        self.state.setdefault("confirm", {})

    def _save(self):
        STATE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE.with_suffix(STATE.suffix + ".tmp")
        tmp.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(STATE)

    def _available_capital(self):
        eq = float(self.state.get("realized_equity_quote") or START_CAPITAL)
        locked = len(self.state.get("positions") or {}) * CAPITAL_PER_POSITION
        return max(0.0, eq - locked)

    def _resolve(self, sig, pos, row, now):
        qty = float(pos["qty"])
        sbid = float(row["spot_bid"])
        pask = float(row["perp_ask"])
        sf = float(pos["spot_fee_pct"]) / 100.0
        pf = float(pos["perp_fee_pct"]) / 100.0
        gross = qty * (sbid - float(pos["spot_entry"])) + qty * (float(pos["perp_entry"]) - pask)
        exit_fees = qty * sbid * sf + qty * pask * pf
        fees = float(pos["entry_fees"]) + exit_fees
        pnl = gross - fees + float(pos.get("funding_quote") or 0.0)
        rec = {
            **pos,
            "resolved_at": now,
            "resolution_lag_sec": max(0.0, now - float(pos["due_ts"])),
            "exit_spot_bid": sbid,
            "exit_perp_ask": pask,
            "gross_quote": gross,
            "fees_quote": fees,
            "funding_quote": float(pos.get("funding_quote") or 0.0),
            "pnl_quote": pnl,
            "return_on_position_capital_pct": pnl / CAPITAL_PER_POSITION * 100.0,
            "stress_2x_pnl_quote": pnl - fees,
            "stress_3x_pnl_quote": pnl - 2.0 * fees,
        }
        self.state.setdefault("resolved", []).append(rec)
        self.state["resolved"] = self.state["resolved"][-1500:]
        self.state["positions"].pop(sig, None)
        self.state["realized_pnl_quote"] = float(self.state.get("realized_pnl_quote") or 0.0) + pnl
        self.state["realized_equity_quote"] = START_CAPITAL + float(self.state["realized_pnl_quote"])

    def _abort(self, sig, pos, reason, now):
        self.state.setdefault("aborted", []).append({**pos, "aborted_at": now, "abort_reason": reason, "pnl_counted": False})
        self.state["aborted"] = self.state["aborted"][-1500:]
        self.state["positions"].pop(sig, None)

    async def refresh(self):
        now = time.time()
        try:
            if not basis_funding_scanner.market or not basis_funding_scanner.last_refresh or now - float(basis_funding_scanner.last_refresh) > 240:
                await basis_funding_scanner.refresh()
            routes = crossvenue_spot_perp_shadow_v2._routes()
            by = {x["signature"]: x for x in routes}
            positions = self.state.setdefault("positions", {})

            # Update funding and close only inside a bounded future-only resolution window.
            for sig, pos in list(positions.items()):
                row = by.get(sig)
                due = float(pos.get("due_ts") or 0.0)
                if not row:
                    if now > due + MAX_RESOLUTION_LAG_SEC:
                        self._abort(sig, pos, "ROUTE_MISSING_AT_RESOLUTION", now)
                    continue
                elapsed = max(0.0, now - float(pos.get("last_mark") or now))
                mid = (float(row["perp_bid"]) + float(row["perp_ask"])) / 2.0
                interval = max(1.0, float(row.get("funding_interval_hours") or pos.get("funding_interval_hours") or 8.0))
                pos["funding_quote"] = float(pos.get("funding_quote") or 0.0) + float(pos["qty"]) * mid * (float(row["funding_rate_pct"]) / 100.0) * (elapsed / (interval * 3600.0))
                pos["last_mark"] = now
                pos["funding_interval_hours"] = interval
                if now < due:
                    continue
                if now - due > MAX_RESOLUTION_LAG_SEC:
                    self._abort(sig, pos, "STALE_RESOLUTION_WINDOW", now)
                    continue
                self._resolve(sig, pos, row, now)

            # Independent confirmation state for the capital-constrained portfolio.
            confirm = self.state.setdefault("confirm", {})
            eligible = {
                x["signature"]: x
                for x in routes
                if not x.get("verification_required")
                and float(x.get("conservative_net_basis_pct") or 0.0) >= THRESHOLD_CONSERVATIVE_NET_PCT
            }
            for sig in list(confirm):
                if sig not in eligible:
                    confirm[sig] = 0
            for sig in eligible:
                confirm[sig] = int(confirm.get(sig) or 0) + 1

            # Fill best confirmed routes until the actual $100 capital budget is exhausted.
            active_bases = {p.get("base") for p in positions.values()}
            ready = [x for x in eligible.values() if int(confirm.get(x["signature"]) or 0) >= CONFIRM_SCANS]
            ready.sort(key=lambda x: float(x.get("conservative_net_basis_pct") or 0.0), reverse=True)
            opened = 0
            for row in ready:
                if len(positions) >= MAX_POSITIONS or self._available_capital() + 1e-9 < CAPITAL_PER_POSITION:
                    break
                sig = row["signature"]
                if sig in positions or row["base"] in active_bases:
                    continue
                sf = float(row["spot_fee_pct"]) / 100.0
                pf = float(row["perp_fee_pct"]) / 100.0
                qty = NOTIONAL_PER_LEG / float(row["spot_ask"])
                positions[sig] = {
                    "signature": sig,
                    "base": row["base"],
                    "spot_venue": row["spot_venue"],
                    "perp_venue": row["perp_venue"],
                    "opened_at": now,
                    "due_ts": now + HORIZON_SEC,
                    "last_mark": now,
                    "notional_per_leg": NOTIONAL_PER_LEG,
                    "capital_required_quote": CAPITAL_PER_POSITION,
                    "qty": qty,
                    "spot_entry": float(row["spot_ask"]),
                    "perp_entry": float(row["perp_bid"]),
                    "spot_fee_pct": float(row["spot_fee_pct"]),
                    "perp_fee_pct": float(row["perp_fee_pct"]),
                    "entry_fees": NOTIONAL_PER_LEG * sf + qty * float(row["perp_bid"]) * pf,
                    "funding_quote": 0.0,
                    "funding_interval_hours": float(row.get("funding_interval_hours") or 8.0),
                    "entry_execution_net_basis_pct": float(row["execution_net_basis_pct"]),
                    "entry_conservative_net_basis_pct": float(row["conservative_net_basis_pct"]),
                    "entry_raw_basis_pct": float(row["raw_basis_pct"]),
                    "identity_confidence": row.get("identity_confidence"),
                }
                confirm[sig] = 0
                active_bases.add(row["base"])
                opened += 1

            self.state["positions"] = positions
            self.state["confirm"] = confirm
            self.state["scan_count"] = int(self.state.get("scan_count") or 0) + 1
            self.state["last_scan_ts"] = now
            self.state["last_new_positions"] = opened
            self.state["eligible_routes_now"] = len(eligible)
            self._save()
            self.last_error = None
        except Exception as exc:
            self.last_error = str(exc)[:500]
            self.state["last_error"] = self.last_error
            self.state["last_error_ts"] = now
            try:
                self._save()
            except Exception:
                pass
        self.last_refresh = now
        return self.status()

    def status(self):
        eq = float(self.state.get("realized_equity_quote") or START_CAPITAL)
        pos = self.state.get("positions") or {}
        return {
            "ok": self.last_error is None,
            "strategy": "CROSSVENUE_SPOT_PERP_PORTFOLIO_V1_100USDT",
            "mode": "FUTURE_ONLY_CAPITAL_CONSTRAINED_PAPER",
            "paper_only": True,
            "live_enabled": False,
            "starting_capital_quote": START_CAPITAL,
            "realized_equity_quote": eq,
            "realized_pnl_quote": float(self.state.get("realized_pnl_quote") or 0.0),
            "realized_return_pct": (eq / START_CAPITAL - 1.0) * 100.0,
            "open_positions": len(pos),
            "max_positions": MAX_POSITIONS,
            "capital_per_position_quote": CAPITAL_PER_POSITION,
            "locked_capital_quote": len(pos) * CAPITAL_PER_POSITION,
            "available_capital_quote": self._available_capital(),
            "open_funding_accrued_quote": sum(float(x.get("funding_quote") or 0.0) for x in pos.values()),
            "resolved_count": len(self.state.get("resolved") or []),
            "aborted_no_pnl": len(self.state.get("aborted") or []),
            "last_new_positions": self.state.get("last_new_positions", 0),
            "eligible_routes_now": self.state.get("eligible_routes_now", 0),
            "scan_count": self.state.get("scan_count", 0),
            "last_error": self.last_error,
            "locked_rule": {
                "confirmation_scans": CONFIRM_SCANS,
                "conservative_entry_pct": THRESHOLD_CONSERVATIVE_NET_PCT,
                "hold_hours": HORIZON_SEC / 3600.0,
                "notional_per_leg_quote": NOTIONAL_PER_LEG,
                "starting_capital_quote": START_CAPITAL,
                "max_resolution_lag_sec": MAX_RESOLUTION_LAG_SEC,
            },
            "policy": {
                "actual_capital_budget_enforced": True,
                "two_leg_capital_counted": True,
                "one_position_per_base": True,
                "high_basis_review_required": True,
                "no_grid": True,
                "no_martingale": True,
                "no_dca": True,
                "no_parameter_tuning": True,
                "no_live_orders": True,
            },
        }

    async def start(self):
        if self.task and not self.task.done():
            return
        await self.refresh()
        self.task = asyncio.create_task(self._loop(), name="crossvenue-spot-perp-portfolio-v1")

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        self.task = None

    async def _loop(self):
        while True:
            await asyncio.sleep(65)
            if self.enabled:
                await self.refresh()


crossvenue_spot_perp_portfolio_v1 = CrossVenueSpotPerpPortfolioV1()
