from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from app.services.moex_broad_futures_shadow_v1 import moex_broad_futures_shadow_v1

ROOT = Path(__file__).parents[2]
DATA = ROOT / "data"
STATE = DATA / "moex_broad_portfolio_v1.json"

STARTING_CAPITAL = 100.0
ALLOCATION_PER_POSITION = 10.0
MAX_POSITIONS = 10


def _blank():
    now = time.time()
    return {
        "version": "MOEX_BROAD_PORTFOLIO_V1_100_NORMALIZED",
        "rule_frozen_at": now,
        "starting_capital": STARTING_CAPITAL,
        "books": {
            "continuation": {
                "equity": STARTING_CAPITAL,
                "peak": STARTING_CAPITAL,
                "max_dd_pct": 0.0,
                "positions": {},
                "resolved": [],
                "skipped_capital": 0,
                "skipped_duplicate_asset": 0,
            },
            "reversal": {
                "equity": STARTING_CAPITAL,
                "peak": STARTING_CAPITAL,
                "max_dd_pct": 0.0,
                "positions": {},
                "resolved": [],
                "skipped_capital": 0,
                "skipped_duplicate_asset": 0,
            },
        },
        "seen_source_ids": [],
    }


class MoexBroadPortfolioV1:
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
        self.state["version"] = "MOEX_BROAD_PORTFOLIO_V1_100_NORMALIZED"

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

    def _book(self, kind):
        return self.state.setdefault("books", {}).setdefault(
            kind,
            {
                "equity": STARTING_CAPITAL,
                "peak": STARTING_CAPITAL,
                "max_dd_pct": 0.0,
                "positions": {},
                "resolved": [],
                "skipped_capital": 0,
                "skipped_duplicate_asset": 0,
            },
        )

    def _open_new(self):
        frozen = self._f(self.state.get("rule_frozen_at"))
        seen = set(self.state.get("seen_source_ids") or [])
        pending = list(moex_broad_futures_shadow_v1.state.get("pending") or [])
        opened = 0
        for p in sorted(pending, key=lambda x: self._f(x.get("opened_at"))):
            source_id = str(p.get("id") or "")
            if not source_id or source_id in seen:
                continue
            if self._f(p.get("opened_at")) < frozen:
                seen.add(source_id)
                continue
            kind = str(p.get("kind") or "")
            if kind not in ("continuation", "reversal"):
                seen.add(source_id)
                continue
            book = self._book(kind)
            pos = book.setdefault("positions", {})
            active_assets = {str(x.get("asset") or "") for x in pos.values()}
            asset = str(p.get("asset") or "")
            if asset and asset in active_assets:
                book["skipped_duplicate_asset"] = int(book.get("skipped_duplicate_asset") or 0) + 1
                seen.add(source_id)
                continue
            if len(pos) >= MAX_POSITIONS:
                book["skipped_capital"] = int(book.get("skipped_capital") or 0) + 1
                seen.add(source_id)
                continue
            pos[source_id] = {
                "source_id": source_id,
                "secid": p.get("secid"),
                "asset": asset,
                "tier": p.get("tier"),
                "kind": kind,
                "opened_at": self._f(p.get("opened_at")),
                "due_ts": self._f(p.get("due_ts")),
                "entry": self._f(p.get("entry")),
                "side": self._f(p.get("side")),
                "trigger_move_pct": self._f(p.get("trigger_move_pct")),
                "round_trip_cost_pct": self._f(p.get("round_trip_cost_pct")),
                "contract_value_rub": self._f(p.get("contract_value_rub")),
                "allocation_capital": ALLOCATION_PER_POSITION,
            }
            seen.add(source_id)
            opened += 1
        self.state["seen_source_ids"] = list(seen)[-30000:]
        return opened

    def _resolve(self):
        resolved_source = {
            str(x.get("id") or ""): x
            for x in (moex_broad_futures_shadow_v1.state.get("resolved") or [])
            if x.get("id")
        }
        closed = 0
        for kind in ("continuation", "reversal"):
            book = self._book(kind)
            positions = dict(book.get("positions") or {})
            for source_id, pos in list(positions.items()):
                src = resolved_source.get(source_id)
                if not src:
                    continue
                net_pct = self._f(src.get("net_return_pct"))
                pnl_capital = ALLOCATION_PER_POSITION * net_pct / 100.0
                contract_value = self._f(src.get("contract_value_rub"), self._f(pos.get("contract_value_rub")))
                one_contract_net_rub = contract_value * net_pct / 100.0 if contract_value > 0 else None

                eq_before = self._f(book.get("equity"), STARTING_CAPITAL)
                eq_after = eq_before + pnl_capital
                peak = max(self._f(book.get("peak"), STARTING_CAPITAL), eq_after)
                dd = (eq_after / peak - 1.0) * 100.0 if peak > 0 else 0.0

                out = {
                    **pos,
                    "exit": self._f(src.get("exit")),
                    "resolved_at": self._f(src.get("resolved_at")),
                    "gross_return_pct": self._f(src.get("gross_return_pct")),
                    "net_return_pct": net_pct,
                    "normalized_pnl": pnl_capital,
                    "equity_before": eq_before,
                    "equity_after": eq_after,
                    "one_contract_net_rub_stress_estimate": one_contract_net_rub,
                }
                book.setdefault("resolved", []).append(out)
                book["resolved"] = book["resolved"][-5000:]
                book["equity"] = eq_after
                book["peak"] = peak
                book["max_dd_pct"] = min(self._f(book.get("max_dd_pct")), dd)
                positions.pop(source_id, None)
                closed += 1
            book["positions"] = positions
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

    def _status_book(self, kind):
        b = self._book(kind)
        eq = self._f(b.get("equity"), STARTING_CAPITAL)
        rr = b.get("resolved") or []
        one_contract = [self._f(x.get("one_contract_net_rub_stress_estimate")) for x in rr if x.get("one_contract_net_rub_stress_estimate") is not None]
        return {
            "equity": eq,
            "pnl": eq - STARTING_CAPITAL,
            "return_pct": (eq / STARTING_CAPITAL - 1.0) * 100.0,
            "max_dd_pct": self._f(b.get("max_dd_pct")),
            "open_positions": len(b.get("positions") or {}),
            "resolved_positions": len(rr),
            "wins": sum(1 for x in rr if self._f(x.get("normalized_pnl")) > 0),
            "skipped_capital": int(b.get("skipped_capital") or 0),
            "skipped_duplicate_asset": int(b.get("skipped_duplicate_asset") or 0),
            "one_contract_net_rub_stress_sum": sum(one_contract),
            "recent_resolved": rr[-12:],
        }

    def status(self):
        return {
            "ok": self.last_error is None,
            "strategy": "MOEX_BROAD_PORTFOLIO_V1_100_NORMALIZED",
            "mode": "FUTURE_ONLY_CAPITAL_NORMALIZED_PAPER",
            "paper_only": True,
            "live_enabled": False,
            "rule_frozen_at": self.state.get("rule_frozen_at"),
            "starting_capital": STARTING_CAPITAL,
            "allocation_per_position": ALLOCATION_PER_POSITION,
            "max_positions_per_book": MAX_POSITIONS,
            "continuation": self._status_book("continuation"),
            "reversal": self._status_book("reversal"),
            "last_new_positions": self.state.get("last_new_positions", 0),
            "last_new_resolved": self.state.get("last_new_resolved", 0),
            "last_error": self.last_error,
            "assumptions": {
                "capital_is_normalized_not_rubles": True,
                "one_contract_rub_uses_last_rub_contract_value": True,
                "one_contract_rub_is_stress_estimate_not_broker_statement": True,
                "source_net_return_includes_tier_round_trip_cost_stress": True,
                "no_historical_backfill": True,
            },
            "policy": {
                "no_grid": True,
                "no_martingale": True,
                "no_dca": True,
                "no_live_orders": True,
                "continuation_and_reversal_kept_separate": True,
            },
        }

    async def start(self):
        if self.task and not self.task.done():
            return
        await self.refresh()
        self.task = asyncio.create_task(self._loop(), name="moex-broad-portfolio-v1")

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


moex_broad_portfolio_v1 = MoexBroadPortfolioV1()
