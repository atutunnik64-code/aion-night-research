from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict, deque
from pathlib import Path

from app.services.crossvenue_spot_arb_cloud_v1 import TAKER, crossvenue_spot_arb_cloud_v1

ROOT = Path(__file__).parents[2]
DATA = ROOT / "data"
STATE = DATA / "crossvenue_leadlag_episode_cloud_v1.json"
LOOKBACK_SEC = 30.0
FAST_MOVE_PCT = 0.15
MAX_SLOW_MOVE_PCT = 0.05
CATCHUP_FRACTION = 0.70
TIMEOUT_SEC = 120.0
SAFETY_PCT = 0.05
REBALANCE_RESERVE_PCT = 0.10
NOTIONAL = 25.0
COOLDOWN_SEC = 300.0


def _fresh_state():
    return {
        "version": "CROSSVENUE_LEADLAG_EPISODE_CLOUD_V1",
        "started_at": time.time(),
        "pending": [],
        "resolved": [],
        "last_event_ts": {},
        "scan_count": 0,
        "error_count": 0,
    }


class CrossVenueLeadLagEpisodeCloudV1:
    def __init__(self):
        self.enabled = True
        self.live_enabled = False
        self.task = None
        self.last_error = None
        self.last_refresh = None
        self.hist = defaultdict(lambda: defaultdict(lambda: deque(maxlen=240)))
        self.state = self._load_state()

    def _load_state(self):
        try:
            raw = STATE.read_text(encoding="utf-8-sig")
            if not raw.strip():
                raise ValueError("empty state file")
            state = json.loads(raw)
            if not isinstance(state, dict):
                raise ValueError("state is not an object")
            state.setdefault("pending", [])
            state.setdefault("resolved", [])
            state.setdefault("last_event_ts", {})
            state.setdefault("scan_count", 0)
            state.setdefault("error_count", 0)
            return state
        except Exception as exc:
            state = _fresh_state()
            state["load_recovery_reason"] = str(exc)[:500]
            state["load_recovery_ts"] = time.time()
            return state

    def _save(self):
        DATA.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self.state, ensure_ascii=False, indent=2)
        tmp = STATE.with_suffix(STATE.suffix + ".tmp")
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(STATE)

    def _save_error(self, exc):
        self.last_error = str(exc)[:500]
        self.state["last_error"] = self.last_error
        self.state["last_error_ts"] = time.time()
        self.state["error_count"] = int(self.state.get("error_count") or 0) + 1
        try:
            self._save()
        except Exception as save_exc:
            self.last_error = f"{self.last_error}; state_save_error={save_exc}"[:500]

    @staticmethod
    def _mid(q):
        return (float(q["bid"]) + float(q["ask"])) / 2.0

    def _old_mid(self, base, venue, target):
        rows = self.hist[base][venue]
        for ts, mid in reversed(rows):
            if ts <= target:
                return mid
        return None

    def _record(self, market, now):
        for base, vm in market.items():
            for venue, q in vm.items():
                mid = self._mid(q)
                if mid > 0:
                    self.hist[base][venue].append((now, mid))

    def _open_candidates(self, market, now):
        pending = self.state.setdefault("pending", [])
        active = {(p["base"], p["fast_venue"], p["slow_venue"]) for p in pending}
        for base, vm in market.items():
            if len(vm) < 2:
                continue
            moves = {}
            for venue, q in vm.items():
                old = self._old_mid(base, venue, now - LOOKBACK_SEC)
                cur = self._mid(q)
                if old and old > 0:
                    moves[venue] = (cur / old - 1.0) * 100.0
            for fast, fast_move in moves.items():
                if abs(fast_move) < FAST_MOVE_PCT:
                    continue
                direction = 1.0 if fast_move > 0 else -1.0
                for slow, slow_move in moves.items():
                    if slow == fast or abs(slow_move) > MAX_SLOW_MOVE_PCT or slow_move * direction < 0:
                        continue
                    key = (base, fast, slow)
                    if key in active:
                        continue
                    lk = "|".join(key)
                    prev = float((self.state.get("last_event_ts") or {}).get(lk) or 0)
                    if now - prev < COOLDOWN_SEC:
                        continue
                    fq = vm[fast]
                    sq = vm[slow]
                    if direction > 0:
                        raw = (float(fq["bid"]) / float(sq["ask"]) - 1.0) * 100.0
                    else:
                        raw = (float(sq["bid"]) / float(fq["ask"]) - 1.0) * 100.0
                    fees = (TAKER[fast] + TAKER[slow]) * 100.0
                    all_in = raw - fees - SAFETY_PCT - REBALANCE_RESERVE_PCT
                    fast_mid = self._mid(fq)
                    slow_mid = self._mid(sq)
                    episode = {
                        "id": f"{base}:{fast}>{slow}:{int(now)}",
                        "base": base,
                        "fast_venue": fast,
                        "slow_venue": slow,
                        "opened_at": now,
                        "due_ts": now + TIMEOUT_SEC,
                        "direction": "UP" if direction > 0 else "DOWN",
                        "fast_move_30s_pct": fast_move,
                        "slow_move_30s_pct": slow_move,
                        "fast_entry_mid": fast_mid,
                        "slow_entry_mid": slow_mid,
                        "initial_gap_pct": (fast_mid / slow_mid - 1.0) * 100.0,
                        "raw_executable_pct": raw,
                        "trading_fee_pct": fees,
                        "safety_pct": SAFETY_PCT,
                        "rebalance_reserve_pct": REBALANCE_RESERVE_PCT,
                        "initial_all_in_net_pct": all_in,
                        "max_all_in_net_pct": all_in,
                        "max_gap_pct": abs((fast_mid / slow_mid - 1.0) * 100.0),
                        "catchup_fraction": 0.0,
                    }
                    pending.append(episode)
                    self.state.setdefault("last_event_ts", {})[lk] = now
                    active.add(key)

    def _resolve(self, market, now):
        keep = []
        resolved = self.state.setdefault("resolved", [])
        for p in self.state.get("pending") or []:
            vm = market.get(p["base"]) or {}
            fq = vm.get(p["fast_venue"])
            sq = vm.get(p["slow_venue"])
            if not fq or not sq:
                if now < p["due_ts"]:
                    keep.append(p)
                continue
            fm = self._mid(fq)
            sm = self._mid(sq)
            direction = 1.0 if p["direction"] == "UP" else -1.0
            slow_move = (sm / float(p["slow_entry_mid"]) - 1.0) * 100.0
            denom = max(abs(float(p["fast_move_30s_pct"])), 1e-9)
            catch = max(0.0, min(2.0, (slow_move * direction) / denom))
            gap = abs((fm / sm - 1.0) * 100.0)
            p["catchup_fraction"] = max(float(p.get("catchup_fraction") or 0), catch)
            p["max_gap_pct"] = max(float(p.get("max_gap_pct") or 0), gap)
            if direction > 0:
                raw = (float(fq["bid"]) / float(sq["ask"]) - 1.0) * 100.0
            else:
                raw = (float(sq["bid"]) / float(fq["ask"]) - 1.0) * 100.0
            all_in = raw - float(p["trading_fee_pct"]) - SAFETY_PCT - REBALANCE_RESERVE_PCT
            p["max_all_in_net_pct"] = max(float(p.get("max_all_in_net_pct") or -999), all_in)
            done = catch >= CATCHUP_FRACTION or now >= float(p["due_ts"])
            if not done:
                keep.append(p)
                continue
            out = {
                **p,
                "resolved_at": now,
                "resolution": "CAUGHT_UP" if catch >= CATCHUP_FRACTION else "TIMEOUT",
                "resolution_seconds": now - float(p["opened_at"]),
                "final_catchup_fraction": catch,
                "final_all_in_net_pct": all_in,
                "paper_edge_quote": NOTIONAL * max(0.0, float(p.get("max_all_in_net_pct") or 0)) / 100.0,
            }
            resolved.append(out)
        self.state["pending"] = keep
        self.state["resolved"] = resolved[-1000:]

    async def refresh(self):
        try:
            if not crossvenue_spot_arb_cloud_v1.market:
                await crossvenue_spot_arb_cloud_v1.refresh()
            market = crossvenue_spot_arb_cloud_v1.market
            now = time.time()
            self._record(market, now)
            self._resolve(market, now)
            self._open_candidates(market, now)
            self.state["scan_count"] = int(self.state.get("scan_count") or 0) + 1
            self.state["last_scan_ts"] = now
            self.state["last_error"] = None
            self._save()
            self.last_error = None
            self.last_refresh = now
        except Exception as exc:
            self._save_error(exc)
        return self.status()

    def status(self):
        r = self.state.get("resolved") or []
        caught = sum(1 for x in r if x.get("resolution") == "CAUGHT_UP")
        pos = sum(1 for x in r if float(x.get("max_all_in_net_pct") or 0) > 0)
        return {
            "ok": self.last_error is None,
            "strategy": "CROSSVENUE_LEADLAG_EPISODE_CLOUD_V1",
            "mode": "FUTURE_ONLY_EPISODES",
            "paper_only": True,
            "live_enabled": False,
            "locked_rule": {
                "lookback_sec": LOOKBACK_SEC,
                "fast_move_pct": FAST_MOVE_PCT,
                "max_slow_move_pct": MAX_SLOW_MOVE_PCT,
                "catchup_fraction": CATCHUP_FRACTION,
                "timeout_sec": TIMEOUT_SEC,
                "safety_pct": SAFETY_PCT,
                "rebalance_reserve_pct": REBALANCE_RESERVE_PCT,
                "notional_quote": NOTIONAL,
            },
            "scan_count": self.state.get("scan_count", 0),
            "pending_count": len(self.state.get("pending") or []),
            "resolved_count": len(r),
            "catchup_rate": caught / len(r) if r else None,
            "positive_all_in_episode_rate": pos / len(r) if r else None,
            "recent_resolved": r[-20:],
            "last_error": self.last_error,
            "error_count": self.state.get("error_count", 0),
            "policy": {
                "atomic_state_write": True,
                "error_state_persisted": True,
                "no_grid": True,
                "no_martingale": True,
                "no_dca": True,
                "no_live_orders": True,
                "no_parameter_tuning": True,
            },
        }

    async def start(self):
        if self.task and not self.task.done():
            return
        await self.refresh()
        self.task = asyncio.create_task(self._loop(), name="crossvenue-leadlag-episode-cloud-v1")

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        self.task = None
        try:
            self._save()
        except Exception as exc:
            self.last_error = str(exc)[:500]

    async def _loop(self):
        while True:
            await asyncio.sleep(5)
            if self.enabled:
                await self.refresh()


crossvenue_leadlag_episode_cloud_v1 = CrossVenueLeadLagEpisodeCloudV1()
