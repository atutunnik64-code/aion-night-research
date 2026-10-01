from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RELAY = DATA / "bybit_local_relay_v1.json"
RUNTIME = Path(os.getenv("AION_BYBIT_RUNTIME", str(ROOT.parent / "aion-bybit-runtime")))
RUNTIME.mkdir(parents=True, exist_ok=True)

# Keep local collector state outside the git checkout so pulling/pushing the
# compact relay file never conflicts with cloud-generated state.
import app.services.crossvenue_spot_arb_cloud_v1 as arbmod
arbmod.STATE = RUNTIME / "crossvenue_spot_arb_local_v1.json"
arb = arbmod.CrossVenueSpotArbCloudV1()


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _bybit_events(limit: int = 100) -> list[dict]:
    rows = []
    for x in arb.state.get("events") or []:
        if x.get("buy_venue") == "Bybit" or x.get("sell_venue") == "Bybit":
            rows.append(x)
    return rows[-limit:]


def _bybit_signals(limit: int = 100) -> list[dict]:
    out = []
    for sig, row in (arb.state.get("signals") or {}).items():
        if ":Bybit>" in sig or ">Bybit" in sig:
            out.append({"signature": sig, **(row or {})})
    out.sort(key=lambda x: (float(x.get("last_seen") or 0), int(x.get("hits") or 0)), reverse=True)
    return out[:limit]


def _relay_payload() -> dict:
    counts = arb.state.get("venue_symbol_counts") or {}
    errs = arb.state.get("venue_errors") or {}
    market = arb.market or {}
    bybit_guarded = sum(1 for vm in market.values() if "Bybit" in vm)
    events = _bybit_events()
    signals = _bybit_signals()
    return {
        "version": "BYBIT_LOCAL_RELAY_V1",
        "generated_at": time.time(),
        "paper_only": True,
        "live_enabled": False,
        "source": "LOCAL_WINDOWS_DIRECT_BYBIT_PUBLIC_API",
        "source_health": {
            "bybit_symbol_count": int(counts.get("Bybit") or 0),
            "bybit_guarded_common_assets": bybit_guarded,
            "bybit_error": errs.get("Bybit"),
            "all_venue_symbol_counts": counts,
        },
        "spot_arb": {
            "scan_count": int(arb.state.get("scan_count") or 0),
            "last_raw_positive": int(arb.state.get("last_raw_positive") or 0),
            "last_after_fees_positive": int(arb.state.get("last_after_fees_positive") or 0),
            "last_depth_checks": int(arb.state.get("last_depth_checks") or 0),
            "last_depth_pass": int(arb.state.get("last_depth_pass") or 0),
            "execution_edge_quote_sum_all_venues": float(arb.state.get("execution_edge_quote") or 0),
            "recent_bybit_event_count": len(events),
            "recent_bybit_events": events,
            "bybit_signal_count": len(signals),
            "top_bybit_signals": signals,
        },
        "policy": {
            "no_grid": True,
            "no_martingale": True,
            "no_dca": True,
            "no_live_orders": True,
            "compact_relay_only": True,
        },
    }


def _git(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=ROOT, text=True, capture_output=True, check=check)


def sync_relay() -> tuple[bool, str]:
    try:
        # Relay is the only local file we intentionally publish. Runtime state
        # remains outside the checkout and therefore cannot conflict with cloud state.
        _git("fetch", "origin", "main")
        _git("pull", "--rebase", "--autostash", "origin", "main")
        _git("add", RELAY.relative_to(ROOT).as_posix())
        diff = _git("diff", "--cached", "--quiet", check=False)
        if diff.returncode == 0:
            return True, "NO_CHANGE"
        _git("-c", "user.name=aion-bybit-local", "-c", "user.email=aion-bybit-local@users.noreply.github.com", "commit", "-m", "research: Bybit local relay snapshot")
        _git("push", "origin", "HEAD:main")
        return True, "PUSHED"
    except Exception as exc:
        return False, f"{type(exc).__name__}:{exc}"


async def run(interval: float, sync_every: float, do_sync: bool, once: bool) -> None:
    last_sync = 0.0
    while True:
        await arb.refresh()
        payload = _relay_payload()
        _atomic_json(RELAY, payload)
        print(
            f"BYBIT_RELAY scan={payload['spot_arb']['scan_count']} "
            f"symbols={payload['source_health']['bybit_symbol_count']} "
            f"guarded={payload['source_health']['bybit_guarded_common_assets']} "
            f"events={payload['spot_arb']['recent_bybit_event_count']} "
            f"err={payload['source_health']['bybit_error']}",
            flush=True,
        )
        now = time.time()
        if do_sync and now - last_sync >= sync_every:
            ok, msg = sync_relay()
            print(f"BYBIT_RELAY_SYNC ok={ok} msg={msg}", flush=True)
            last_sync = now
        if once:
            return
        await asyncio.sleep(max(5.0, interval))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--interval", type=float, default=15.0)
    p.add_argument("--sync-every", type=float, default=300.0)
    p.add_argument("--sync", action="store_true")
    p.add_argument("--once", action="store_true")
    a = p.parse_args()
    asyncio.run(run(a.interval, a.sync_every, a.sync, a.once))


if __name__ == "__main__":
    main()
