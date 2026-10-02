from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict
from pathlib import Path

import httpx
import websockets

from app.http_shared import SHARED_SSL_CONTEXT

ROOT = Path(__file__).parents[2]
DATA = ROOT / "data"
STATE = DATA / "bybit_spot_ws_mirror_v1.json"
WS_URL = "wss://stream.bybit.com/v5/public/spot"
MAX_TICKER_SYMBOLS = 350
MAX_BOOK_SYMBOLS = 180
STALE_SEC = 20.0
DISCOVERY_REFRESH_SEC = 3600.0
PING_SEC = 20.0


class BybitSpotWsMirrorV1:
    def __init__(self):
        self.enabled = True
        self.live_enabled = False
        self.task = None
        self.last_error = None
        self.last_refresh = None
        self.tickers: dict[str, dict] = {}
        self.books: dict[str, dict] = {}
        self.symbols: list[str] = []
        self.valid_symbols: set[str] = set()
        self._sub_batches: dict[str, list[str]] = {}
        self._individual_retry: set[str] = set()
        self.state = self._load_state()

    def _load_state(self):
        try:
            raw = STATE.read_text(encoding="utf-8-sig")
            if raw.strip():
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    return obj
        except Exception:
            pass
        return {"version": "BYBIT_SPOT_WS_MIRROR_V1", "started_at": time.time(), "connect_count": 0, "message_count": 0, "error_count": 0}

    def _save(self):
        DATA.mkdir(parents=True, exist_ok=True)
        now = time.time()
        fresh_tickers = {k: v for k, v in self.tickers.items() if now - float(v.get("ts") or 0) <= STALE_SEC}
        payload = {
            **self.state,
            "version": "BYBIT_SPOT_WS_MIRROR_V1",
            "last_save_ts": now,
            "configured_symbols": len(self.symbols),
            "valid_symbols": len(self.valid_symbols),
            "fresh_ticker_symbols": len(fresh_tickers),
            "fresh_book_symbols": sum(1 for v in self.books.values() if now - float(v.get("ts") or 0) <= STALE_SEC),
            "last_error": self.last_error,
        }
        tmp = STATE.with_suffix(STATE.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(STATE)

    @staticmethod
    def _f(v):
        try:
            return float(v or 0)
        except Exception:
            return 0.0

    async def _discover_symbols(self):
        # Bybit REST is 403 from many US-hosted GitHub runners. Build a broad
        # candidate universe from the other four exchanges, then let the
        # official Bybit public websocket confirm which symbols actually exist.
        maps = defaultdict(dict)
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=httpx.Timeout(12.0), follow_redirects=True) as c:
            async def okx():
                r = await c.get("https://www.okx.com/api/v5/market/tickers", params={"instType": "SPOT"}); r.raise_for_status()
                for x in r.json().get("data") or []:
                    s = str(x.get("instId") or "")
                    if s.endswith("-USDT"):
                        b = s[:-5]; maps[b]["OKX"] = self._f(x.get("volCcy24h"))
            async def bitget():
                r = await c.get("https://api.bitget.com/api/v2/spot/market/tickers"); r.raise_for_status()
                for x in r.json().get("data") or []:
                    s = str(x.get("symbol") or "")
                    if s.endswith("USDT"):
                        b = s[:-4]; maps[b]["Bitget"] = self._f(x.get("usdtVolume") or x.get("quoteVolume"))
            async def gate():
                r = await c.get("https://api.gateio.ws/api/v4/spot/tickers"); r.raise_for_status()
                for x in r.json() if isinstance(r.json(), list) else []:
                    s = str(x.get("currency_pair") or "")
                    if s.endswith("_USDT"):
                        b = s[:-5]; maps[b]["Gate"] = self._f(x.get("quote_volume"))
            async def kucoin():
                r = await c.get("https://api.kucoin.com/api/v1/market/allTickers"); r.raise_for_status()
                for x in ((r.json().get("data") or {}).get("ticker") or []):
                    s = str(x.get("symbol") or "")
                    if s.endswith("-USDT"):
                        b = s[:-5]; maps[b]["KuCoin"] = self._f(x.get("volValue"))
            await asyncio.gather(okx(), bitget(), gate(), kucoin(), return_exceptions=True)

        rows = []
        for base, vm in maps.items():
            if len(vm) < 2 or not base or len(base) > 24:
                continue
            score = max(vm.values() or [0.0]) + 1_000_000.0 * len(vm)
            rows.append((score, base))
        rows.sort(reverse=True)
        self.symbols = [base + "USDT" for _, base in rows[:MAX_TICKER_SYMBOLS]]
        self.state["discovery_candidates"] = len(rows)
        self.state["discovery_ts"] = time.time()

    async def _send_batches(self, ws, topics: list[str], prefix: str):
        for i in range(0, len(topics), 10):
            batch = topics[i:i + 10]
            req_id = f"{prefix}-{i//10}-{int(time.time()*1000)}"
            self._sub_batches[req_id] = batch
            await ws.send(json.dumps({"req_id": req_id, "op": "subscribe", "args": batch}))
            await asyncio.sleep(0.03)

    async def _subscribe_initial(self, ws):
        # Spot ticker snapshots provide last/volume/turnover but, unlike
        # derivatives tickers, do not provide best bid/ask. Subscribe to
        # orderbooks separately for the highest-priority candidate symbols.
        await self._send_batches(ws, [f"tickers.{s}" for s in self.symbols], "ticker")
        await self._send_batches(
            ws,
            [f"orderbook.50.{s}" for s in self.symbols[:MAX_BOOK_SYMBOLS]],
            "book-initial",
        )

    async def _subscribe_book(self, ws, symbol: str):
        if symbol not in self.symbols[:MAX_BOOK_SYMBOLS]:
            return
        if len(self.books) >= MAX_BOOK_SYMBOLS and symbol not in self.books:
            return
        topic = f"orderbook.50.{symbol}"
        req_id = f"book-{symbol}-{int(time.time()*1000)}"
        self._sub_batches[req_id] = [topic]
        await ws.send(json.dumps({"req_id": req_id, "op": "subscribe", "args": [topic]}))

    def _apply_book(self, symbol: str, data: dict, typ: str, ts: float):
        rec = self.books.setdefault(symbol, {"b": {}, "a": {}, "ts": ts})
        if typ == "snapshot":
            rec["b"] = {}; rec["a"] = {}
        for side_key, out_key in (("b", "b"), ("a", "a")):
            for row in data.get(side_key) or []:
                try:
                    px = float(row[0]); qty = float(row[1])
                except Exception:
                    continue
                if qty <= 0:
                    rec[out_key].pop(px, None)
                else:
                    rec[out_key][px] = qty
        rec["ts"] = ts

    async def _handle(self, ws, msg: dict):
        self.state["message_count"] = int(self.state.get("message_count") or 0) + 1
        if msg.get("op") == "subscribe" and msg.get("success") is False:
            req_id = str(msg.get("req_id") or "")
            batch = self._sub_batches.get(req_id) or []
            # If one invalid ticker poisoned a 10-symbol batch, retry symbols
            # individually so the rest of the valid Bybit universe is not lost.
            if len(batch) > 1:
                for topic in batch:
                    if topic in self._individual_retry:
                        continue
                    self._individual_retry.add(topic)
                    rid = f"retry-{len(self._individual_retry)}-{int(time.time()*1000)}"
                    self._sub_batches[rid] = [topic]
                    await ws.send(json.dumps({"req_id": rid, "op": "subscribe", "args": [topic]}))
                    await asyncio.sleep(0.02)
            return

        topic = str(msg.get("topic") or "")
        ts = self._f(msg.get("ts")) / 1000.0 or time.time()
        if topic.startswith("tickers."):
            symbol = topic.split(".", 1)[1]
            data = msg.get("data") or {}
            if isinstance(data, list):
                data = data[0] if data else {}
            # Bybit spot ticker payload intentionally has no bid1Price/ask1Price.
            # Treat lastPrice + turnover as ticker validity and obtain executable
            # bid/ask from the dedicated orderbook stream.
            last = self._f(data.get("lastPrice"))
            qv = self._f(data.get("turnover24h"))
            bid = self._f(data.get("bid1Price"))
            ask = self._f(data.get("ask1Price"))
            if last > 0 or (bid > 0 and ask > 0):
                self.valid_symbols.add(symbol)
                self.tickers[symbol] = {
                    "last": last,
                    "bid": bid,
                    "ask": ask,
                    "quote_volume": qv,
                    "ts": ts,
                }
        elif topic.startswith("orderbook.50."):
            symbol = topic.rsplit(".", 1)[1]
            data = msg.get("data") or {}
            self.valid_symbols.add(symbol)
            self._apply_book(symbol, data, str(msg.get("type") or "delta"), ts)

    async def _connection_loop(self):
        while self.enabled:
            try:
                await self._discover_symbols()
                async with websockets.connect(WS_URL, ping_interval=None, close_timeout=5, max_size=4_000_000) as ws:
                    self.state["connect_count"] = int(self.state.get("connect_count") or 0) + 1
                    self.state["last_connect_ts"] = time.time()
                    self.last_error = None
                    await self._subscribe_initial(ws)
                    last_ping = time.time(); last_save = 0.0; last_discovery = time.time()
                    while self.enabled:
                        timeout = max(1.0, PING_SEC - (time.time() - last_ping))
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                            msg = json.loads(raw)
                            if isinstance(msg, dict):
                                await self._handle(ws, msg)
                        except asyncio.TimeoutError:
                            await ws.send(json.dumps({"op": "ping"}))
                            last_ping = time.time()
                        now = time.time()
                        if now - last_save > 30:
                            self._save(); last_save = now
                        if now - last_discovery > DISCOVERY_REFRESH_SEC:
                            # Reconnect after rediscovery so newly listed common
                            # markets can enter the mirror without a process restart.
                            break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = f"{type(exc).__name__}:{str(exc)[:300]}"
                self.state["error_count"] = int(self.state.get("error_count") or 0) + 1
                self.state["last_error_ts"] = time.time()
                self._save()
                await asyncio.sleep(5)

    def ticker_snapshot(self):
        now = time.time(); out = {}
        for symbol, q in self.tickers.items():
            if now - float(q.get("ts") or 0) > STALE_SEC or not symbol.endswith("USDT"):
                continue
            rec = self.books.get(symbol)
            if not rec or now - float(rec.get("ts") or 0) > STALE_SEC:
                continue
            bids = rec.get("b") or {}
            asks = rec.get("a") or {}
            if not bids or not asks:
                continue
            bid = max(bids)
            ask = min(asks)
            if bid <= 0 or ask <= 0 or ask < bid:
                continue
            base = symbol[:-4]
            out[base] = {
                "venue": "Bybit",
                "base": base,
                "bid": bid,
                "ask": ask,
                "quote_volume": q.get("quote_volume", 0),
                "source": "BYBIT_WS_BOOK_PLUS_TICKER",
                "source_ts": min(float(q.get("ts") or 0), float(rec.get("ts") or 0)),
            }
        return out

    def book_snapshot(self, base: str):
        symbol = base + "USDT"; rec = self.books.get(symbol)
        if not rec or time.time() - float(rec.get("ts") or 0) > STALE_SEC:
            return None
        bids = sorted(rec.get("b", {}).items(), reverse=True)[:50]
        asks = sorted(rec.get("a", {}).items())[:50]
        if not bids or not asks:
            return None
        return {"bids": bids, "asks": asks, "source": "BYBIT_WS", "source_ts": rec.get("ts")}

    def status(self):
        now = time.time()
        return {
            "ok": self.last_error is None,
            "strategy": "BYBIT_SPOT_WS_MIRROR_V1",
            "paper_only": True,
            "live_enabled": False,
            "configured_symbols": len(self.symbols),
            "valid_symbols": len(self.valid_symbols),
            "fresh_ticker_symbols": sum(1 for q in self.tickers.values() if now - float(q.get("ts") or 0) <= STALE_SEC),
            "fresh_book_symbols": sum(1 for q in self.books.values() if now - float(q.get("ts") or 0) <= STALE_SEC),
            "connect_count": self.state.get("connect_count", 0),
            "message_count": self.state.get("message_count", 0),
            "error_count": self.state.get("error_count", 0),
            "last_error": self.last_error,
            "source": WS_URL,
        }

    async def start(self):
        if self.task and not self.task.done():
            return
        self.task = asyncio.create_task(self._connection_loop(), name="bybit-spot-ws-mirror-v1")
        # Give the websocket a short head start without blocking the whole agent.
        await asyncio.sleep(1.0)

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel(); await asyncio.gather(self.task, return_exceptions=True)
        self.task = None
        self._save()


bybit_spot_ws_mirror_v1 = BybitSpotWsMirrorV1()
