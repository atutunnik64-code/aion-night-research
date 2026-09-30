from __future__ import annotations
import asyncio, json, time
from pathlib import Path
from app.services.basis_funding import basis_funding_scanner
from app.services.perp_funding_spread import perp_funding_spread_scanner

ROOT=Path(__file__).parents[2]
OUT=ROOT/'data'/'market_alpha_history.jsonl'

class MarketAlphaHistory:
    def __init__(self):
        self.enabled=True
        self.interval=300.0
        self.task=None
        self.last_write=None
        self.last_error=None
        self.samples=0
        self.last_bucket=None

    def _snapshot(self):
        now=time.time()
        return {
            'ts':now,
            'bucket_5m':int(now//300),
            'basis_market':basis_funding_scanner.market,
            'basis_opportunities':basis_funding_scanner.rows,
            'funding_market':perp_funding_spread_scanner.market,
            'funding_spreads':perp_funding_spread_scanner.rows,
        }

    def capture(self):
        if not self.enabled:return self.status()
        try:
            row=self._snapshot(); bucket=row['bucket_5m']
            if self.last_bucket==bucket:return self.status()
            OUT.parent.mkdir(parents=True,exist_ok=True)
            with OUT.open('a',encoding='utf-8') as f:
                f.write(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n')
            self.last_bucket=bucket
            self.last_write=row['ts']
            self.samples+=1
            self.last_error=None
        except Exception as exc:
            self.last_error=str(exc)[:300]
        return self.status()

    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'DATA_COLLECTION_ONLY',
                'interval_seconds':self.interval,'last_write':self.last_write,'samples':self.samples,
                'last_error':self.last_error,'path':str(OUT)}

    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='market-alpha-history')

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None

    async def _loop(self):
        await asyncio.sleep(25)
        while True:
            self.capture()
            await asyncio.sleep(max(60.0,self.interval))

market_alpha_history=MarketAlphaHistory()
