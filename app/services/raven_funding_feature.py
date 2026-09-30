from __future__ import annotations
import asyncio,time
from pathlib import Path
import httpx,numpy as np,pandas as pd
from app.http_shared import SHARED_SSL_CONTEXT
ROOT=Path(__file__).parents[2]
SRC=ROOT/'data'/'funding_history_v1'/'funding_history_720d.csv'
SYMBOLS=['BTCUSDT','ETHUSDT','SOLUSDT','SUIUSDT','NEARUSDT','APTUSDT','INJUSDT','TIAUSDT','SEIUSDT','WIFUSDT','PEPEUSDT','FLOKIUSDT','ARBUSDT','OPUSDT','AAVEUSDT','RUNEUSDT','JUPUSDT']
ALIASES={'PEPEUSDT':'1000PEPEUSDT','FLOKIUSDT':'1000FLOKIUSDT'}

class RavenFundingFeature:
    def __init__(self):
        self.ttl=3600.0;self.last_fetch=0.0;self.recent=[];self.last_error=None
        try:
            x=pd.read_csv(SRC,low_memory=False);x=x[x.venue.isin(['binance','bybit'])].copy();x['funding_time_ms']=pd.to_numeric(x.funding_time_ms,errors='coerce');x['funding_rate']=pd.to_numeric(x.funding_rate,errors='coerce');self.base=x[['venue','symbol','funding_time_ms','funding_rate']].dropna()
        except Exception as exc:
            self.base=pd.DataFrame(columns=['venue','symbol','funding_time_ms','funding_rate']);self.last_error=f'BASE:{exc}'
    @staticmethod
    def _alias(symbol):return ALIASES.get(symbol,symbol)
    async def _one(self,c,venue,symbol):
        req=self._alias(symbol)
        if venue=='binance':
            r=await c.get('https://fapi.binance.com/fapi/v1/fundingRate',params={'symbol':req,'limit':12});r.raise_for_status();d=r.json();return [{'venue':venue,'symbol':symbol,'funding_time_ms':int(x['fundingTime']),'funding_rate':float(x['fundingRate'])} for x in d]
        r=await c.get('https://api.bybit.com/v5/market/funding/history',params={'category':'linear','symbol':req,'limit':12});r.raise_for_status();d=r.json()
        if int(d.get('retCode',-1))!=0:raise RuntimeError(f"BYBIT:{d.get('retCode')}:{d.get('retMsg')}")
        return [{'venue':venue,'symbol':symbol,'funding_time_ms':int(x['fundingRateTimestamp']),'funding_rate':float(x['fundingRate'])} for x in ((d.get('result') or {}).get('list') or [])]
    async def refresh_recent(self,force=False):
        now=time.time()
        if not force and self.recent and now-self.last_fetch<self.ttl:return True
        sem=asyncio.Semaphore(6);rows=[];errors=[]
        async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT,timeout=12,follow_redirects=True) as c:
            async def one(v,s):
                async with sem:
                    try:return await self._one(c,v,s)
                    except Exception as exc:errors.append(f'{v}:{s}:{str(exc)[:100]}');return []
            out=await asyncio.gather(*(one(v,s) for v in ('binance','bybit') for s in SYMBOLS))
        for z in out:rows.extend(z)
        if rows:self.recent=rows;self.last_fetch=now;self.last_error=';'.join(errors[:4]) if errors else None;return True
        self.last_error='NO_RECENT_FUNDING;'+';'.join(errors[:4]);return False
    async def snapshot(self,bar_ts,lag_bars=2):
        await self.refresh_recent()
        t=pd.Timestamp(bar_ts)
        if t.tzinfo is None:t=t.tz_localize('UTC')
        else:t=t.tz_convert('UTC')
        recent=pd.DataFrame(self.recent) if self.recent else pd.DataFrame(columns=self.base.columns)
        z=pd.concat([self.base,recent],ignore_index=True).drop_duplicates(['venue','symbol','funding_time_ms'],keep='last')
        if z.empty:return {'ok':False,'reason':'NO_FUNDING_DATA','ranks':{},'dispersion':0.0}
        z['dt']=pd.to_datetime(z.funding_time_ms.astype('int64'),unit='ms',utc=True);start=t-pd.Timedelta(days=5);z=z[(z.dt>=start)&(z.dt<=t+pd.Timedelta(hours=8))]
        z['bucket']=z.dt.dt.floor('8h');q=z.groupby(['bucket','symbol','venue']).funding_rate.sum().groupby(['bucket','symbol']).mean().unstack('symbol')
        grid=pd.date_range(start.floor('8h'),t.floor('8h'),freq='8h',tz='UTC');q=q.reindex(grid);f24=q.rolling(3,min_periods=2).sum().shift(int(lag_bars));row=f24.loc[t.floor('8h')] if t.floor('8h') in f24.index else pd.Series(dtype=float);row=row.dropna()
        if len(row)<6:return {'ok':False,'reason':'FUNDING_COVERAGE_LOW','coverage':int(len(row)),'ranks':{},'dispersion':0.0,'last_error':self.last_error}
        ranks=row.rank(pct=True);disp=float(row.max()-row.min());return {'ok':True,'lag_bars':int(lag_bars),'lag_hours':int(lag_bars)*8,'bar_ts':str(t),'coverage':int(len(row)),'ranks':{str(k):float(v) for k,v in ranks.items()},'dispersion':disp,'dispersion_pct':disp*100.0,'last_error':self.last_error,'sources':['binance','bybit']}

raven_funding_feature=RavenFundingFeature()
