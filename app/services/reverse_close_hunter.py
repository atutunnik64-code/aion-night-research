from __future__ import annotations
import asyncio,time,httpx
from app.http_shared import SHARED_SSL_CONTEXT
from app.services.depth_guard import depth_guard
from app.services.natural_rebalance_ledger import natural_rebalance_ledger

class ReverseCloseHunter:
    def __init__(self):
        self.enabled=True; self.interval=20.0; self.task=None; self.start_id=0
        self.last_refresh=None; self.last_error=None; self.latest={}
    async def _one(self,c,x):
        qty=float(x.get('remaining') or 0); bv=str(x.get('buy') or ''); sv=str(x.get('sell') or '')
        bs=str(x.get('buy_symbol') or ''); ss=str(x.get('sell_symbol') or '')
        if qty<=0 or not all((bv,sv,bs,ss)):return None
        try:
            (sbids,sasks),(bbids,basks)=await asyncio.gather(depth_guard._fetch(c,sv,ss),depth_guard._fetch(c,bv,bs))
            rb=depth_guard._consume(sasks,qty); rs=depth_guard._consume(bbids,qty)
            if not rb.get('ok') or not rs.get('ok'):return None
            buy_fee=float(x.get('sell_fee') or 0); sell_fee=float(x.get('buy_fee') or 0)
            reverse_pnl=float(rs['value'])*(1-sell_fee)-float(rb['value'])*(1+buy_fee)
            reserve=max(float(rb['value']),float(rs['value']))*.001
            combined=float(x.get('remaining_pnl') or 0)+reverse_pnl-reserve
            return {'source_execution_id':x.get('id'),'base':x.get('base'),'buy_venue':sv,'sell_venue':bv,'qty':qty,
                    'buy_symbol':ss,'sell_symbol':bs,'reverse_trade_pnl':reverse_pnl,'safety_reserve':reserve,
                    'source_unsettled_pnl':float(x.get('remaining_pnl') or 0),'combined_locked_estimate':combined,
                    'buy_vwap':rb.get('vwap'),'sell_vwap':rs.get('vwap')}
        except Exception:return None
    async def refresh(self):
        if not self.enabled:return self.status()
        try:
            natural_rebalance_ledger.refresh(self.start_id)
            residual=sorted(natural_rebalance_ledger.residual,key=lambda x:float(x.get('remaining_pnl') or 0),reverse=True)[:8]
            async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT,timeout=3.5,follow_redirects=True) as c:
                vals=await asyncio.gather(*[asyncio.wait_for(self._one(c,x),timeout=4.5) for x in residual],return_exceptions=True)
            rows=[x for x in vals if isinstance(x,dict)]
            for x in rows:
                x['status']='NETTING_CLOSE_POSITIVE' if float(x.get('combined_locked_estimate') or 0)>0 else 'WAIT'
            good=sorted([x for x in rows if x['status']=='NETTING_CLOSE_POSITIVE'],key=lambda x:x['combined_locked_estimate'],reverse=True)
            self.latest={'mode':'ANALYSIS_ONLY','residual_checked':len(residual),'candidate_count':len(good),
                         'potential_closed_pnl':round(sum(float(x['combined_locked_estimate']) for x in good),8),
                         'candidates':good[:20]}
            self.last_refresh=time.time(); self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:300]
        return self.status()
    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'ANALYSIS_ONLY','interval_seconds':self.interval,
                'last_refresh':self.last_refresh,'last_error':self.last_error,'latest':self.latest}
    async def start(self,start_id=0):
        self.start_id=int(start_id or 0)
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='reverse-close-hunter')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        await asyncio.sleep(10.0)
        while True:
            await self.refresh(); await asyncio.sleep(max(15.0,self.interval))

reverse_close_hunter=ReverseCloseHunter()