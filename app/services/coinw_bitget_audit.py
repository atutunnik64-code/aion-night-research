from __future__ import annotations
import time,asyncio,json
from pathlib import Path
from app.connectors.cex_public import CoinWPublicConnector
from app.services.live_executor import live_executor
from app.services.hot_book_cache import hot_book_cache
from app.services.depth_guard import depth_guard
from app.services.order_rules import order_rules_registry
from app.services.private_trading import readiness

class CoinWBitgetAudit:
    def __init__(self):
        self.last=None
        self.history_path=Path(__file__).parents[2]/'data'/'coinw_bitget_audit_history.json'
        self.audit_log_path=Path(__file__).parents[2]/'data'/'coinw_bitget_audit_log.jsonl'
        self.auto_enabled=False
        self.auto_interval=30.0
        self.auto_amount=25.0
        self.auto_task=None
        self.running=False
        self.scan_count=0
        self.last_started_at=None
        self.last_completed_at=None
        self.next_run_at=None
        self.last_error=None
        try:self.history=json.loads(self.history_path.read_text(encoding='utf-8'))[-50:]
        except Exception:self.history=[]
        self.scan_count=max([int(x.get('scan_id') or 0) for x in self.history] or [len(self.history)])
    def auto_status(self):
        return {'enabled':bool(self.auto_enabled),'interval_seconds':float(self.auto_interval),'amount':float(self.auto_amount),
                'running':bool(self.running),'scan_count':int(self.scan_count),'last_started_at':self.last_started_at,
                'last_completed_at':self.last_completed_at,'next_run_at':self.next_run_at,'last_error':self.last_error}
    def snapshot(self):
        base=dict(self.last or {'ok':True,'environment':'DEMO_AUDIT','ready':False,'real_orders':False,'real_money':False,'amount':self.auto_amount,'history':self.history[-30:]})
        base['auto']=self.auto_status(); base['history']=self.history[-30:]; return base
    async def start_auto(self,interval=30.0,amount=25.0):
        self.auto_interval=max(15.0,float(interval)); self.auto_amount=max(10.0,min(1000.0,float(amount))); self.auto_enabled=True
        if self.auto_task is None or self.auto_task.done(): self.auto_task=asyncio.create_task(self._auto_loop(),name='coinw-bitget-auto-audit')
        return self.auto_status()
    async def stop_auto(self):
        self.auto_enabled=False
        task=self.auto_task; self.auto_task=None
        if task and not task.done():
            task.cancel()
            try: await task
            except BaseException: pass
        self.next_run_at=None; return self.auto_status()
    async def _auto_loop(self):
        await asyncio.sleep(2.0)
        while self.auto_enabled:
            t0=time.monotonic()
            try: await self.run(self.auto_amount)
            except asyncio.CancelledError: raise
            except Exception as exc: self.last_error=f'{type(exc).__name__}: {str(exc)[:180]}'
            elapsed=time.monotonic()-t0; wait=max(5.0,self.auto_interval-elapsed)
            self.next_run_at=time.time()+wait
            await asyncio.sleep(wait)
    def _save_history(self):
        try:
            self.history_path.parent.mkdir(parents=True,exist_ok=True)
            self.history_path.write_text(json.dumps(self.history[-50:],ensure_ascii=False,indent=2),encoding='utf-8')
        except Exception:pass
    def _append_log(self,result):
        try:
            rec={'ts':time.time(),'scan_id':int(result.get('scan_id') or self.scan_count),'amount':float(result.get('amount') or 0),
                 'ws_ready_pairs':int(result.get('ws_ready_pairs') or 0),'candidate_count':int(result.get('candidate_count') or 0),
                 'market_positive_count':int(result.get('market_positive_count') or 0),'strict_pass_count':int(result.get('strict_pass_count') or 0),
                 'top_market':list(result.get('top_market') or [])[:10],'audits':list(result.get('audits') or [])}
            with self.audit_log_path.open('a',encoding='utf-8') as f:f.write(json.dumps(rec,ensure_ascii=False,separators=(',',':'))+'\n')
        except Exception:pass
    @staticmethod
    def _row(base,quote,buy_venue,sell_venue,buy_symbol,sell_symbol,ask,ask_qty,bid,bid_qty,buy_fee,sell_fee,amount):
        quote_in=min(float(amount),ask*ask_qty,bid_qty*ask/max(1e-12,1-buy_fee))
        if quote_in<10:return None
        base_out=quote_in/ask*(1-buy_fee); quote_out=base_out*bid*(1-sell_fee)
        net=quote_out-quote_in; net_pct=net/quote_in*100.0
        return {'base':base,'quote':quote,'buy_venue':buy_venue,'sell_venue':sell_venue,'quote_in':round(quote_in,8),'base_bought':round(base_out,10),
                'quote_out':round(quote_out,8),'net_pct':round(net_pct,6),'net_profit':round(net,8),'buy_ask':ask,'sell_bid':bid,
                'buy_l1_qty':ask_qty,'sell_l1_qty':bid_qty,'buy_fee_pct':buy_fee*100,'sell_fee_pct':sell_fee*100,
                'signature':f'{buy_venue}:{buy_symbol}:{quote}->{base}>{sell_venue}:{sell_symbol}:{base}->{quote}',
                'steps':[{'index':1,'venue':buy_venue,'from_asset':quote,'to_asset':base,'symbol':buy_symbol,'side':'buy','fee_rate':buy_fee,'capacity':ask*ask_qty,'best_ask':ask,'l1_qty':ask_qty},
                         {'index':2,'venue':sell_venue,'from_asset':base,'to_asset':quote,'symbol':sell_symbol,'side':'sell','fee_rate':sell_fee,'capacity':bid_qty,'best_bid':bid,'l1_qty':bid_qty}]}
    async def _strict(self,row,amount):
        notional=max(10.0,min(float(amount),float(row.get('quote_in') or amount))); legs=live_executor.plan(row,'CLASSIC',notional)
        if len(legs)!=2:return {'passed':False,'reason':'PLAN_INVALID'}
        hot_book_cache.watch_legs(legs,priority=255,ttl=90); await order_rules_registry.prewarm(legs)
        ready=await hot_book_cache.wait_for_legs(legs,timeout=.8,max_age=hot_book_cache.execution_stale_after)
        if not ready:return {'passed':False,'reason':'WS_NOT_READY','ages':[hot_book_cache.age_info(x['venue'],x['symbol']) for x in legs]}
        first=depth_guard.assess_execution_cached_legs(legs)
        if not first.get('ok'):return {'passed':False,'reason':first.get('status'),'first':first}
        first=depth_guard.sustainable(row,'CLASSIC',notional,first)
        if not first.get('ok'):return {'passed':False,'reason':first.get('status'),'first':first}
        pf=order_rules_registry.preflight_cached(first.get('legs') or legs)
        if not pf.get('ok'):return {'passed':False,'reason':'ORDER_RULE_FAILED','errors':pf.get('errors')}
        checked=pf.get('legs') or legs; rev=hot_book_cache.revisions(checked)
        refreshed=await hot_book_cache.wait_for_newer(checked,rev,timeout=.25,max_age=hot_book_cache.execution_stale_after)
        second=depth_guard.assess_execution_cached_legs(checked)
        if not second.get('ok'):return {'passed':False,'reason':'RECHECK_'+str(second.get('status')),'refresh_observed':refreshed}
        second=depth_guard.sustainable(row,'CLASSIC',notional,second); passed=bool(second.get('ok') and float(second.get('sustainable_net_pct') or -999)>0)
        return {'passed':passed,'reason':'STRICT_PASS' if passed else second.get('status'),'notional':notional,'first_net_pct':first.get('sustainable_net_pct'),
                'second_net_pct':second.get('sustainable_net_pct'),'profit_estimate':round(notional*float(second.get('sustainable_net_pct') or 0)/100,8),
                'book_age_ms':second.get('book_age_ms'),'book_ages':second.get('book_ages') or [],'book_source':second.get('book_source'),'refresh_observed':refreshed,'rules_ok':True}
    async def run(self,amount=25.0):
        if self.running:
            snap=self.snapshot(); snap['busy']=True; return snap
        self.running=True; self.last_started_at=time.time(); self.next_run_at=None; self.last_error=None
        try:
            result=await self._run_once(amount)
            self.scan_count+=1; self.last_completed_at=time.time()
            if self.history:
                self.history[-1]['scan_id']=self.scan_count; self._save_history()
            result['scan_id']=self.scan_count; result['history']=self.history[-30:]; result['auto']=self.auto_status(); self._append_log(result); self.last=result
            return result
        except Exception as exc:
            self.last_error=f'{type(exc).__name__}: {str(exc)[:180]}'; raise
        finally:
            self.running=False
    async def _run_once(self,amount=25.0):
        amount=max(10.0,min(1000.0,float(amount))); started=time.perf_counter(); cw=CoinWPublicConnector(); t=time.perf_counter()
        try: cw_edges=await cw.get_edges(amount); cw_health={'venue':'CoinW','ok':True,'edges':len(cw_edges),'latency_ms':round((time.perf_counter()-t)*1000,1)}
        except Exception as exc: cw_edges=[]; cw_health={'venue':'CoinW','ok':False,'edges':0,'latency_ms':round((time.perf_counter()-t)*1000,1),'error':type(exc).__name__+': '+str(exc)[:180]}
        pairs={}
        for e in cw_edges:
            m=e.meta or {}; b=m.get('base'); q=m.get('quote'); sym=m.get('symbol')
            if b and q and sym:pairs[(b,q)]=sym
        watch=[]
        for (b,q),sym in pairs.items(): watch.extend([{'venue':'CoinW','symbol':sym},{'venue':'Bitget','symbol':sym.replace('_','')}])
        hot_book_cache.watch_legs(watch,priority=255,ttl=120)
        if watch:
            target=min(12,len(pairs)); end=time.monotonic()+7.0
            while time.monotonic()<end:
                n=sum(1 for (base,quote),cw_sym in pairs.items() if hot_book_cache.get('CoinW',cw_sym,hot_book_cache.execution_stale_after) and hot_book_cache.get('Bitget',cw_sym.replace('_',''),hot_book_cache.execution_stale_after))
                if n>=target: break
                await asyncio.sleep(.1)
        rows=[]; ready_pairs=0; cw_fee=.001; bg_fee=.001
        for (base,quote),cw_sym in pairs.items():
            bg_sym=cw_sym.replace('_',''); cwh=hot_book_cache.get('CoinW',cw_sym,hot_book_cache.execution_stale_after); bgh=hot_book_cache.get('Bitget',bg_sym,hot_book_cache.execution_stale_after)
            if not cwh or not bgh:continue
            ready_pairs+=1; cw_bids,cw_asks=cwh[0],cwh[1]; bg_bids,bg_asks=bgh[0],bgh[1]
            a=self._row(base,quote,'CoinW','Bitget',cw_sym,bg_sym,cw_asks[0][0],cw_asks[0][1],bg_bids[0][0],bg_bids[0][1],cw_fee,bg_fee,amount)
            b=self._row(base,quote,'Bitget','CoinW',bg_sym,cw_sym,bg_asks[0][0],bg_asks[0][1],cw_bids[0][0],cw_bids[0][1],bg_fee,cw_fee,amount)
            if a:rows.append(a)
            if b:rows.append(b)
        rows.sort(key=lambda x:(x['net_pct'],x['net_profit']),reverse=True); positive=[x for x in rows if x['net_pct']>0]; eligible=[x for x in rows if x['net_pct']>=0.20][:8]; audits=[]
        for row in eligible: audits.append({'base':row['base'],'quote':row['quote'],'buy_venue':row['buy_venue'],'sell_venue':row['sell_venue'],'signature':row['signature'],'quote_in':row['quote_in'],'market_net_pct':row['net_pct'],'market_profit':row['net_profit'],**(await self._strict(row,amount))})
        creds=readiness(); auth={v:{'configured':bool(creds.get(v,{}).get('ready')),'verified':bool(creds.get(v,{}).get('verified'))} for v in ('Bitget','CoinW')}
        top=[{k:x.get(k) for k in ('base','quote','buy_venue','sell_venue','quote_in','net_pct','net_profit','buy_ask','sell_bid','signature')} for x in rows[:20]]
        for x in top:x['market_net_pct']=x.pop('net_pct');x['market_profit']=x.pop('net_profit')
        best=float((top[0] if top else {}).get('market_net_pct') or -999.0)
        strict_count=sum(1 for x in audits if x.get('passed'))
        sample={'ts':time.time(),'amount':amount,'ws_ready_pairs':ready_pairs,'candidate_count':len(rows),'best_market_net_pct':best if top else None,
                'market_positive_count':len(positive),'market_gt_1pct':len(eligible),'strict_pass_count':strict_count,'execution_rest_hits':depth_guard.execution_rest_hits}
        self.history.append(sample); self.history=self.history[-50:]; self._save_history()
        result={'ok':True,'environment':'DEMO_AUDIT','real_orders':False,'real_money':False,'amount':amount,'threshold_pct':1.0,
                'fee_model':{'Bitget':'0.10% taker conservative assumption','CoinW':'0.10% base taker','safety_buffer_pct':depth_guard.safety_buffer_pct},
                'profit_scope':'trade fees + live WS depth + 0.10% safety buffer; blockchain rebalance cost NOT included','rebalance_included':False,
                'auth':auth,'health':[cw_health,{'venue':'Bitget','ok':ready_pairs>0,'ws_ready_pairs':ready_pairs,'latency_ms':None}],
                'pair_universe':len(pairs),'ws_ready_pairs':ready_pairs,'candidate_count':len(rows),'market_positive_count':len(positive),'best_positive_profit':round(float((positive[0] if positive else {}).get('net_profit') or 0),8),'market_gt_1pct':len(eligible),'strict_pass_count':strict_count,
                'audits':audits,'top_market':top,'duration_ms':round((time.perf_counter()-started)*1000,1),'execution_rest_hits':depth_guard.execution_rest_hits,
                'history':self.history[-30:]}
        self.last=result; return result
coinw_bitget_audit=CoinWBitgetAudit()

