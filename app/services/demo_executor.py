from __future__ import annotations
from app.http_shared import SHARED_SSL_CONTEXT
import asyncio,time,uuid
from app.services.demo_trading import DEMO_ADAPTERS,demo_readiness
from app.services.live_executor import live_executor
from app.services.order_rules import order_rules_registry
from app.services.depth_guard import depth_guard
from app.services.inventory_guard import inventory_guard
from app.services.demo_wallet import demo_wallet
from app.services.hot_book_cache import hot_book_cache
from app.services.shadow_impact_guard import shadow_impact_guard

STRICT_EXECUTION_MODES={'FULL_NOW','BATCH_READY','CLASSIC','LATENCY'}

class DemoExecutor:
    def __init__(self):
        self.kill_switch=False
        self.last_error=None
        self.last_result=None
        self.shadow_latency_ms=250.0
    def armed(self):
        return not self.kill_switch
    def status(self):
        creds=demo_readiness(); ready=[k for k,v in creds.items() if v.get('ready')]
        backend='EXCHANGE_DEMO' if len(ready)>=2 else 'SHADOW_LIVE'
        return {'armed':self.armed(),'kill_switch':self.kill_switch,'credentials':creds,
                'ready_venues':ready,'ready_count':len(ready),'supported_venues':list(DEMO_ADAPTERS),
                'backend':backend,'exchange_demo_ready':len(ready)>=2,'shadow_latency_ms':self.shadow_latency_ms,'shadow_confirmation_mode':'EVENT_OR_STRICT_RECHECK',
                'last_error':self.last_error,'last_result':self.last_result,'environment':'DEMO',
                'real_market_books':True,'real_money':False,'withdrawals':False,'wallet':demo_wallet.status(),
                'shadow_impact_guard':shadow_impact_guard.status()}
    def plan(self,row,mode,notional):
        return live_executor.plan(row,mode,notional)
    async def _order_state(self,leg,placement,deadline=2.0):
        adapter=DEMO_ADAPTERS[leg['venue']]
        oid=placement.get('order_id'); cid=placement.get('client_id')
        end=time.time()+deadline
        last={'ok':False,'terminal':False,'status':'UNKNOWN','filled_qty':0.0,'filled_quote':0.0}
        while time.time()<end:
            try:
                last=await adapter.order_status(leg['symbol'],oid,cid)
                if last.get('terminal'):break
            except Exception as exc:
                last={'ok':False,'terminal':False,'status':'STATUS_ERROR','error':str(exc)[:240],
                      'filled_qty':0.0,'filled_quote':0.0}
            await asyncio.sleep(.12)
        if not last.get('terminal'):
            try:await adapter.cancel_order(leg['symbol'],oid,cid)
            except Exception:pass
            await asyncio.sleep(.12)
            try:last=await adapter.order_status(leg['symbol'],oid,cid)
            except Exception:pass
        last['requested_qty']=float(leg.get('qty') or 0)
        return last

    async def _emergency_hedge(self,leg,excess_qty,hot_only=False):
        if excess_qty<=0:return {'ok':True,'status':'NO_HEDGE'}
        adapter=DEMO_ADAPTERS[leg['venue']]
        side='sell' if leg['side']=='buy' else 'buy'
        try:
            if hot_only:
                hot_book_cache.watch_legs([leg],priority=250,ttl=300)
                hit=hot_book_cache.get(leg['venue'],leg['symbol'],hot_book_cache.execution_stale_after)
                if not hit:
                    await hot_book_cache.wait_for_legs([leg],timeout=.08,max_age=hot_book_cache.execution_stale_after)
                    hit=hot_book_cache.get(leg['venue'],leg['symbol'],hot_book_cache.execution_stale_after)
                if not hit:return {'ok':False,'status':'HEDGE_WS_NOT_READY','book_source':'WS_CACHE'}
                bids,asks=hit[0],hit[1]; source='WS_CACHE'; age_ms=hit[2]
            else:
                import httpx
                async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=3.5,follow_redirects=True) as c:
                    bids,asks=await depth_guard._fetch(c,leg['venue'],leg['symbol'])
                source='REST'; age_ms=None
            levels=asks if side=='buy' else bids
            consumed=depth_guard._consume(levels,excess_qty)
            if not consumed.get('ok'):return {'ok':False,'status':'HEDGE_DEPTH_INSUFFICIENT','book_source':source}
            price=consumed['last_price']*(1.003 if side=='buy' else .997)
            hleg=live_executor._leg(leg['venue'],leg['symbol'],side,excess_qty,price,leg.get('base'),leg.get('quote'),leg.get('fee_rate',0))
        except Exception as exc:
            return {'ok':False,'status':'HEDGE_BOOK_ERROR','error':str(exc)[:240]}
        pf=order_rules_registry.preflight_cached([hleg]) if hot_only else await order_rules_registry.preflight([hleg])
        if not pf.get('ok'):return {'ok':False,'status':'HEDGE_PREFLIGHT_FAILED','errors':pf.get('errors'),'rules_source':pf.get('source')}
        hleg=(pf.get('legs') or [hleg])[0]
        cid='aion-demo-hedge-'+uuid.uuid4().hex[:14]
        placed=await adapter.place_ioc(hleg['symbol'],hleg['side'],hleg['qty'],hleg['price'],cid)
        if not placed.get('ok'):return {'ok':False,'status':'HEDGE_PLACE_FAILED','placement':placed}
        state=await self._order_state(hleg,placed,1.5)
        ratio=float(state.get('filled_qty') or 0)/max(float(hleg.get('qty') or 0),1e-12)
        return {'ok':ratio>=.995,'status':'HEDGE_FILLED' if ratio>=.995 else 'HEDGE_PARTIAL',
                'leg':hleg,'placement':placed,'state':state,'fill_ratio':ratio,'book_source':source,'book_age_ms':age_ms}
    async def _execute_exchange(self,row,mode,notional):
        legs=self.plan(row,mode,notional)
        if len(legs)!=2:return {'ok':False,'status':'PLAN_INVALID','environment':'DEMO'}
        venues={x['venue'] for x in legs}
        unsupported=[v for v in venues if v not in DEMO_ADAPTERS]
        if unsupported:return {'ok':False,'status':'DEMO_VENUE_UNSUPPORTED','venues':unsupported,'environment':'DEMO'}
        missing=[v for v in venues if not DEMO_ADAPTERS[v].ready()]
        if missing:return {'ok':False,'status':'DEMO_CREDENTIALS_MISSING','venues':missing,'environment':'DEMO'}
        if self.kill_switch:return {'ok':False,'status':'DEMO_KILL_SWITCH','environment':'DEMO'}
        if mode in STRICT_EXECUTION_MODES:
            hot_book_cache.watch_legs(legs,priority=190,ttl=300)
            if not all(hot_book_cache.get(x.get('venue'),x.get('symbol'),hot_book_cache.execution_stale_after) for x in legs):
                await hot_book_cache.wait_for_legs(legs,timeout=.08,max_age=hot_book_cache.execution_stale_after)
            depth=depth_guard.assess_execution_cached_legs(legs)
            if not depth.get('ok'):return {'ok':False,'status':'DEMO_WS_NOT_READY','depth':depth,'environment':'DEMO'}
        else:
            depth=await depth_guard.assess_legs(legs)
        if not depth.get('ok'):return {'ok':False,'status':depth.get('status'),'depth':depth,'environment':'DEMO'}
        depth=depth_guard.sustainable(row,mode,notional,depth)
        if not depth.get('ok'):return {'ok':False,'status':depth.get('status'),'depth':depth,'environment':'DEMO'}
        legs=depth.get('legs') or legs
        pf=order_rules_registry.preflight_cached(legs) if mode in STRICT_EXECUTION_MODES else await order_rules_registry.preflight(legs)
        if not pf.get('ok'):
            return {'ok':False,'status':'ORDER_RULE_PREFLIGHT_FAILED','errors':pf.get('errors'),
                    'legs':pf.get('legs'),'environment':'DEMO'}
        legs=pf.get('legs') or legs
        venue_list=sorted(venues)
        fetched=await asyncio.gather(*[DEMO_ADAPTERS[v].balances() for v in venue_list],return_exceptions=True)
        balance_map={}
        for venue,data in zip(venue_list,fetched):
            if isinstance(data,Exception):
                return {'ok':False,'status':'DEMO_BALANCE_CHECK_FAILED','venue':venue,
                        'error':str(data)[:250],'environment':'DEMO'}
            balance_map[venue]=data
        shortages=[]
        for leg in legs:
            asset=str(leg.get('input_asset') or '').upper()
            need=float(leg.get('input_required') or 0)*1.002
            free=float(balance_map.get(leg['venue'],{}).get(asset,0))
            if not asset or free<need:
                shortages.append({'venue':leg['venue'],'asset':asset,'required':need,'free':free})
        if shortages:
            return {'ok':False,'status':'DEMO_INSUFFICIENT_PREFUNDED_BALANCE',
                    'shortages':shortages,'environment':'DEMO'}
        inv=inventory_guard.check(balance_map,legs)
        if not inv.get('ok'):
            return {'ok':False,'status':'DEMO_INVENTORY_RESERVE_GUARD','inventory':inv,'environment':'DEMO'}
        presend=await live_executor._final_presend(row,mode,legs,notional)
        if not presend.get('ok'):return {**presend,'environment':'DEMO'}
        legs=presend.get('legs') or legs
        if presend.get('depth'):depth=presend['depth']
        final_short=[]
        for leg in legs:
            asset=str(leg.get('input_asset') or '').upper(); need=float(leg.get('qty') or 0)*(float(leg.get('price') or 0) if leg.get('side')=='buy' else 1.0)*1.002
            free=float(balance_map.get(leg['venue'],{}).get(asset,0))
            if not asset or free<need:final_short.append({'venue':leg['venue'],'asset':asset,'required':need,'free':free})
        if final_short:return {'ok':False,'status':'PRE_SEND_INSUFFICIENT_PREFUNDED_BALANCE','shortages':final_short,'depth':depth,'environment':'DEMO'}
        inv=inventory_guard.check(balance_map,legs)
        if not inv.get('ok'):return {'ok':False,'status':'PRE_SEND_INVENTORY_RESERVE_GUARD','inventory':inv,'depth':depth,'environment':'DEMO'}
        cid='aion-demo-'+uuid.uuid4().hex[:18]
        started=time.time()
        async def one(i,leg):
            return await DEMO_ADAPTERS[leg['venue']].place_ioc(
                leg['symbol'],leg['side'],leg['qty'],leg['price'],f'{cid}-{i}')
        raw=await asyncio.gather(one(1,legs[0]),one(2,legs[1]),return_exceptions=True)
        placed=[({'ok':False,'error':str(r)[:300]}) if isinstance(r,Exception) else r for r in raw]
        if not any(bool(x.get('ok')) for x in placed):
            result={'ok':False,'status':'FAILED_BOTH','strategy':mode,'environment':'DEMO',
                    'legs':legs,'responses':placed,'depth':depth,'kill_switch':False,'lifecycle':['ATTEMPT','ORDERS_REJECTED'],
                    'duration_ms':round((time.time()-started)*1000,1)}
            self.last_result=result; return result
        states=await asyncio.gather(*[
            self._order_state(leg,pl,2.0) if pl.get('ok') else
            asyncio.sleep(0,result={'ok':False,'terminal':True,'status':'PLACE_FAILED','filled_qty':0.0,'filled_quote':0.0})
            for leg,pl in zip(legs,placed)
        ])
        fills=[float(x.get('filled_qty') or 0) for x in states]
        target=max(float(x.get('qty') or 0) for x in legs)
        matched=min(fills); mismatch=abs(fills[0]-fills[1]); tol=max(target*.003,1e-10)
        recovery=None
        status='FILLED_BOTH' if matched>0 and mismatch<=tol else ('FAILED_BOTH' if max(fills)<=1e-12 else 'PARTIAL_FILL_RISK')
        if status=='PARTIAL_FILL_RISK':
            idx=0 if fills[0]>fills[1] else 1
            recovery=await self._emergency_hedge(legs[idx],mismatch,hot_only=mode in STRICT_EXECUTION_MODES)
            self.kill_switch=True
            self.last_error='DEMO asymmetric fill; hedge attempted; demo auto paused.'
            status='PARTIAL_HEDGED_PAUSED' if recovery.get('ok') else 'PARTIAL_UNHEDGED_KILL'
        pnl,trade_cashflow,hedge_cashflow=live_executor._realized_pnl(legs,states,recovery,depth.get('rebalance_cost_quote') or 0)
        residual=max(0.0,mismatch-float(((recovery or {}).get('state') or {}).get('filled_qty') or 0)) if recovery else mismatch
        if not (status in {'FILLED_BOTH','PARTIAL_HEDGED_PAUSED'} and residual<=tol):pnl=None
        inventory=inventory_guard.apply_execution(legs,states,recovery)
        result={'ok':status=='FILLED_BOTH','status':status,'strategy':mode,'environment':'DEMO',
                'legs':legs,'responses':placed,'order_states':states,'filled_qty':fills,
                'matched_qty':matched,'fill_mismatch_qty':mismatch,'residual_exposure_qty':residual,
                'realized_trade_cashflow':trade_cashflow,'hedge_cashflow':hedge_cashflow,
                'realized_profit_estimate':pnl,'realized_pnl_source':'EXCHANGE_DEMO_FILLS','depth':depth,'depth_verified':True,'fill_verified':True,
                'recovery':recovery,'inventory':inventory,'next_cycle_eligible':status=='FILLED_BOTH' and not self.kill_switch,
                'lifecycle':(['ATTEMPT','ORDERS_ACCEPTED','FILLS_VERIFIED']+(['PARTIAL_FILL','HEDGE_FILLED' if (recovery or {}).get('ok') else 'HEDGE_FAILED'] if recovery else [])+['PNL_RECONCILED','INVENTORY_UPDATED',('NEXT_CYCLE_ELIGIBLE' if status=='FILLED_BOTH' and not self.kill_switch else 'AUTO_PAUSED')]),
                'duration_ms':round((time.time()-started)*1000,1),'kill_switch':self.kill_switch}
        self.last_result=result
        return result

    async def _execute_sandbox(self,row,mode,notional):
        backend='SHADOW_LIVE'
        if self.kill_switch:return {'ok':False,'status':'DEMO_KILL_SWITCH','environment':'DEMO','backend':backend}
        legs=self.plan(row,mode,notional)
        if len(legs)!=2:return {'ok':False,'status':'PLAN_INVALID','environment':'DEMO','backend':backend}
        started=time.time()
        if mode in STRICT_EXECUTION_MODES:
            hot_book_cache.watch_legs(legs,priority=160,ttl=300)
            if not all(hot_book_cache.get(x.get('venue'),x.get('symbol'),hot_book_cache.execution_stale_after) for x in legs):
                await hot_book_cache.wait_for_legs(legs,timeout=.08,max_age=hot_book_cache.execution_stale_after)
            impact=shadow_impact_guard.ready(row.get('signature'),legs)
            if not impact.get('ok'):
                return {'ok':False,'status':'SHADOW_BOOK_NOT_REFRESHED','impact_guard':impact,'environment':'DEMO','backend':backend}
        first=depth_guard.assess_execution_cached_legs(legs) if mode in STRICT_EXECUTION_MODES else await depth_guard.assess_legs(legs)
        if not first.get('ok'):
            raw=str(first.get('status') or '')
            status=('SHADOW_SOURCE_BOOK_STALE' if raw=='WS_SOURCE_STALE' else 'SHADOW_WS_NOT_READY') if mode in STRICT_EXECUTION_MODES else first.get('status')
            return {'ok':False,'status':status,'depth':first,'environment':'DEMO','backend':backend}
        if mode in STRICT_EXECUTION_MODES and first.get('book_source')!='WS_CACHE':return {'ok':False,'status':'SHADOW_WS_NOT_READY','depth':first,'environment':'DEMO','backend':backend}
        first=depth_guard.sustainable(row,mode,notional,first)
        if not first.get('ok'):return {'ok':False,'status':first.get('status'),'depth':first,'environment':'DEMO','backend':backend}
        if mode in STRICT_EXECUTION_MODES:
            age=float(first.get('book_age_ms') or 999999); hard_ms=hot_book_cache.execution_stale_after*1000.0
            needed=float(self.shadow_latency_ms)+50.0; headroom=max(0.0,hard_ms-age); freshness_recovered=False
            if headroom < needed:
                check_legs=first.get('legs') or legs
                freshness_recovered=await hot_book_cache.wait_for_fresh_age(check_legs,max_age=max(.05,hot_book_cache.execution_stale_after-needed/1000.0),timeout=.45)
                if freshness_recovered:
                    first=depth_guard.assess_execution_cached_legs(check_legs)
                    if first.get('ok'): first=depth_guard.sustainable(row,mode,notional,first)
                    age=float(first.get('book_age_ms') or 999999); headroom=max(0.0,hard_ms-age)
            if headroom < needed:return {'ok':False,'status':'SHADOW_FRESHNESS_BUDGET_LOW','depth':first,'book_age_ms':age,'headroom_ms':headroom,'required_headroom_ms':needed,'freshness_recovered':freshness_recovered,'environment':'DEMO','backend':backend}
        confirmation_mode='FIXED_DELAY'
        refresh_observed=False
        if mode in STRICT_EXECUTION_MODES:
            check_legs=first.get('legs') or legs
            revisions=hot_book_cache.revisions(check_legs)
            refresh_observed=await hot_book_cache.wait_for_newer(check_legs,revisions,timeout=self.shadow_latency_ms/1000.0,max_age=hot_book_cache.execution_stale_after)
            confirmation_mode='EVENT_DRIVEN_WS_REFRESH' if refresh_observed else 'STRICT_TIME_RECHECK'
        else:
            await asyncio.sleep(self.shadow_latency_ms/1000.0)
        second=depth_guard.assess_execution_cached_legs(first.get('legs') or legs) if mode in STRICT_EXECUTION_MODES else await depth_guard.assess_legs(first.get('legs') or legs)
        if not second.get('ok'):
            raw=str(second.get('status') or 'UNKNOWN'); return {'ok':False,'status':'SHADOW_RECHECK_'+raw,'recheck_reason':raw,'depth_first':first,'depth_second':second,'environment':'DEMO','backend':backend}
        if mode in STRICT_EXECUTION_MODES and second.get('book_source')!='WS_CACHE':return {'ok':False,'status':'SHADOW_WS_NOT_READY','depth_first':first,'depth_second':second,'environment':'DEMO','backend':backend}
        depth=depth_guard.sustainable(row,mode,notional,second)
        if not depth.get('ok'):
            raw=str(depth.get('status') or 'UNKNOWN'); return {'ok':False,'status':'SHADOW_RECHECK_'+raw,'recheck_reason':raw,'depth_first':first,'depth_second':depth,'environment':'DEMO','backend':backend}
        depth['shadow_first_net_pct']=first.get('sustainable_net_pct'); depth['shadow_second_net_pct']=depth.get('sustainable_net_pct'); depth['shadow_latency_ms']=self.shadow_latency_ms; depth['shadow_confirmation_mode']=confirmation_mode; depth['shadow_refresh_observed']=refresh_observed
        legs=depth.get('legs') or legs
        pref=await demo_wallet.ensure_prefunded(legs,inventory_guard.reserve_pct)
        if not pref.get('ok'):return {'ok':False,'status':pref.get('status'),'inventory':pref,'environment':'DEMO','backend':backend}
        wallet=await demo_wallet.settle_cycle(legs,depth=depth); profit=float(wallet.get('cycle_profit') or 0)
        if profit<=0:return {'ok':False,'status':'SHADOW_NET_NONPOSITIVE','depth':depth,'wallet':wallet,'environment':'DEMO','backend':backend}
        inventory_guard.record_balances(wallet.get('balances') or {},'shadow_wallet_snapshot')
        inventory=inventory_guard.status()
        qty=min(float(x.get('qty') or 0) for x in legs)
        states=[{'ok':True,'terminal':True,'status':'SHADOW_FILLED','filled_qty':qty,'filled_quote':float(x.get('depth_value') or qty*float(x.get('price') or 0))} for x in legs]
        impact_mark=shadow_impact_guard.mark(row.get('signature'),legs)
        result={'ok':True,'status':'FILLED_BOTH','strategy':mode,'environment':'DEMO','backend':backend,
                'legs':legs,'responses':[{'ok':True,'shadow':True},{'ok':True,'shadow':True}],
                'order_states':states,'filled_qty':[qty,qty],'matched_qty':qty,'fill_mismatch_qty':0.0,
                'realized_trade_pnl_estimate':float(depth.get('net_profit') or 0),'realized_profit_estimate':profit,
                'realized_pnl_source':'SHADOW_WALLET_SIMULATION','execution_book_age_ms':depth.get('book_age_ms'),
                'depth':depth,'depth_verified':True,'fill_verified':False,'shadow_fill':True,'inventory':inventory,'inventory_source':'shadow_wallet_snapshot','wallet':wallet,
                'impact_guard':impact_mark,'next_cycle_eligible':False,'next_cycle_requires_book_refresh':True,
                'lifecycle':['ATTEMPT','SHADOW_WINDOW_CONFIRMED','FILLS_SIMULATED','PNL_RECONCILED','INVENTORY_UPDATED','SHADOW_IMPACT_MARKED','WAIT_BOOK_REFRESH'],'duration_ms':round((time.time()-started)*1000,1),'kill_switch':False}
        self.last_result=result; return result

    async def execute(self,row,mode,notional):
        legs=self.plan(row,mode,notional)
        venues={x.get('venue') for x in legs}
        exchange_ready=len(legs)==2 and all(v in DEMO_ADAPTERS and DEMO_ADAPTERS[v].ready() for v in venues)
        if exchange_ready:return await self._execute_exchange(row,mode,notional)
        return await self._execute_sandbox(row,mode,notional)

    def reset_kill_switch(self):
        self.kill_switch=False; self.last_error=None
        return self.status()

demo_executor=DemoExecutor()
