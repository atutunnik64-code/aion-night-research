from __future__ import annotations
from app.http_shared import SHARED_SSL_CONTEXT
import os,asyncio,time,uuid
from pathlib import Path
from app.services.private_trading import ADAPTERS,readiness
from app.services.order_rules import order_rules_registry
from app.services.depth_guard import depth_guard
from app.services.inventory_guard import inventory_guard
from app.services.hot_book_cache import hot_book_cache

GATE=Path(__file__).parents[2]/'data'/'LIVE_TRADING_ENABLED'
STRICT_EXECUTION_MODES={'FULL_NOW','BATCH_READY','CLASSIC','LATENCY'}

class LiveExecutor:
    def __init__(self):
        self.kill_switch=False; self.last_error=None; self.last_result=None
        self.min_locked_profit_quote=max(0.0,float(os.getenv('AION_MIN_LOCKED_PROFIT_QUOTE','0.01')))
        self.min_locked_net_pct=max(0.0,float(os.getenv('AION_MIN_LOCKED_NET_PCT','0.05')))
        self.fee_guard_extra_rate=max(0.0,float(os.getenv('AION_FEE_GUARD_EXTRA_RATE','0.0005')))
        self.rebalance_guard_mult=max(1.0,float(os.getenv('AION_REBALANCE_GUARD_MULT','1.25')))
        self.live_quarantined_venues={x.strip() for x in os.getenv('AION_LIVE_QUARANTINED_VENUES','HTX').split(',') if x.strip()}
    def armed(self):
        env=os.getenv('AION_LIVE_TRADING','0')=='1'
        file_ok=GATE.exists() and GATE.read_text(encoding='utf-8').strip().upper()=='YES'
        creds=readiness(); verified=[k for k,v in creds.items() if v.get('ready') and v.get('fill_verification') and v.get('live_execution_ready',True) and v.get('verified')]
        return bool(env and file_ok and len(verified)>=2 and not self.kill_switch)
    def status(self):
        creds=readiness(); verified=[k for k,v in creds.items() if v.get('ready') and v.get('fill_verification') and v.get('live_execution_ready',True) and v.get('verified')]
        env_ok=os.getenv('AION_LIVE_TRADING','0')=='1'; file_ok=GATE.exists() and GATE.read_text(encoding='utf-8').strip().upper()=='YES'
        return {'armed':self.armed(),'kill_switch':self.kill_switch,'credentials':creds,'verified_ready':verified,'verified_ready_count':len(verified),
                'environment_gate':env_ok,'file_gate':file_ok,'last_error':self.last_error,'last_result':self.last_result,
                'gate_file':str(GATE),'requires_trade_permission_only':True,'withdraw_permission_required':False,'inventory_guard':inventory_guard.status(),
                'live_quarantined_venues':sorted(self.live_quarantined_venues),
                'profit_lock':{'enabled':True,'min_profit_quote':self.min_locked_profit_quote,'min_net_pct':self.min_locked_net_pct,'fee_guard_extra_rate':self.fee_guard_extra_rate,'rebalance_guard_mult':self.rebalance_guard_mult,'condition':'BOTH_LEGS_FULL_WITHIN_IOC_LIMITS','atomic_cross_exchange':False},
                'hot_path':{'FULL_NOW':'WS_CACHE_ONLY','BATCH_READY':'WS_CACHE_ONLY','CLASSIC':'WS_CACHE_ONLY','LATENCY':'WS_CACHE_ONLY','max_book_age_ms':hot_book_cache.execution_stale_after*1000,'rest_fallback':False,'order_rules':'PREWARMED_CACHE_ONLY'}}
    def set_file_gate(self,enabled,confirmation=''):
        if enabled:
            if confirmation!='ENABLE LIVE TRADING':return {'ok':False,'status':'CONFIRMATION_REQUIRED',**self.status()}
            if self.status().get('verified_ready_count',0)<2:return {'ok':False,'status':'NEED_TWO_VERIFIED_EXCHANGES',**self.status()}
            GATE.parent.mkdir(parents=True,exist_ok=True); GATE.write_text('YES',encoding='utf-8')
        elif GATE.exists():GATE.unlink()
        return {'ok':True,'status':'GATE_UPDATED',**self.status()}

    @staticmethod
    def _leg(venue,symbol,side,qty,price,base=None,quote=None,fee_rate=0.0):
        qty=max(0.0,float(qty)); price=max(0.0,float(price)); side=side.lower()
        return {'venue':venue,'symbol':symbol,'side':side,'qty':qty,'price':price,'base':base,'quote':quote,
                'input_asset':quote if side=='buy' else base,'input_required':qty*price if side=='buy' else qty,'fee_rate':float(fee_rate or 0)}

    def plan(self,row,mode,notional):
        if mode=='LATENCY':
            qty=min(float(row.get('base_qty') or 0),notional/max(1e-12,float(row.get('slow_ask') or row.get('fast_ask') or 1)))
            if row.get('direction')=='BUY_SLOW_SELL_FAST':
                return [self._leg(row['slow_venue'],row.get('slow_symbol') or f"{row['base']}{row['quote']}",'buy',qty,row['slow_ask'],row['base'],row['quote'],float(row.get('slow_fee_pct') or 0)/100.0),
                        self._leg(row['fast_venue'],row.get('fast_symbol') or f"{row['base']}{row['quote']}",'sell',qty,row['fast_bid'],row['base'],row['quote'],float(row.get('fast_fee_pct') or 0)/100.0)]
            return [self._leg(row['slow_venue'],row.get('slow_symbol') or f"{row['base']}{row['quote']}",'sell',qty,row['slow_bid'],row['base'],row['quote'],float(row.get('slow_fee_pct') or 0)/100.0),
                    self._leg(row['fast_venue'],row.get('fast_symbol') or f"{row['base']}{row['quote']}",'buy',qty,row['fast_ask'],row['base'],row['quote'],float(row.get('fast_fee_pct') or 0)/100.0)]
        steps=row.get('steps') or []
        if len(steps)!=2:return []
        buy=next((s for s in steps if s.get('side')=='buy'),None); sell=next((s for s in steps if s.get('side')=='sell'),None)
        if not buy or not sell:return []
        ask=float(buy.get('best_ask') or (1/float(buy.get('rate') or 1))); bid=float(sell.get('best_bid') or sell.get('rate') or 0)
        qty=notional/max(ask,1e-12)
        return [self._leg(buy['venue'],buy.get('symbol') or '', 'buy',qty,ask,buy.get('to_asset'),buy.get('from_asset'),float(buy.get('fee_rate') or 0)),self._leg(sell['venue'],sell.get('symbol') or '', 'sell',qty,bid,sell.get('from_asset'),sell.get('to_asset'),float(sell.get('fee_rate') or 0))]


    async def _execution_depth(self,row,mode,legs,notional):
        hot=mode in STRICT_EXECUTION_MODES
        if hot:
            hot_book_cache.watch_legs(legs,priority=190,ttl=300)
            if not all(hot_book_cache.get(x.get('venue'),x.get('symbol'),hot_book_cache.execution_stale_after) for x in legs):
                await hot_book_cache.wait_for_legs(legs,timeout=.08,max_age=hot_book_cache.execution_stale_after)
            depth=depth_guard.assess_execution_cached_legs(legs)
            if not depth.get('ok'):
                raw=str(depth.get('status') or 'WS_CACHE_MISS')
                status='LIVE_SOURCE_BOOK_STALE' if raw=='WS_SOURCE_STALE' else ('LIVE_WS_NOT_READY' if raw.startswith('WS_') else raw)
                return {**depth,'ok':False,'status':status}
        else:
            depth=await depth_guard.assess_legs(legs)
        if not depth.get('ok'):return depth
        return depth_guard.sustainable(row,mode,notional,depth)

    async def _final_presend(self,row,mode,legs,notional):
        if mode not in STRICT_EXECUTION_MODES:return {'ok':True,'legs':legs,'depth':None}
        depth=await self._execution_depth(row,mode,legs,notional)
        if not depth.get('ok'):
            raw=str(depth.get('status') or 'REJECTED'); mapped={'LIVE_SOURCE_BOOK_STALE':'SOURCE_BOOK_STALE','LIVE_WS_NOT_READY':'WS_NOT_READY'}.get(raw,raw)
            return {'ok':False,'status':'PRE_SEND_'+mapped,'depth':depth}
        fresh_legs=depth.get('legs') or legs
        pf=order_rules_registry.preflight_cached(fresh_legs)
        if not pf.get('ok'):return {'ok':False,'status':'PRE_SEND_ORDER_RULE_FAILED','errors':pf.get('errors'),'depth':depth}
        return {'ok':True,'legs':pf.get('legs') or fresh_legs,'depth':depth,'pre_send_book_age_ms':depth.get('book_age_ms'),'pre_send_book_ages':depth.get('book_ages') or []}

    def _profit_lock(self,legs,depth,notional):
        buy=next((x for x in legs or [] if str(x.get('side')).lower()=='buy'),None)
        sell=next((x for x in legs or [] if str(x.get('side')).lower()=='sell'),None)
        if not buy or not sell:
            return {'passed':False,'status':'PROFIT_LOCK_SIDES_INVALID'}
        qty=min(max(0.0,float(buy.get('qty') or 0)),max(0.0,float(sell.get('qty') or 0)))
        if qty<=0:return {'passed':False,'status':'PROFIT_LOCK_QTY_INVALID'}
        bfee=max(0.0,float(buy.get('fee_rate') or 0))+self.fee_guard_extra_rate
        sfee=max(0.0,float(sell.get('fee_rate') or 0))+self.fee_guard_extra_rate
        buy_limit=max(0.0,float(buy.get('price') or 0)); sell_limit=max(0.0,float(sell.get('price') or 0))
        buy_max=qty*buy_limit*(1.0+bfee); sell_min=qty*sell_limit*(1.0-sfee)
        reb_raw=max(0.0,float((depth or {}).get('rebalance_cost_quote') or 0))
        reb_guard=reb_raw*self.rebalance_guard_mult
        reserve=max(0.0,float(notional or 0))*max(0.0,float(depth_guard.safety_buffer_pct))/100.0
        floor=sell_min-buy_max-reb_guard-reserve
        capital=max(buy_max,1e-12); net=floor/capital*100.0
        passed=bool(floor>=self.min_locked_profit_quote and net>=self.min_locked_net_pct)
        return {'passed':passed,'status':'PROFIT_LOCKED' if passed else 'PROFIT_LOCK_TOO_LOW',
                'condition':'BOTH_LEGS_FULL_WITHIN_IOC_LIMITS','atomic_cross_exchange':False,
                'qty':round(qty,12),'buy_limit':buy_limit,'sell_limit':sell_limit,
                'buy_max_cost_quote':round(buy_max,10),'sell_min_proceeds_quote':round(sell_min,10),
                'fee_guard_extra_rate':self.fee_guard_extra_rate,'rebalance_raw_quote':round(reb_raw,10),
                'rebalance_guard_quote':round(reb_guard,10),'execution_reserve_quote':round(reserve,10),
                'locked_min_profit_quote':round(floor,10),'locked_min_net_pct':round(net,6),
                'min_required_profit_quote':self.min_locked_profit_quote,'min_required_net_pct':self.min_locked_net_pct}

    async def _order_state(self,leg,placement,deadline=2.0):
        adapter=ADAPTERS[leg['venue']]; oid=placement.get('order_id'); cid=placement.get('client_id')
        deadline=min(float(deadline),float(getattr(adapter,'ioc_timeout',deadline) or deadline))
        end=time.time()+deadline; last={'ok':False,'terminal':False,'status':'UNKNOWN','filled_qty':0.0,'filled_quote':0.0}
        while time.time()<end:
            try:
                last=await adapter.order_status(leg['symbol'],oid,cid)
                if last.get('terminal'): break
            except Exception as exc:last={'ok':False,'terminal':False,'status':'STATUS_ERROR','error':str(exc)[:240],'filled_qty':0.0,'filled_quote':0.0}
            await asyncio.sleep(.12)
        if not last.get('terminal'):
            try: await adapter.cancel_order(leg['symbol'],oid,cid)
            except Exception: pass
            await asyncio.sleep(.12)
            try:last=await adapter.order_status(leg['symbol'],oid,cid)
            except Exception: pass
        last['requested_qty']=float(leg.get('qty') or 0); return last

    async def _emergency_hedge(self,leg,excess_qty,hot_only=False):
        if excess_qty<=0:return {'ok':True,'status':'NO_HEDGE'}
        adapter=ADAPTERS[leg['venue']]; side='sell' if leg['side']=='buy' else 'buy'
        try:
            if hot_only:
                hot_book_cache.watch_legs([leg],priority=250,ttl=300)
                hit=hot_book_cache.get(leg['venue'],leg['symbol'],hot_book_cache.execution_stale_after)
                if not hit:
                    await hot_book_cache.wait_for_legs([leg],timeout=.08,max_age=hot_book_cache.execution_stale_after)
                    hit=hot_book_cache.get(leg['venue'],leg['symbol'],hot_book_cache.execution_stale_after)
                if not hit:return {'ok':False,'status':'HEDGE_WS_NOT_READY','book_source':'WS_CACHE'}
                bids,asks=hit[0],hit[1]; book_source='WS_CACHE'; book_age_ms=hit[2]
            else:
                import httpx
                async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=3.5,follow_redirects=True) as c:bids,asks=await depth_guard._fetch(c,leg['venue'],leg['symbol'])
                book_source='REST'; book_age_ms=None
            levels=asks if side=='buy' else bids; consumed=depth_guard._consume(levels,excess_qty)
            if not consumed.get('ok'):return {'ok':False,'status':'HEDGE_DEPTH_INSUFFICIENT','book_source':book_source}
            price=consumed['last_price']*(1.003 if side=='buy' else .997)
            hleg=self._leg(leg['venue'],leg['symbol'],side,excess_qty,price,leg.get('base'),leg.get('quote'),leg.get('fee_rate',0))
        except Exception as exc:return {'ok':False,'status':'HEDGE_BOOK_ERROR','error':str(exc)[:240]}
        pf=order_rules_registry.preflight_cached([hleg]) if hot_only else await order_rules_registry.preflight([hleg])
        if not pf.get('ok'):return {'ok':False,'status':'HEDGE_PREFLIGHT_FAILED','errors':pf.get('errors'),'rules_source':pf.get('source')}
        hleg=(pf.get('legs') or [hleg])[0]; cid='aion-hedge-'+uuid.uuid4().hex[:18]
        try:
            placed=await adapter.place_ioc(hleg['symbol'],hleg['side'],hleg['qty'],hleg['price'],cid)
            if not placed.get('ok'):return {'ok':False,'status':'HEDGE_PLACE_FAILED','placement':placed}
            state=await self._order_state(hleg,placed,1.5); ratio=float(state.get('filled_qty') or 0)/max(float(hleg.get('qty') or 0),1e-12)
            return {'ok':ratio>=.995,'status':'HEDGE_FILLED' if ratio>=.995 else 'HEDGE_PARTIAL','leg':hleg,'placement':placed,'state':state,'fill_ratio':ratio,'book_source':book_source,'book_age_ms':book_age_ms}
        except Exception as exc:return {'ok':False,'status':'HEDGE_EXCEPTION','error':str(exc)[:240]}

    @staticmethod
    def _actual_fee_quote(leg,state):
        qty=max(0.0,float((state or {}).get('filled_qty') or 0)); quote=max(0.0,float((state or {}).get('filled_quote') or 0)); px=(quote/qty if qty>0 and quote>0 else max(0.0,float(leg.get('price') or 0)))
        base=str(leg.get('base') or '').upper(); qcur=str(leg.get('quote') or '').upper(); total=0.0; seen=False; reconciled=True
        for z in ((state or {}).get('fee_items') or []):
            try:cur=str(z.get('currency') or '').upper(); amt=float(z.get('amount') or 0)
            except Exception:continue
            if not cur:continue
            seen=True
            if cur==qcur:total+=amt
            elif cur==base:total+=amt*px
            else:reconciled=False
        return total,seen and reconciled
    @classmethod
    def _effective_base_qty(cls,leg,state):
        qty=max(0.0,float((state or {}).get('filled_qty') or 0)); base=str(leg.get('base') or '').upper(); fee_base=0.0
        for z in ((state or {}).get('fee_items') or []):
            if str(z.get('currency') or '').upper()==base:
                try:fee_base+=float(z.get('amount') or 0)
                except Exception:pass
        return max(0.0,qty-fee_base) if str(leg.get('side')).lower()=='buy' else max(0.0,qty+fee_base)
    @classmethod
    def _cashflow(cls,leg,state):
        qty=max(0.0,float((state or {}).get('filled_qty') or 0)); quote=max(0.0,float((state or {}).get('filled_quote') or 0))
        if quote<=0 and qty>0:quote=qty*max(0.0,float(leg.get('price') or 0))
        actual,reconciled=cls._actual_fee_quote(leg,state)
        fee_quote=actual if reconciled else quote*max(0.0,float(leg.get('fee_rate') or 0))
        return (-quote-fee_quote) if str(leg.get('side')).lower()=='buy' else (quote-fee_quote)

    def _realized_pnl(self,legs,states,recovery,rebalance_cost):
        cash=sum(self._cashflow(leg,state) for leg,state in zip(legs or [],states or []))
        hedge_cash=0.0
        if isinstance(recovery,dict) and recovery.get('leg') and recovery.get('state'):
            hedge_cash=self._cashflow(recovery['leg'],recovery['state'])
        return cash+hedge_cash-float(rebalance_cost or 0),cash,hedge_cash

    async def execute(self,row,mode,notional):
        if not self.armed(): return {'ok':False,'status':'LIVE_NOT_ARMED'}
        legs=self.plan(row,mode,notional)
        if len(legs)!=2:return {'ok':False,'status':'PLAN_INVALID'}
        blocked=sorted({x['venue'] for x in legs if x.get('venue') in self.live_quarantined_venues})
        if blocked:return {'ok':False,'status':'VENUE_LIVE_QUARANTINED','venues':blocked}
        unsupported=[x['venue'] for x in legs if x['venue'] not in ADAPTERS or not getattr(ADAPTERS[x['venue']],'fill_verification',False)]
        if unsupported:return {'ok':False,'status':'FILL_VERIFICATION_UNSUPPORTED','venues':unsupported}
        depth=await self._execution_depth(row,mode,legs,notional)
        if not depth.get('ok'):return {'ok':False,'status':depth.get('status'),'depth':depth}
        legs=depth.get('legs') or legs
        preflight=order_rules_registry.preflight_cached(legs) if mode in STRICT_EXECUTION_MODES else await order_rules_registry.preflight(legs)
        if not preflight.get('ok'):return {'ok':False,'status':'ORDER_RULE_PREFLIGHT_FAILED','errors':preflight.get('errors'),'legs':preflight.get('legs')}
        legs=preflight.get('legs') or legs
        missing=[x['venue'] for x in legs if not ADAPTERS[x['venue']].ready()]
        if missing:return {'ok':False,'status':'CREDENTIALS_MISSING','venues':missing}
        venues=sorted({x['venue'] for x in legs}); fetched=await asyncio.gather(*[ADAPTERS[v].balances() for v in venues],return_exceptions=True); balance_map={}
        for venue,data in zip(venues,fetched):
            if isinstance(data,Exception):return {'ok':False,'status':'BALANCE_CHECK_FAILED','venue':venue,'error':str(data)[:250]}
            balance_map[venue]=data
        shortages=[]
        for leg in legs:
            asset=str(leg.get('input_asset') or '').upper(); need=float(leg.get('input_required') or 0)*1.002; free=float(balance_map.get(leg['venue'],{}).get(asset,0))
            if not asset or free<need:shortages.append({'venue':leg['venue'],'asset':asset,'required':need,'free':free})
        if shortages:return {'ok':False,'status':'INSUFFICIENT_PREFUNDED_BALANCE','shortages':shortages}
        inv=inventory_guard.check(balance_map,legs)
        if not inv.get('ok'):return {'ok':False,'status':'INVENTORY_RESERVE_GUARD','inventory':inv}
        presend=await self._final_presend(row,mode,legs,notional)
        if not presend.get('ok'):return presend
        legs=presend.get('legs') or legs
        if presend.get('depth'):depth=presend['depth']
        final_short=[]
        for leg in legs:
            asset=str(leg.get('input_asset') or '').upper(); need=float(leg.get('qty') or 0)*(float(leg.get('price') or 0) if leg.get('side')=='buy' else 1.0)*1.002
            free=float(balance_map.get(leg['venue'],{}).get(asset,0))
            if not asset or free<need:final_short.append({'venue':leg['venue'],'asset':asset,'required':need,'free':free})
        if final_short:return {'ok':False,'status':'PRE_SEND_INSUFFICIENT_PREFUNDED_BALANCE','shortages':final_short,'depth':depth}
        inv=inventory_guard.check(balance_map,legs)
        if not inv.get('ok'):return {'ok':False,'status':'PRE_SEND_INVENTORY_RESERVE_GUARD','inventory':inv,'depth':depth}
        profit_lock=self._profit_lock(legs,depth,notional)
        if not profit_lock.get('passed'):
            return {'ok':False,'status':'PRE_SEND_PROFIT_LOCK_FAILED','profit_lock':profit_lock,'depth':depth}
        depth={**(depth or {}),'profit_lock':profit_lock}
        cid='aion-'+uuid.uuid4().hex[:20]; started=time.time()
        async def one(i,leg):return await ADAPTERS[leg['venue']].place_ioc(leg['symbol'],leg['side'],leg['qty'],leg['price'],f'{cid}-{i}')
        try:
            raw=await asyncio.gather(one(1,legs[0]),one(2,legs[1]),return_exceptions=True); placed=[]
            for r in raw:placed.append({'ok':False,'error':str(r)[:300]} if isinstance(r,Exception) else r)
            accepted=[bool(x.get('ok')) for x in placed]
            if not any(accepted):
                result={'ok':False,'status':'FAILED_BOTH','strategy':mode,'legs':legs,'responses':placed,'depth':depth,'lifecycle':['ATTEMPT','ORDERS_REJECTED'],'duration_ms':round((time.time()-started)*1000,1),'kill_switch':False}; self.last_result=result; return result
            states=await asyncio.gather(*[self._order_state(leg,pl,2.0) if pl.get('ok') else asyncio.sleep(0,result={'ok':False,'terminal':True,'status':'PLACE_FAILED','filled_qty':0.0,'filled_quote':0.0}) for leg,pl in zip(legs,placed)])
            f=[self._effective_base_qty(leg,state) for leg,state in zip(legs,states)]; target=max(float(x.get('qty') or 0) for x in legs); matched=min(f); mismatch=abs(f[0]-f[1]); tol=max(target*.003,1e-10)
            recovery=None; status='FILLED_BOTH' if matched>0 and mismatch<=tol else ('FAILED_BOTH' if max(f)<=1e-12 else 'PARTIAL_FILL_RISK')
            if status=='PARTIAL_FILL_RISK':
                idx=0 if f[0]>f[1] else 1; recovery=await self._emergency_hedge(legs[idx],mismatch,hot_only=mode in STRICT_EXECUTION_MODES); self.kill_switch=True
                self.last_error='Asymmetric fill detected; emergency hedge attempted; automatic trading paused.'
                status='PARTIAL_HEDGED_PAUSED' if recovery.get('ok') else 'PARTIAL_UNHEDGED_KILL'
            pnl,trade_cashflow,hedge_cashflow=self._realized_pnl(legs,states,recovery,depth.get('rebalance_cost_quote') or 0)
            fee_reconciliation=[]
            for leg,state in zip(legs,states):
                fq,fr=self._actual_fee_quote(leg,state); fee_reconciliation.append({'venue':leg.get('venue'),'fee_quote_equiv':round(fq,10),'reconciled':fr,'fee_items':state.get('fee_items') or []})
            fees_all_reconciled=all(x.get('reconciled') for x in fee_reconciliation)
            residual=max(0.0,mismatch-float(((recovery or {}).get('state') or {}).get('filled_qty') or 0)) if recovery else mismatch
            pnl_final=status in {'FILLED_BOTH','PARTIAL_HEDGED_PAUSED'} and residual<=tol
            if not pnl_final:pnl=None
            inventory=inventory_guard.apply_execution(legs,states,recovery)
            result={'ok':status=='FILLED_BOTH','status':status,'strategy':mode,'legs':legs,'responses':placed,'order_states':states,'filled_qty':f,'matched_qty':matched,'fill_mismatch_qty':mismatch,'residual_exposure_qty':residual,'realized_trade_cashflow':trade_cashflow,'hedge_cashflow':hedge_cashflow,'realized_profit_estimate':pnl,'realized_pnl_source':('EXCHANGE_FILLS_ACTUAL_FEES' if fees_all_reconciled else 'EXCHANGE_FILLS_MIXED_FEES'),'fee_reconciliation':fee_reconciliation,'profit_lock':profit_lock,'depth':depth,'depth_verified':True,'fill_verified':True,'recovery':recovery,'inventory':inventory,'next_cycle_eligible':status=='FILLED_BOTH' and not self.kill_switch,'lifecycle':(['ATTEMPT','ORDERS_ACCEPTED','FILLS_VERIFIED']+(['PARTIAL_FILL','HEDGE_FILLED' if (recovery or {}).get('ok') else 'HEDGE_FAILED'] if recovery else [])+['PNL_RECONCILED','INVENTORY_UPDATED',('NEXT_CYCLE_ELIGIBLE' if status=='FILLED_BOTH' and not self.kill_switch else 'AUTO_PAUSED')]),'duration_ms':round((time.time()-started)*1000,1),'kill_switch':self.kill_switch}
            self.last_result=result; return result
        except Exception as exc:
            self.kill_switch=True; self.last_error=str(exc)[:500]; result={'ok':False,'status':'EXECUTION_EXCEPTION','error':self.last_error,'kill_switch':True}; self.last_result=result; return result

    def reset_kill_switch(self):
        self.kill_switch=False; self.last_error=None; return self.status()

live_executor=LiveExecutor()


