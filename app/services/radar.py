from __future__ import annotations
import asyncio
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from collections import defaultdict
from app.connectors.mock import MockConnector
from app.connectors.cex_public import LIVE_CONNECTOR_CLASSES, START_ASSETS, CYCLE_ASSETS
from app.services.route_finder import RouteFinder
from app.services.window_hunter import window_hunter
from app.services.prefunded_cycles import PrefundedCycleFinder
from app.services.network_registry import network_registry
from app.services.opportunity_memory import memory
from app.services.latency_arb import latency_arb
from app.services.risk_manager import risk_manager
from app.services.cpu_workers import optimized_cycles_worker, prefunded_find_worker, prefunded_confirm_worker


class RadarService:
    def __init__(self):
        self.demo_connectors = [MockConnector()]
        self.live_connectors = [cls() for cls in LIVE_CONNECTOR_CLASSES]
        self.finder = RouteFinder(max_hops=5, min_profit_pct=0.20)
        self.pref_cycle_finder = PrefundedCycleFinder(max_hops=3, min_profit_pct=0.20)
        self.last_scan = None
        self.scan_count = 0
        self.hot_candidates = []
        self.classic_hot_candidates = []
        self.hot_amount = 10000.0
        strict={'Binance','KuCoin','Bitget','HTX','BitMart','CoinEx','Gate.io','CoinW'}
        self.latency_connectors=[c for c in self.live_connectors if c.name in strict]
        self.latency_lock=asyncio.Lock()
        self.last_latency_result=None
        self.cpu_pool=ProcessPoolExecutor(max_workers=2)

    async def _run_cpu(self, fn, *args):
        loop=asyncio.get_running_loop()
        return await loop.run_in_executor(self.cpu_pool, fn, *args)

    def close_cpu_pool(self):
        self.cpu_pool.shutdown(wait=False, cancel_futures=True)

    async def _load_connector(self, connector, amount):
        started = datetime.now(timezone.utc)
        try:
            edges = await connector.get_edges(amount)
            ms = int((datetime.now(timezone.utc)-started).total_seconds()*1000)
            pairs = len(edges)//2
            return edges, {'name':connector.name,'status':'LIVE','edges':len(edges),'pairs':pairs,'latency_ms':ms,'error':None}
        except Exception as exc:
            ms = int((datetime.now(timezone.utc)-started).total_seconds()*1000)
            return [], {'name':connector.name,'status':'ERROR','edges':0,'pairs':0,'latency_ms':ms,'error':str(exc)[:300]}

    @staticmethod
    def _amount_grid(max_amount: float) -> list[float]:
        seeds = [25000,10000,5000,2500,1000,500,250,100,50,25,10]
        values = [float(max_amount)] + [float(x) for x in seeds if x < max_amount]
        return sorted(set(v for v in values if v > 0), reverse=True)
    def _optimized_cycles(self, edges, max_amount: float):
        edges = [e for e in edges if e.src.asset in CYCLE_ASSETS and e.dst.asset in CYCLE_ASSETS]
        chosen = {}
        for amount in self._amount_grid(max_amount):
            for route in self.finder.find_cycles(edges, amount, START_ASSETS):
                sig = '>'.join(route['nodes'])
                if sig not in chosen:
                    route['optimized_amount'] = amount
                    chosen[sig] = route
        return sorted(chosen.values(), key=lambda x: (x['profit_amount'], x['profit_pct']), reverse=True)

    def _prefunded_spreads(self, edges, requested_quote_amount):
        groups = defaultdict(lambda: {'buys':[], 'sells':[]})
        for e in edges:
            if e.kind != 'trade':
                continue
            m = e.meta or {}
            pair = (m.get('base'), m.get('quote'))
            if not all(pair):
                continue
            if m.get('side') == 'buy': groups[pair]['buys'].append(e)
            elif m.get('side') == 'sell': groups[pair]['sells'].append(e)

        rows = []
        for (base, quote), group in groups.items():
            if quote not in START_ASSETS:
                continue
            for buy in group['buys']:
                bm = buy.meta or {}
                for sell in group['sells']:
                    if buy.src.venue == sell.src.venue:
                        continue
                    sm = sell.meta or {}
                    quote_in = min(float(requested_quote_amount), float(buy.capacity))
                    base_out = buy.apply(quote_in)
                    if base_out <= 0:
                        continue
                    if base_out > sell.capacity:
                        quote_in *= float(sell.capacity) / base_out
                        base_out = buy.apply(quote_in)
                    if quote_in < 10:
                        continue
                    quote_out = sell.apply(base_out)
                    if quote_out <= 0:
                        continue
                    buy_ask = float(bm.get('best_ask', 1 / buy.rate))
                    sell_bid = float(sm.get('best_bid', sell.rate))
                    gross_pct = (sell_bid / buy_ask - 1) * 100
                    net_pct = (quote_out / quote_in - 1) * 100
                    rows.append({
                        'base':base,'quote':quote,
                        'buy_venue':buy.src.venue,'sell_venue':sell.src.venue,
                        'quote_in':round(quote_in,8),'base_bought':round(base_out,8),
                        'quote_out':round(quote_out,8),'gross_pct':round(gross_pct,5),
                        'net_pct':round(net_pct,5),'net_profit':round(quote_out-quote_in,8),
                        'buy_ask':buy_ask,'sell_bid':sell_bid,
                        'buy_symbol':bm.get('symbol'),'sell_symbol':sm.get('symbol'),
                        'buy_l1_qty':bm.get('l1_qty'),'sell_l1_qty':sm.get('l1_qty'),
                        'buy_fee_pct':round(buy.fee_rate*100,5),'sell_fee_pct':round(sell.fee_rate*100,5),
                        'requires_prefunded':True,'rebalance_included':False,
                        'liquidity_verified':bool(bm.get('liquidity_verified') and sm.get('liquidity_verified')),
                        'signature':f"{buy.src.venue}:{bm.get('symbol')}:{quote}->{base}>{sell.src.venue}:{sm.get('symbol')}:{base}->{quote}",
                        'steps':[{'index':1,'venue':buy.src.venue,'from_asset':quote,'to_asset':base,'symbol':bm.get('symbol'),'side':'buy','fee_rate':buy.fee_rate,'capacity':buy.capacity,'best_ask':buy_ask,'l1_qty':bm.get('l1_qty')},
                                 {'index':2,'venue':sell.src.venue,'from_asset':base,'to_asset':quote,'symbol':sm.get('symbol'),'side':'sell','fee_rate':sell.fee_rate,'capacity':sell.capacity,'best_bid':sell_bid,'l1_qty':sm.get('l1_qty')}],
                    })
        return sorted(rows, key=lambda x: (x['net_pct'], x['net_profit']), reverse=True)

    async def _validate_classic_spreads(self, rows):
        if not rows:return []
        await network_registry.refresh()
        assets=[x.get('base') for x in rows if 'CoinEx' in (x.get('buy_venue'),x.get('sell_venue'))]
        await network_registry.ensure_coinex_assets(assets)
        out=[]
        for row in rows:
            z=dict(row); base=z.get('base'); quote=z.get('quote'); qty=float(z.get('base_bought') or 0); qamt=float(z.get('quote_in') or 0)
            br=network_registry.best_transfer(z.get('buy_venue'),z.get('sell_venue'),base,qty)
            qr=network_registry.best_transfer(z.get('sell_venue'),z.get('buy_venue'),quote,qamt)
            z['base_rebalance']=br; z['quote_rebalance']=qr; z['repeatable']=bool(br.get('ok') and qr.get('ok'))
            z['classic_ready']=z['repeatable']; z['asset_identity_status']=br.get('identity') if br.get('ok') else 'UNVERIFIED'
            z['transfer_status']='OPEN_COMMON_NETWORKS' if z['repeatable'] else 'NETWORK_OR_IDENTITY_PENDING'
            out.append(z)
        return out

    @staticmethod
    def _cycle_nodes(row):
        steps=row.get('steps') or []
        if not steps:return row.get('assets') or []
        out=[f"{steps[0].get('venue')}:{steps[0].get('from_asset')}"]
        prev_venue=steps[0].get('venue')
        for step in steps:
            venue=step.get('venue'); src=step.get('from_asset'); dst=step.get('to_asset')
            if venue!=prev_venue: out.append(f"{venue}:{src}")
            out.append(f"{venue}:{dst}"); prev_venue=venue
        return out

    @staticmethod
    def _batch_plans(row, base_fee_quote, quote_fee, optimized_start, market_pct, base_amount):
        plans=[]
        fixed=base_fee_quote+quote_fee
        for n in (1,2,3,5,10,20,50):
            market_profit=optimized_start*market_pct/100.0
            net_per_cycle=market_profit-fixed/n
            plans.append({
                'cycles':n,'net_per_cycle':round(net_per_cycle,8),
                'net_pct_per_cycle':round((net_per_cycle/optimized_start*100) if optimized_start else -999,5),
                'series_profit':round(market_profit*n-fixed,8),
                'rebalance_cost_total':round(fixed,8),
                'rebalance_cost_per_cycle':round(fixed/n,8),
                'required_buy_quote_inventory':round(optimized_start*n,8),
                'required_sell_base_inventory':round(base_amount*n,8),
            })
        return plans

    async def _validate_full_loops(self, rows):
        await network_registry.refresh()
        coinex_assets=[]
        for row in rows:
            for st in row.get('steps') or []:
                if st.get('venue')=='CoinEx':
                    coinex_assets.extend([st.get('from_asset'),st.get('to_asset')])
        await network_registry.ensure_coinex_assets(coinex_assets)
        out=[]
        for row in rows:
            z=dict(row); steps=z.get('steps') or []; z['full_loop_confirmed']=False
            if len(steps)!=2:
                z['loop_status']='NETWORK_VALIDATION_REQUIRES_2_HOP'; out.append(z); continue
            a,b=steps[0],steps[1]
            if a.get('venue')==b.get('venue') or a.get('from_asset')!=b.get('to_asset') or a.get('to_asset')!=b.get('from_asset'):
                z['loop_status']='NOT_SIMPLE_CROSS_VENUE_LOOP'; out.append(z); continue
            start_asset=z.get('start_asset') or a.get('from_asset'); base=a.get('to_asset')
            original_start=float(z.get('start_amount') or 0); buy_cap=float(a.get('capacity') or 0)
            ask=float(a.get('best_ask') or (1/float(a.get('rate') or 1))); buy_fee=float(a.get('fee_rate') or 0)
            buy_mult=float(a.get('rate') or 0)*(1-buy_fee)
            base_amount=buy_mult*original_start
            base_route=network_registry.best_transfer(a.get('venue'),b.get('venue'),base,base_amount)
            quote_route=network_registry.best_transfer(b.get('venue'),a.get('venue'),start_asset,original_start)
            if not base_route.get('ok') or not quote_route.get('ok'):
                z['base_rebalance']=base_route; z['quote_rebalance']=quote_route; z['loop_status']='NETWORK_OR_IDENTITY_PENDING'; out.append(z); continue

            optimized_start=original_start
            for _ in range(3):
                base_fee=float(base_route.get('fee') or 0)
                base_fee_quote=base_fee*ask/max(1e-9,1-buy_fee)
                optimized_start=min(original_start,max(0.0,buy_cap-base_fee_quote))
                if optimized_start<=0: break
                base_amount=buy_mult*optimized_start
                nr=network_registry.best_transfer(a.get('venue'),b.get('venue'),base,base_amount+base_fee)
                qr=network_registry.best_transfer(b.get('venue'),a.get('venue'),start_asset,optimized_start+base_fee_quote)
                if not nr.get('ok') or not qr.get('ok'):
                    base_route,quote_route=nr,qr; break
                base_route,quote_route=nr,qr
            z['base_rebalance']=base_route; z['quote_rebalance']=quote_route
            if not base_route.get('ok') or not quote_route.get('ok') or optimized_start<=0:
                z['loop_status']='NETWORK_OR_IDENTITY_PENDING'; out.append(z); continue

            base_fee_quote=float(base_route.get('fee') or 0)*ask/max(1e-9,1-buy_fee)
            quote_fee=float(quote_route.get('fee') or 0)
            market_pct=float(z.get('confirmed_profit_pct',z.get('profit_pct',0)) or 0)
            market_profit=optimized_start*market_pct/100.0
            z['batch_plans']=self._batch_plans(z,base_fee_quote,quote_fee,optimized_start,market_pct,base_amount)
            z['batch_ready_plan']=next((q for q in z['batch_plans'] if q['net_per_cycle']>0),None)
            z['series_ready']=bool(z['batch_ready_plan'])
            full_profit=market_profit-base_fee_quote-quote_fee
            full_pct=(full_profit/optimized_start*100) if optimized_start else -999
            z['original_start_amount']=round(original_start,8); z['start_amount']=round(optimized_start,8)
            z['final_amount']=round(optimized_start+market_profit,8); z['profit_amount']=round(market_profit,8)
            z['rebalance_cost_quote']=round(base_fee_quote+quote_fee,8)
            z['required_buy_quote']=round(optimized_start+base_fee_quote,8); z['buy_l1_capacity_quote']=round(buy_cap,8)
            z['full_profit_amount']=round(full_profit,8); z['full_net_pct']=round(full_pct,5)
            z['asset_identity_status']=base_route.get('identity'); z['transfer_status']='OPEN_COMMON_NETWORKS'
            if full_profit>0:
                z['full_loop_confirmed']=True; z['loop_status']='FULL_LOOP_CONFIRMED'
            else: z['loop_status']='REBALANCE_NEGATIVE'
            out.append(z)
        return out

    @staticmethod
    def _window_cycles(full_loops,series_loops):
        rows=[]
        for x in full_loops:
            z=dict(x); z['profit_pct']=float(x.get('full_net_pct',0)); z['profit_amount']=float(x.get('full_profit_amount',0)); z['window_mode']='FULL_NOW'; rows.append(z)
        for x in series_loops:
            plan=x.get('batch_ready_plan') or {}; z=dict(x); z['profit_pct']=float(plan.get('net_pct_per_cycle',0)); z['profit_amount']=float(plan.get('net_per_cycle',0)); z['window_mode']='BATCH_READY'; rows.append(z)
        return rows

    async def scan(self, amount: float = 10000.0, source: str = 'live'):
        connectors = self.demo_connectors if source == 'demo' else self.live_connectors
        loaded = await asyncio.gather(*[self._load_connector(c, amount) for c in connectors])
        edges = [edge for group, _ in loaded for edge in group]
        health = [h for _, h in loaded]
        routes = self.finder.find_cycles(edges, amount) if source == 'demo' else await self._run_cpu(optimized_cycles_worker, edges, amount, tuple(CYCLE_ASSETS), tuple(START_ASSETS), self.finder.max_hops, self.finder.min_profit_pct)
        prefunded = await asyncio.to_thread(self._prefunded_spreads, edges, amount) if source == 'live' else []
        first_pass = [x for x in prefunded if x['net_pct'] >= 0.20]
        cycle_first = await self._run_cpu(prefunded_find_worker, edges, amount, ('USDT','USDC','FDUSD'), self.pref_cycle_finder.max_hops, self.pref_cycle_finder.min_profit_pct) if source == 'live' else []
        confirmed = []; confirmed_cycles = []
        if source == 'live' and (first_pass or cycle_first):
            await asyncio.sleep(0.45)
            loaded2 = await asyncio.gather(*[self._load_connector(c, amount) for c in connectors])
            edges2 = [edge for group, _ in loaded2 for edge in group]
            second = await asyncio.to_thread(self._prefunded_spreads, edges2, amount)
            second_map = {(x['base'],x['quote'],x['buy_venue'],x['sell_venue']):x for x in second}
            for x in first_pass:
                key=(x['base'],x['quote'],x['buy_venue'],x['sell_venue']); y=second_map.get(key)
                if y and y['net_pct'] > 0:
                    z=dict(y); z['first_net_pct']=x['net_pct']; z['confirmed_net_pct']=round(min(x['net_pct'],y['net_pct']),5); z['confirmation_passes']=2; z['l1_confirmed']=True; z['asset_identity_status']='SYMBOL_ONLY_UNVERIFIED'; z['transfer_status']='UNKNOWN'; confirmed.append(z)
            cycle_second=await self._run_cpu(prefunded_confirm_worker, cycle_first, edges2, amount, self.pref_cycle_finder.max_hops, self.pref_cycle_finder.min_profit_pct)
            cycle_map={x['signature']:x for x in cycle_second}
            for x in cycle_first:
                y=cycle_map.get(x['signature'])
                if y and y['profit_pct'] > 0:
                    z=dict(y); z['first_profit_pct']=x['profit_pct']; z['confirmed_profit_pct']=round(min(x['profit_pct'],y['profit_pct']),5); z['confirmation_passes']=2; z['l1_confirmed']=True; z['asset_identity_status']='SYMBOL_ONLY_UNVERIFIED'; z['transfer_status']='UNKNOWN'; z['loop_status']='MARKET_CLOSED_REBALANCE_PENDING'; z['nodes']=self._cycle_nodes(z); confirmed_cycles.append(z)
        confirmed.sort(key=lambda x:(x['net_profit'],x['confirmed_net_pct']), reverse=True)
        confirmed_cycles.sort(key=lambda x:(x['profit_amount'],x['confirmed_profit_pct']), reverse=True)
        classic_validated=await self._validate_classic_spreads(prefunded[:250]) if source=='live' else []
        classic_watch=[x for x in classic_validated if x.get('repeatable') and float(x.get('net_pct') or 0)>=0.20]
        classic_watch=memory.record(classic_watch,'CLASSIC') if source=='live' else classic_watch
        classic_floor=float(risk_manager.min_net_by_mode.get('CLASSIC',risk_manager.min_net_pct))
        classic=[x for x in classic_watch if float(x.get('net_pct') or 0)>=classic_floor]
        classic.sort(key=lambda x:(x.get('aion_score',0),x.get('net_profit',0)),reverse=True)
        self.classic_hot_candidates=sorted(classic_watch,key=lambda x:(float(x.get('net_pct') or 0),x.get('aion_score',0)),reverse=True)[:100]
        validated_cycles = await self._validate_full_loops(confirmed_cycles) if source == 'live' else confirmed_cycles
        full_loops = [x for x in validated_cycles if x.get('full_loop_confirmed')]
        series_loops = [x for x in validated_cycles if (not x.get('full_loop_confirmed')) and x.get('series_ready')]
        if source == 'live':
            full_loops = memory.record(full_loops, 'FULL_NOW')
            series_loops = memory.record(series_loops, 'BATCH_READY')
            full_floor=float(risk_manager.min_net_by_mode.get('FULL_NOW',risk_manager.min_net_pct))
            batch_floor=float(risk_manager.min_net_by_mode.get('BATCH_READY',risk_manager.min_net_pct))
            full_loops=[x for x in full_loops if float(x.get('full_net_pct') or 0)>=full_floor]
            series_loops=[x for x in series_loops if float((x.get('batch_ready_plan') or {}).get('net_pct_per_cycle') or 0)>=batch_floor]
        full_loops.sort(key=lambda x:(x.get('aion_score',0),x.get('full_profit_amount',0),x.get('full_net_pct',0)), reverse=True)
        series_loops.sort(key=lambda x:(x.get('aion_score',0),(x.get('batch_ready_plan') or {}).get('series_profit',0)), reverse=True)
        pending_loops = [x for x in validated_cycles if not x.get('full_loop_confirmed') and not x.get('series_ready')]
        self.hot_candidates = validated_cycles[:300]
        self.hot_amount = amount
        windows = window_hunter.update(self._window_cycles(full_loops,series_loops), []) if source == 'live' else {'active':[],'counts':{'flash':0,'confirmed':0,'stable':0},'recently_closed':[]}

        self.scan_count += 1
        self.last_scan = datetime.now(timezone.utc).isoformat()
        venues = sorted({e.src.venue for e in edges} | {e.dst.venue for e in edges})
        assets = sorted({e.src.asset for e in edges} | {e.dst.asset for e in edges})
        return {
            'mode':'paper','source_mode':source,'amount':amount,'scan_count':self.scan_count,
            'last_scan':self.last_scan,'edge_count':len(edges),'venue_count':len(venues),
            'asset_count':len(assets),'venues':venues,'assets':assets,'source_health':health,
            'opportunities':routes,
            'prefunded_candidates':prefunded[:100],
            'prefunded_gross_positive':[x for x in prefunded if x['gross_pct'] > 0][:100],
            'prefunded_positive_first_pass':first_pass[:100],
            'prefunded_first_pass_count':len(first_pass),
            'prefunded_opportunities':confirmed[:100],
            'prefunded_confirmed_count':len(confirmed),
            'classic_opportunities':classic[:100] if source=='live' else [],
            'classic_count':len(classic) if source=='live' else 0,
            'classic_watch_candidates':self.classic_hot_candidates[:100] if source=='live' else [],
            'arbitrage_cycle_candidates':cycle_first[:150],
            'arbitrage_cycles':validated_cycles[:150],
            'arbitrage_cycle_count':len(validated_cycles),
            'full_loop_opportunities':full_loops[:150],
            'full_loop_count':len(full_loops),
            'series_ready_opportunities':series_loops[:150],
            'series_ready_count':len(series_loops),
            'network_pending_loops':pending_loops[:150],
            'network_pending_count':len(pending_loops),
            'network_registry':network_registry.summary(),
            'windows':windows,
            'fee_note':'LIVE CEX: публичный L1 bid/ask; объём ограничен размером лучшей цены, поэтому дополнительный slippage не начисляется. Комиссия берётся из публичной пары, если доступна, иначе используется 0.10% taker assumption. Подтверждённые prefunded-связки должны быть NET-положительными в двух последовательных L1-снимках. Rebalance запасов, идентичность токена по контракту и доступность перевода пока вынесены в отдельные статусы.',
            'amount_optimizer':self._amount_grid(amount),
        }


    async def hot_scan(self):
        if not self.hot_candidates:
            return {'ready':False,'reason':'NO_DISCOVERY_CACHE','full_loop_opportunities':[],'series_ready_opportunities':[]}
        venues={v for x in self.hot_candidates for v in (x.get('venues') or [])}
        connectors=[c for c in self.live_connectors if c.name in venues]
        loaded=await asyncio.gather(*[self._load_connector(c,self.hot_amount) for c in connectors])
        edges=[e for group,_ in loaded for e in group]
        health=[h for _,h in loaded]
        current=await self._run_cpu(prefunded_confirm_worker,self.hot_candidates,edges,self.hot_amount,self.pref_cycle_finder.max_hops,self.pref_cycle_finder.min_profit_pct)
        for x in current:
            x['confirmed_profit_pct']=x.get('profit_pct',0); x['l1_confirmed']=True
        validated=await self._validate_full_loops(current)
        full=[x for x in validated if x.get('full_loop_confirmed')]
        series=[x for x in validated if (not x.get('full_loop_confirmed')) and x.get('series_ready')]
        full=memory.record(full,'FULL_NOW'); series=memory.record(series,'BATCH_READY')
        full_floor=float(risk_manager.min_net_by_mode.get('FULL_NOW',risk_manager.min_net_pct))
        batch_floor=float(risk_manager.min_net_by_mode.get('BATCH_READY',risk_manager.min_net_pct))
        full=[x for x in full if float(x.get('full_net_pct') or 0)>=full_floor]
        series=[x for x in series if float((x.get('batch_ready_plan') or {}).get('net_pct_per_cycle') or 0)>=batch_floor]
        full.sort(key=lambda x:(x.get('aion_score',0),x.get('full_profit_amount',0)),reverse=True)
        series.sort(key=lambda x:(x.get('aion_score',0),(x.get('batch_ready_plan') or {}).get('series_profit',0)),reverse=True)
        windows=window_hunter.update(self._window_cycles(full,series), [])
        return {'ready':True,'timestamp':datetime.now(timezone.utc).isoformat(),'full_loop_opportunities':full[:100],
                'full_loop_count':len(full),'series_ready_opportunities':series[:100],'series_ready_count':len(series),
                'checked_candidates':len(self.hot_candidates),'source_health':health,'windows':windows}


    async def latency_scan(self, amount: float = 10000.0):
        if self.latency_lock.locked() and self.last_latency_result:
            return {**self.last_latency_result,'busy':True}
        async with self.latency_lock:
            loaded=await asyncio.gather(*[self._load_connector(c,amount) for c in self.latency_connectors])
            edges=[e for group,_ in loaded for e in group]; health=[h for _,h in loaded]
            raw=latency_arb.observe(edges,health,amount); verified=await latency_arb.verify(raw[:250])
            ready_all=[x for x in verified if x.get('latency_ready')]
            ready_all=memory.record(ready_all,'LATENCY')
            latency_floor=float(risk_manager.min_net_by_mode.get('LATENCY',risk_manager.min_net_pct))
            ready=[x for x in ready_all if float(x.get('net_pct') or 0)>=latency_floor]
            repeatable=[x for x in ready if x.get('repeatable')]
            candidates=[x for x in verified if (not x.get('latency_ready')) or float(x.get('net_pct') or 0)<latency_floor]
            ready.sort(key=lambda x:(x.get('aion_score',0),x.get('net_profit',0)),reverse=True)
            result={'ready':True,'busy':False,'timestamp':datetime.now(timezone.utc).isoformat(),'amount':amount,
                    'sample_count':latency_arb.sample_count,'latency_opportunities':ready[:100],
                    'latency_ready_count':len(ready),'latency_repeatable_count':len(repeatable),
                    'latency_candidates':candidates[:100],'latency_candidate_count':len(candidates),
                    'speed_table':latency_arb.speed_table(),'source_health':health,'edge_count':len(edges),
                    'latency_venues':[c.name for c in self.latency_connectors]}
            self.last_latency_result=result; return result


radar = RadarService()
