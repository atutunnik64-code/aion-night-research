from __future__ import annotations
import time
from app.services.hot_book_cache import hot_book_cache
from app.services.risk_manager import risk_manager
from app.services.depth_guard import depth_guard
from app.services.live_executor import live_executor

class HotSignalEngine:
    def __init__(self):
        self.classic_templates={}; self.latency_templates={}
        self.classic_refreshed_at=0.0; self.latency_refreshed_at=0.0
        self.classic_ttl=300.0; self.latency_ttl=12.0
        self.scans=0; self.classic_signals=0; self.latency_signals=0
        self.last_scan_ms=0.0; self.last_scan_at=0.0; self.last_signals=[]; self.executions=0
        self.previewed=0; self.preview_viable=0; self.preview_rejected=0; self.prefiltered=0
        self.preview_reasons={}; self.last_preview_reasons={}; self.prefilter_reasons={}
        self.preview_fresh_target_ms=500.0; self.required_headroom_ms=300.0

    @staticmethod
    def _sig(row): return str(row.get('signature') or '')

    def refresh_classic(self,rows):
        self.classic_templates={self._sig(x):dict(x) for x in (rows or []) if self._sig(x)}
        self.classic_refreshed_at=time.time(); return len(self.classic_templates)

    def refresh_latency(self,rows):
        self.latency_templates={self._sig(x):dict(x) for x in (rows or []) if self._sig(x)}
        self.latency_refreshed_at=time.time(); return len(self.latency_templates)

    @staticmethod
    def _book(venue,symbol):
        hit=hot_book_cache.get(venue,symbol,hot_book_cache.execution_stale_after)
        if not hit:return None
        bids,asks,age=hit
        if not bids or not asks:return None
        bids=[(float(p),float(q)) for p,q in bids]; asks=[(float(p),float(q)) for p,q in asks]
        return {'bids':bids,'asks':asks,'bid':bids[0][0],'bid_qty':bids[0][1],
                'bid_qty_total':sum(q for _,q in bids),'ask':asks[0][0],'ask_qty':asks[0][1],
                'ask_qty_total':sum(q for _,q in asks),'age_ms':float(age)}

    def _classic(self,row):
        steps=row.get('steps') or []
        buy=next((x for x in steps if x.get('side')=='buy'),None)
        sell=next((x for x in steps if x.get('side')=='sell'),None)
        if not buy or not sell:return None
        bb=self._book(buy.get('venue'),buy.get('symbol')); sb=self._book(sell.get('venue'),sell.get('symbol'))
        if not bb or not sb:return None
        bf=float(buy.get('fee_rate') or 0); sf=float(sell.get('fee_rate') or 0)
        ask=bb['ask']; bid=sb['bid']; gross=(bid/ask-1)*100.0
        net=(bid*(1-sf)/(ask*(1+bf))-1)*100.0
        z=dict(row); z['buy_ask']=ask; z['sell_bid']=bid; z['gross_pct']=gross; z['net_pct']=net
        cap=min(bb['ask_qty_total']*ask,sb['bid_qty_total']*ask,1000.0)
        q=min(float(z.get('quote_in') or cap or 10.0),max(cap,0.0),1000.0)
        z['quote_in']=q; z['base_bought']=q/max(ask,1e-12)*(1-bf); z['quote_out']=z['base_bought']*bid*(1-sf)
        z['net_profit']=z['quote_out']-q; z['signal_capacity_quote']=cap
        z['steps']=[dict(x) for x in steps]
        for x in z['steps']:
            if x.get('side')=='buy':x['best_ask']=ask; x['l1_qty']=bb['ask_qty']
            elif x.get('side')=='sell':x['best_bid']=bid; x['l1_qty']=sb['bid_qty']
        z['signal_source']='WS_DIRECT'; z['signal_book_age_ms']=max(bb['age_ms'],sb['age_ms'])
        z['_ws_signal_detected_mono']=time.monotonic(); return z

    def _latency(self,row):
        slow=self._book(row.get('slow_venue'),row.get('slow_symbol') or f"{row.get('base','')}{row.get('quote','')}")
        fast=self._book(row.get('fast_venue'),row.get('fast_symbol') or f"{row.get('base','')}{row.get('quote','')}")
        if not slow or not fast:return None
        sf=float(row.get('slow_fee_pct') or 0)/100.0; ff=float(row.get('fast_fee_pct') or 0)/100.0
        buy_slow=fast['bid']*(1-ff)/(slow['ask']*(1+sf))-1
        buy_fast=slow['bid']*(1-sf)/(fast['ask']*(1+ff))-1
        z=dict(row); z.update({'slow_bid':slow['bid'],'slow_ask':slow['ask'],'fast_bid':fast['bid'],'fast_ask':fast['ask']})
        if buy_slow>=buy_fast:
            z['direction']='BUY_SLOW_SELL_FAST'; z['net_pct']=buy_slow*100.0
            qty=min(slow['ask_qty_total'],fast['bid_qty_total']); buy_px=slow['ask']
        else:
            z['direction']='SELL_SLOW_BUY_FAST'; z['net_pct']=buy_fast*100.0
            qty=min(slow['bid_qty_total'],fast['ask_qty_total']); buy_px=fast['ask']
        z['base_qty']=qty; z['signal_capacity_quote']=min(qty*buy_px,1000.0)
        z['net_profit']=z['signal_capacity_quote']*z['net_pct']/100.0
        z['signal_source']='WS_DIRECT'; z['signal_book_age_ms']=max(slow['age_ms'],fast['age_ms'])
        z['_ws_signal_detected_mono']=time.monotonic(); return z

    @staticmethod
    def _amounts(row):
        cap=max(0.0,min(1000.0,float(row.get('signal_capacity_quote') or row.get('quote_in') or 0)))
        base=max(0.0,float(row.get('quote_in') or 0))
        values=[cap,cap*.75,cap*.5,cap*.35,250,175,125,100,75,50,35,25,20,15,12.5,10,base]
        return sorted({round(float(x),8) for x in values if 10<=float(x)<=cap+1e-9},reverse=True)

    def _preview(self,row,mode):
        amounts=self._amounts(row)
        if not amounts:return {'best':None,'reasons':{'NOTIONAL_BELOW_10':1}}
        first=live_executor.plan(row,mode,amounts[0])
        if len(first)!=2:return {'best':None,'reasons':{'PLAN_INVALID':1}}
        hits=[]
        for leg in first:
            hit=hot_book_cache.get(leg.get('venue'),leg.get('symbol'),hot_book_cache.execution_stale_after)
            if not hit:return {'best':None,'reasons':{'WS_CACHE_MISS':1}}
            hits.append(hit)
        books=[(x[0],x[1]) for x in hits]; age=max(float(x[2]) for x in hits)
        best=None; reasons={}
        for amount in amounts:
            legs=live_executor.plan(row,mode,amount)
            depth=depth_guard._assess_with_books(legs,books)
            if not depth.get('ok'):
                st=str(depth.get('status') or 'DEPTH_REJECTED'); reasons[st]=reasons.get(st,0)+1; continue
            depth=depth_guard.sustainable(row,mode,amount,depth)
            if not depth.get('ok'):
                st=str(depth.get('status') or 'SUSTAINABLE_REJECTED'); reasons[st]=reasons.get(st,0)+1; continue
            net=float(depth.get('sustainable_net_pct') or 0); profit=amount*net/100.0
            if best is None or (profit,net,amount)>(best[0],best[1],best[2]):best=(profit,net,amount,depth)
        return {'best':best,'reasons':reasons,'book_age_ms':age}

    def _depth_rank(self,rows,mode,limit):
        rows=sorted(rows,key=lambda x:(float(x.get('net_pct') or 0),float(x.get('aion_score') or 0)),reverse=True)[:48]
        viable=[]; last_reasons={}; now=time.time()
        score_floor=max(float(risk_manager.min_score),float(risk_manager.min_score_by_mode.get(mode,risk_manager.min_score)))
        cooldown=max(0.0,float(risk_manager.cooldown_by_mode.get(mode,risk_manager.cooldown)))
        for row in rows:
            sig=self._sig(row); raw_score=row.get('aion_score')
            pre=None
            if raw_score is not None and float(raw_score)<score_floor: pre='SCORE_LOW'
            elif sig and now-float(risk_manager.last_exec.get(sig,0))<cooldown: pre='COOLDOWN'
            elif mode=='LATENCY':
                lag=float(row.get('observed_lag_ms') or 0); transport=float(row.get('slow_transport_ms') or 0)+float(row.get('fast_transport_ms') or 0)
                if lag < max(1000.0,transport*1.25): pre='LATENCY_TOO_SHORT_FOR_EXECUTION'
            if pre:
                self.prefiltered+=1; self.prefilter_reasons[pre]=self.prefilter_reasons.get(pre,0)+1; continue
            self.previewed+=1; p=self._preview(row,mode); best=p.get('best'); reasons=p.get('reasons') or {}
            for k,v in reasons.items():
                last_reasons[k]=last_reasons.get(k,0)+v
                self.preview_reasons[k]=self.preview_reasons.get(k,0)+v
            if not best:
                self.preview_rejected+=1; continue
            profit,net,notional,depth=best; age=float(p.get('book_age_ms') or 999999)
            headroom=max(0.0,hot_book_cache.execution_stale_after*1000.0-age)
            if headroom < self.required_headroom_ms:
                self.preview_rejected+=1
                last_reasons['FRESHNESS_HEADROOM_LOW']=last_reasons.get('FRESHNESS_HEADROOM_LOW',0)+1
                self.preview_reasons['FRESHNESS_HEADROOM_LOW']=self.preview_reasons.get('FRESHNESS_HEADROOM_LOW',0)+1
                continue
            z=dict(row)
            z['signal_best_notional']=notional; z['signal_sustainable_net_pct']=net
            z['signal_expected_profit']=profit; z['signal_preview_book_age_ms']=age
            z['signal_freshness_headroom_ms']=max(0.0,hot_book_cache.execution_stale_after*1000.0-age)
            z['signal_preview_fresh']=bool(age<=self.preview_fresh_target_ms)
            z['signal_preview_status']='SUSTAINABLE_NET_CONFIRMED'; z['signal_depth']=depth
            try:
                boost=live_executor.plan(z,mode,notional)
                if len(boost)==2: hot_book_cache.watch_legs(boost,priority=240,ttl=30)
            except Exception: pass
            viable.append(z); self.preview_viable+=1
        self.last_preview_reasons[mode]=last_reasons
        viable.sort(key=lambda x:(bool(x.get('signal_preview_fresh')),float(x.get('signal_expected_profit') or 0),float(x.get('signal_sustainable_net_pct') or 0),float(x.get('aion_score') or 0)),reverse=True)
        return viable[:limit]

    def scan(self,limit=12):
        started=time.perf_counter(); floor=float(risk_manager.min_net_pct)+float(risk_manager.execution_buffer_pct)
        raw_classic=[]; raw_latency=[]; now=time.time()
        classic_rows=self.classic_templates.values() if now-self.classic_refreshed_at<=self.classic_ttl else []
        latency_rows=self.latency_templates.values() if now-self.latency_refreshed_at<=self.latency_ttl else []
        for row in classic_rows:
            z=self._classic(row)
            if z and z.get('repeatable') and float(z.get('net_pct') or 0)>=floor:raw_classic.append(z)
        for row in latency_rows:
            z=self._latency(row)
            if z and z.get('repeatable') and float(z.get('net_pct') or 0)>=floor:raw_latency.append(z)
        classic=self._depth_rank(raw_classic,'CLASSIC',limit); latency=self._depth_rank(raw_latency,'LATENCY',limit)
        self.scans+=1; self.classic_signals+=len(classic); self.latency_signals+=len(latency)
        self.last_scan_at=time.time(); self.last_scan_ms=(time.perf_counter()-started)*1000.0
        def brief(mode,x):
            return {'mode':mode,'signature':x.get('signature'),'net_pct':round(float(x.get('net_pct') or 0),6),
                    'sustainable_net_pct':round(float(x.get('signal_sustainable_net_pct') or 0),6),
                    'notional':round(float(x.get('signal_best_notional') or 0),4),
                    'expected_profit':round(float(x.get('signal_expected_profit') or 0),6),
                    'age_ms':round(float(x.get('signal_book_age_ms') or 0),1)}
        self.last_signals=[brief('CLASSIC',x) for x in classic[:5]]+[brief('LATENCY',x) for x in latency[:5]]
        return {'classic':classic,'latency':latency}

    def status(self):
        now=time.time()
        return {'templates':{'CLASSIC':len(self.classic_templates),'LATENCY':len(self.latency_templates)},
                'template_age_s':{'CLASSIC':round(now-self.classic_refreshed_at,2) if self.classic_refreshed_at else None,
                                  'LATENCY':round(now-self.latency_refreshed_at,2) if self.latency_refreshed_at else None},
                'template_ttl_s':{'CLASSIC':self.classic_ttl,'LATENCY':self.latency_ttl},'scans':self.scans,
                'signals':{'CLASSIC':self.classic_signals,'LATENCY':self.latency_signals},'executions':self.executions,
                'last_scan_ms':round(self.last_scan_ms,3),'last_scan_at':self.last_scan_at,'last_signals':self.last_signals,
                'depth_preview':{'previewed':self.previewed,'viable':self.preview_viable,'rejected':self.preview_rejected,
                                 'prefiltered':self.prefiltered,'prefilter_reasons':self.prefilter_reasons,
                                 'fresh_target_ms':self.preview_fresh_target_ms,'required_headroom_ms':self.required_headroom_ms,'reasons':self.preview_reasons,'last_reasons':self.last_preview_reasons}}

hot_signal_engine=HotSignalEngine()
