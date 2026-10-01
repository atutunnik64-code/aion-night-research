from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.perp_funding_spread import perp_funding_spread_scanner,TAKER_FEE

ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'perp_funding_spread_paper.json'

class PerpFundingSpreadPaper:
    def __init__(self):
        self.enabled=True; self.interval=60.0; self.task=None; self.last_error=None; self.last_refresh=None
        self.research_book_limit=128; self.max_per_base=1; self.notional_per_leg=25.0; self.max_hold_hours=72.0; self.stop_usdt=1.0
        self.min_conservative_edge_pct=0.0
        self.state=self._load(); self.latest={}
    def _load(self):
        try:return json.loads(STATE.read_text(encoding='utf-8-sig'))
        except Exception:return {'starting_equity':100.0,'closed_pnl':0.0,'positions':[],'closed':[]}
    def _save(self):
        STATE.parent.mkdir(parents=True,exist_ok=True)
        tmp=STATE.with_suffix(STATE.suffix+'.tmp')
        tmp.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
        tmp.replace(STATE)
    @staticmethod
    def _future(ts,interval_h,now_ms):
        try:t=float(ts or 0)
        except Exception:t=0.0
        step=max(1.0,float(interval_h))*3600000.0
        if t<=0:return now_ms+step
        while t<=now_ms:t+=step
        return t
    def _market(self,p):
        base=p['base']; mm=perp_funding_spread_scanner.market.get(base) or {}
        return mm.get(p['long_venue']),mm.get(p['short_venue'])
    def _mark(self,p,longm,shortm):
        qty=float(p['qty']); lf=float(p['long_fee']); sf=float(p['short_fee'])
        long_pnl=qty*(float(longm['bid'])-float(p['entry_long_ask']))
        short_pnl=qty*(float(p['entry_short_bid'])-float(shortm['ask']))
        exit_fee=qty*(float(longm['bid'])*lf+float(shortm['ask'])*sf)
        pnl=long_pnl+short_pnl-float(p['entry_fee'])-exit_fee+float(p.get('funding_realized') or 0)
        return pnl,exit_fee,long_pnl,short_pnl
    def _funding_cross(self,p,longm,shortm,now_ms):
        realized=0.0; events=0; qty=float(p['qty'])
        for side,m in (('long',longm),('short',shortm)):
            key='next_'+side+'_funding'; rate_key='pending_'+side+'_rate'; int_key=side+'_interval_hours'
            nxt=float(p.get(key) or 0); interval=max(1.0,float(p.get(int_key) or 8)); rate=float(p.get(rate_key) or 0)
            while nxt>0 and now_ms>=nxt:
                mark=(float(m['bid'])+float(m['ask']))/2.0; notional=qty*mark
                realized+=((-rate if side=='long' else rate)*notional); events+=1; nxt+=interval*3600000.0
            p[key]=nxt; p[rate_key]=float(m.get('rate') or 0)
        if events:
            p['funding_realized']=float(p.get('funding_realized') or 0)+realized
            p['funding_events']=int(p.get('funding_events') or 0)+events
        return realized,events
    def _open(self,row,now):
        la=float(row['long_ask']); sb=float(row['short_bid']); qty=min(self.notional_per_leg/la,self.notional_per_leg/sb)
        lf=float(TAKER_FEE[row['long_venue']]); sf=float(TAKER_FEE[row['short_venue']]); fee=qty*(la*lf+sb*sf)
        now_ms=now*1000.0
        return {'signature':row['signature'],'base':row['base'],'long_venue':row['long_venue'],'short_venue':row['short_venue'],
                'opened_at':now,'qty':qty,'entry_long_ask':la,'entry_short_bid':sb,'long_fee':lf,'short_fee':sf,'entry_fee':fee,
                'capital_required_usdt':2.0*self.notional_per_leg,
                'long_interval_hours':row['long_interval_hours'],'short_interval_hours':row['short_interval_hours'],
                'next_long_funding':self._future(row.get('long_next_funding_time'),row['long_interval_hours'],now_ms),
                'next_short_funding':self._future(row.get('short_next_funding_time'),row['short_interval_hours'],now_ms),
                'pending_long_rate':float(row['long_funding_rate_pct'])/100.0,'pending_short_rate':float(row['short_funding_rate_pct'])/100.0,
                'funding_realized':0.0,'funding_events':0,'entry_edge_24h_pct':row['net_24h_conservative_pct'],
                'basis_review':bool(row.get('basis_review')),'status':'OPEN'}
    async def refresh(self):
        try:
            if not perp_funding_spread_scanner.market:await perp_funding_spread_scanner.refresh()
            now=time.time(); now_ms=now*1000.0; positions=list(self.state.get('positions') or []); closed=list(self.state.get('closed') or [])
            still=[]
            for p in positions:
                longm,shortm=self._market(p)
                if not longm or not shortm:still.append(p); continue
                self._funding_cross(p,longm,shortm,now_ms)
                pnl,exit_fee,_,_=self._mark(p,longm,shortm); age_h=(now-float(p['opened_at']))/3600.0
                carry_hour=float(shortm['rate'])*100/max(float(shortm['interval_hours']),1e-9)-float(longm['rate'])*100/max(float(longm['interval_hours']),1e-9)
                close_reason=None
                if pnl<=-self.stop_usdt:close_reason='PAIR_STOP'
                elif age_h>=self.max_hold_hours:close_reason='MAX_HOLD'
                elif int(p.get('funding_events') or 0)>=1 and carry_hour<=0 and pnl>0:close_reason='CARRY_FLIPPED_PROFIT'
                elif int(p.get('funding_events') or 0)>=2 and pnl>=0.20:close_reason='FUNDING_TARGET'
                if close_reason:
                    p.update({'status':'CLOSED','closed_at':now,'close_reason':close_reason,'realized_pnl':pnl,'exit_fee':exit_fee})
                    closed.append(p); self.state['closed_pnl']=float(self.state.get('closed_pnl') or 0)+pnl
                else:still.append(p)
            self.state['positions']=still; self.state['closed']=closed[-2000:]

            existing_sig={p['signature'] for p in still}; active_bases={p['base'] for p in still}
            candidates=[x for x in perp_funding_spread_scanner.rows if x.get('status')=='PAPER_CANDIDATE' and x['signature'] not in existing_sig]
            eligible_unique=[]; seen=set(active_bases)
            for row in candidates:
                if row['base'] in seen:continue
                if float(row.get('net_24h_conservative_pct') or 0)<=self.min_conservative_edge_pct:continue
                eligible_unique.append(row);seen.add(row['base'])
            slots=max(0,self.research_book_limit-len(self.state['positions']))
            for row in eligible_unique[:slots]:
                self.state['positions'].append(self._open(row,now))
            overflow=max(0,len(eligible_unique)-slots)
            self._save()

            open_marks=[]; open_pnl=0.0
            for p in self.state['positions']:
                longm,shortm=self._market(p)
                if not longm or not shortm:continue
                pnl,_,_,_=self._mark(p,longm,shortm); open_pnl+=pnl
                open_marks.append({'signature':p['signature'],'base':p['base'],'pnl':round(pnl,6),'funding':round(float(p.get('funding_realized') or 0),6),
                                   'events':int(p.get('funding_events') or 0),'age_hours':round((now-float(p['opened_at']))/3600.0,2),
                                   'entry_edge_24h_pct':p.get('entry_edge_24h_pct'),'basis_review':bool(p.get('basis_review'))})
            closed_pnl=float(self.state.get('closed_pnl') or 0)
            open_capital=len(self.state['positions'])*2.0*self.notional_per_leg
            closed_capital=len(self.state.get('closed') or [])*2.0*self.notional_per_leg
            open_return=(open_pnl/open_capital*100.0) if open_capital>0 else 0.0
            closed_return=(closed_pnl/closed_capital*100.0) if closed_capital>0 else 0.0
            self.latest={'mode':'PAPER_RESEARCH_BOOK','closed_pnl':round(closed_pnl,6),'open_mark_pnl':round(open_pnl,6),
                         'research_book_delta_usdt':round(closed_pnl+open_pnl,6),'open_count':len(self.state['positions']),
                         'closed_count':len(self.state.get('closed') or []),'candidate_count':sum(1 for x in perp_funding_spread_scanner.rows if x.get('status')=='PAPER_CANDIDATE'),
                         'eligible_unique_new_candidates':len(eligible_unique),'overflow_candidates':overflow,'research_book_limit':self.research_book_limit,
                         'open_capital_required_usdt':round(open_capital,2),'closed_turnover_capital_usdt':round(closed_capital,2),
                         'open_mark_return_on_open_capital_pct':round(open_return,4),'closed_return_on_turnover_capital_pct':round(closed_return,4),
                         'max_per_base':self.max_per_base,'allocatable':False,'deployable':False,
                         'reason':'WIDE_PAPER_RESEARCH_BOOK_NOT_A_100_USDT_PORTFOLIO','open_positions':open_marks[-200:],
                         'recent_closed':(self.state.get('closed') or [])[-50:]}
            self.last_refresh=now; self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:500]
        return self.status()
    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_RESEARCH_BOOK','interval_seconds':self.interval,
                'last_refresh':self.last_refresh,'last_error':self.last_error,'latest':self.latest,
                'policy':{'wide_research_book':True,'positive_conservative_edges_not_discarded':True,'capital_usage_reported_separately':True,
                          'diversified_bases':True,'no_grid':True,'no_martingale':True,'no_dca':True,'no_live_orders':True}}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='perp-funding-spread-paper')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        await asyncio.sleep(35)
        while True:
            await self.refresh(); await asyncio.sleep(max(45.0,self.interval))

perp_funding_spread_paper=PerpFundingSpreadPaper()
