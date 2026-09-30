from __future__ import annotations
import asyncio,json,time
from collections import defaultdict
from pathlib import Path

ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
BIN_RAW=DATA/'liquidation_force_orders_v1.jsonl'
BY_RAW=DATA/'crossvenue_bybit_liquidations_v1.jsonl'
BOOK=DATA/'liquidity_migration_perp_snapshots_v1.jsonl'
STATE=DATA/'liquidation_absorption_shadow_v1.json'
SYMS=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT']
WINDOW=60;MIN_NOTIONAL=250000.;MIN_DOM=.70;MIN_OPPOSE_IMB=.25
MIN_DEPTH=500000.;MAX_DISP=3.0;MAX_SPREAD=3.0
HOLD=3600;COOLDOWN=7200;COST=.0012;ALLOC=.05
MIN_HOURS=72.;MIN_RESOLVED=30;MIN_SYMBOLS=6;MIN_PER_PRESSURE=5
STRESS=(2.0,3.0)

def _tail(path,limit=100000):
    if not path.exists():return []
    out=[]
    for line in path.read_text(encoding='utf-8').splitlines()[-limit:]:
        try:out.append(json.loads(line))
        except Exception:pass
    return out
def _pressure(e,venue):
    if venue=='Binance':
        s=str(e.get('side') or '')
        return s if s in {'BUY','SELL'} else ''
    s=str(e.get('pressure_side') or '')
    return s if s in {'BUY','SELL'} else ''

def _aggregate(events,venue,out):
    for e in events:
        sym=str(e.get('symbol') or '')
        if sym not in SYMS:continue
        ts=float(e.get('event_time') or 0)/1000.;side=_pressure(e,venue)
        if ts<=0 or not side:continue
        k=(sym,int(ts//WINDOW));z=out.setdefault(k,{'BUY':0.0,'SELL':0.0})
        z[side]+=float(e.get('notional_usdt') or 0)

def _book_before(rows,sym,ts):
    for row in reversed(rows):
        if float(row.get('ts') or 0)>ts:continue
        return ((row.get('symbols') or {}).get(sym) or {})
    return None

def _book_after(rows,sym,ts):
    for row in rows:
        if float(row.get('ts') or 0)>=ts:
            return row,((row.get('symbols') or {}).get(sym) or {})
    return None,None
def _stress(rows,name,mult):
    eq=100.0
    for x in rows:
        base=float(x.get(name+'_net_return_pct') or 0)/100.0
        cost=float(x.get('round_trip_cost') or COST)
        eq*=1.0+float(x.get('allocation_fraction') or ALLOC)*(base-(float(mult)-1.0)*cost)
    return eq-100.0

def _blank():
    now=time.time();return {'version':'LIQUIDATION_ABSORPTION_V1','rule_frozen_at':now,
        'last_bucket':int(now//WINDOW),'last_signal_ts':{},'pending':[],'resolved':[],
        'absorption_equity':100.0,'absorption_peak':100.0,'absorption_max_dd_pct':0.0,
        'continuation_equity':100.0,'continuation_peak':100.0,'continuation_max_dd_pct':0.0}

class LiquidationAbsorptionShadow:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state=_blank()
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _detect(self,be,ye,books):
        agg={};_aggregate(be,'Binance',agg);_aggregate(ye,'Bybit',agg)
        last=int(self.state.get('last_bucket') or 0);new=0
        for (sym,bucket),z in sorted(agg.items(),key=lambda kv:kv[0][1]):
            if bucket<=last:continue
            total=float(z['BUY']+z['SELL'])
            if total<MIN_NOTIONAL:continue
            pressure='BUY' if z['BUY']>=z['SELL'] else 'SELL';dom=float(z[pressure])/total
            if dom<MIN_DOM:continue
            ts=(bucket+1)*WINDOW;book=_book_before(books,sym,ts)
            if not book:continue
            venues=book.get('venues') or {};bn=venues.get('Binance') or {};by=venues.get('Bybit') or {}
            entry=float(bn.get('mid') or 0);disp=float(book.get('dispersion_bps') or 999)
            depth=float(bn.get('depth_quote') or 0)+float(by.get('depth_quote') or 0)
            if entry<=0 or depth<MIN_DEPTH or disp>MAX_DISP:continue
            if max(float(bn.get('spread_bps') or 999),float(by.get('spread_bps') or 999))>MAX_SPREAD:continue
            w1=float(bn.get('depth_quote') or 0);w2=float(by.get('depth_quote') or 0);den=w1+w2
            imb=((float(bn.get('imbalance') or 0)*w1)+(float(by.get('imbalance') or 0)*w2))/den if den else 0.0
            ps=1.0 if pressure=='BUY' else -1.0
            if ps*imb>-MIN_OPPOSE_IMB:continue
            prev=float((self.state.get('last_signal_ts') or {}).get(sym) or 0)
            if ts-prev<COOLDOWN:continue
            self.state.setdefault('pending',[]).append({'id':f'{sym}:{bucket}:{pressure}','symbol':sym,'ts':ts,
                'due_ts':ts+HOLD,'pressure':pressure,'liquidation_notional':total,'dominance':dom,'book_imbalance':imb,
                'book_depth_quote':depth,'dispersion_bps':disp,'entry':entry,'absorption_side':-ps,'continuation_side':ps,
                'funding_rate':float(bn.get('funding_rate') or 0),'next_funding_time':int(bn.get('next_funding_time') or 0),
                'round_trip_cost':COST,'allocation_fraction':ALLOC})
            self.state.setdefault('last_signal_ts',{})[sym]=ts;new+=1
        if agg:self.state['last_bucket']=max(last,max(k[1] for k in agg))
        return new
    def _resolve(self,books):
        keep=[]
        for p in self.state.get('pending') or []:
            row,book=_book_after(books,p['symbol'],float(p['due_ts']))
            if not row or not book:keep.append(p);continue
            px=float((((book.get('venues') or {}).get('Binance') or {}).get('mid')) or 0)
            if px<=0:keep.append(p);continue
            ret=px/float(p['entry'])-1.0
            crossed=int(p.get('next_funding_time') or 0)>0 and float(p['ts'])*1000<int(p['next_funding_time'])<=float(row.get('ts') or 0)*1000
            out={**p,'exit':px,'exit_ts':float(row.get('ts') or 0),'funding_crossed':crossed}
            for name in ('absorption','continuation'):
                side=float(p[name+'_side']);fund=side*float(p.get('funding_rate') or 0) if crossed else 0.0
                net=side*ret-COST-fund;out[name+'_net_return_pct']=net*100
                eqk=name+'_equity';peakk=name+'_peak';ddk=name+'_max_dd_pct'
                eq=float(self.state.get(eqk) or 100)*(1+ALLOC*net);peak=max(float(self.state.get(peakk) or 100),eq)
                self.state[eqk]=eq;self.state[peakk]=peak;self.state[ddk]=min(float(self.state.get(ddk) or 0),(eq/peak-1)*100)
                out[name+'_paper_equity']=eq
            self.state.setdefault('resolved',[]).append(out);self.state['resolved']=self.state['resolved'][-1000:]
        self.state['pending']=keep
    def _refresh_sync(self):
        be=_tail(BIN_RAW);ye=_tail(BY_RAW);books=_tail(BOOK,20000)
        self.state['last_new_events']=self._detect(be,ye,books);self._resolve(books);self._save();return books
    async def refresh(self):
        try:books=await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:books=[];self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status(books)
    def status(self,books=None):
        books=_tail(BOOK,20000) if books is None else books;r=self.state.get('resolved') or []
        freeze=float(self.state.get('rule_frozen_at') or time.time());ends=[float(x.get('ts') or 0) for x in books]
        hours=max(0.0,((max(ends) if ends else freeze)-freeze)/3600);syms=sorted({x.get('symbol') for x in r if x.get('symbol')})
        pressures={p:sum(1 for x in r if x.get('pressure')==p) for p in ('BUY','SELL')}
        aw=sum(1 for x in r if float(x.get('absorption_net_return_pct') or 0)>0);cw=sum(1 for x in r if float(x.get('continuation_net_return_pct') or 0)>0)
        ast={f'{int(m)}x':_stress(r,'absorption',m) for m in STRESS};cst={f'{int(m)}x':_stress(r,'continuation',m) for m in STRESS}
        sample=bool(hours>=MIN_HOURS and len(r)>=MIN_RESOLVED and len(syms)>=MIN_SYMBOLS and all(pressures[p]>=MIN_PER_PRESSURE for p in pressures))
        ready={'absorption':bool(sample and ast['2x']>0 and ast['3x']>0),'continuation':bool(sample and cst['2x']>0 and cst['3x']>0)}
        gate={'collection_hours':hours,'required_hours':MIN_HOURS,'resolved_events':len(r),'required_resolved_events':MIN_RESOLVED,
              'resolved_symbols':len(syms),'required_symbols':MIN_SYMBOLS,'pressure_events':pressures,'required_per_pressure':MIN_PER_PRESSURE,
              'sample_ready':sample,'absorption_stress_return_pct':ast,'continuation_stress_return_pct':cst,
              'hypothesis_ready':ready,'selection_policy':'NO_AUTOMATIC_WINNER','ready_for_review':bool(ready['absorption'] or ready['continuation'])}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_SHADOW','strategy':'LIQUIDATION_ABSORPTION_V1',
            'live_enabled':False,'paper_only':True,'rule_frozen_at':self.state.get('rule_frozen_at'),'pending_count':len(self.state.get('pending') or []),
            'resolved_count':len(r),'future_gate':gate,'absorption':{'equity':self.state.get('absorption_equity',100.0),'return_pct':float(self.state.get('absorption_equity',100.0))-100,'wins':aw,'win_rate':aw/len(r) if r else None,'cost_stress_return_pct':ast},
            'continuation':{'equity':self.state.get('continuation_equity',100.0),'return_pct':float(self.state.get('continuation_equity',100.0))-100,'wins':cw,'win_rate':cw/len(r) if r else None,'cost_stress_return_pct':cst},
            'promotion_eligible':False,'last_refresh':self.last_refresh,'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='liquidation-absorption-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(60)
            if self.enabled:await self.refresh()

liquidation_absorption_shadow=LiquidationAbsorptionShadow()
