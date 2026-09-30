from __future__ import annotations
import asyncio, json, math, statistics, time
from datetime import datetime, timezone
from pathlib import Path
from app.services.hot_book_cache import hot_book_cache

ROOT = Path(__file__).parents[2]
DATA_DIR = ROOT / 'data' / 'microstructure_v63'
STATE = ROOT / 'data' / 'raven_v63_microstructure_shadow_v2.json'
VENUES = ('Binance', 'Bybit', 'OKX')
SYMBOLS = ('BTCUSDT', 'ETHUSDT', 'SOLUSDT')
SAMPLE_SECONDS = 30
DECISION_SECONDS = 300
HORIZONS = (300, 900, 1800, 3600)
BASE_ROUND_TRIP_BPS = 12.0
SLIPPAGE_RESERVE_BPS = 2.0
CAPITAL_PER_SIGNAL = 0.10
RETENTION_DAYS = 30


def _hblank():
    return {'equity':100.0,'peak':100.0,'max_dd_pct':0.0,
            'resolved':0,'wins':0,'net_sum':0.0,'cost_sum':0.0}

class RavenV63MicrostructureShadowV2:
    def __init__(self):
        self.enabled=True; self.task=None; self.last_error=None
        self.last_refresh=None; self.latest={}
        self.state=self._load(); self.recent=self._load_recent(40)

    @staticmethod
    def _blank():
        return {'mode':'PAPER_SHADOW','version':'v63.2','future_only':True,
                'rule_id':'v63_temporal_multihorizon_v2','equity_model':'PER_HORIZON',
                'sample_count':0,'decision_count':0,'started_at':time.time(),
                'last_sample_bucket':None,'last_decision_bucket':None,
                'open_trials':[],'horizons':{str(h):_hblank() for h in HORIZONS}}

    def _load(self):
        try:
            x=json.loads(STATE.read_text(encoding='utf-8'))
            if not isinstance(x,dict): return self._blank()
            for h in HORIZONS:x.setdefault('horizons',{}).setdefault(str(h),_hblank())
            return x
        except Exception:return self._blank()

    def _save(self):
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')

    def _load_recent(self,n):
        out=[]
        for p in sorted(DATA_DIR.glob('microstructure_*.jsonl'))[-2:]:
            try:
                for line in p.read_text(encoding='utf-8').splitlines()[-n:]:
                    if line.strip(): out.append(json.loads(line))
            except Exception: pass
        return sorted(out,key=lambda x:float(x.get('ts') or 0))[-n:]

    @staticmethod
    def _legs():
        return [{'venue':v,'symbol':s} for v in VENUES for s in SYMBOLS]

    @staticmethod
    def _book_features(bids,asks,age_ms):
        b=bids[:5]; a=asks[:5]
        if not b or not a:return None
        bid,ask=float(b[0][0]),float(a[0][0])
        if bid<=0 or ask<=bid:return None
        mid=(bid+ask)/2.0; spread=(ask-bid)/mid*10000.0
        bq=sum(float(p)*float(q) for p,q in b); aq=sum(float(p)*float(q) for p,q in a)
        denom=bq+aq; imb=(bq-aq)/denom if denom>0 else 0.0
        b1=float(b[0][1]); a1=float(a[0][1])
        micro=(ask*b1+bid*a1)/(b1+a1) if b1+a1>0 else mid
        edge=(micro-mid)/mid*10000.0
        return {'mid':mid,'spread_bps':spread,'imbalance':imb,
                'micro_edge_bps':edge,'depth_quote':denom,'age_ms':float(age_ms)}

    def _symbol_features(self,symbol):
        rows=[]
        for venue in VENUES:
            hit=hot_book_cache.get(venue,symbol,max_age=2.0)
            if not hit:continue
            x=self._book_features(hit[0],hit[1],hit[2])
            if x:rows.append({'venue':venue,**x})
        if len(rows)<2:return None
        mids=[x['mid'] for x in rows]; spreads=[x['spread_bps'] for x in rows]
        imbs=[x['imbalance'] for x in rows]; edges=[x['micro_edge_bps'] for x in rows]
        depths=[x['depth_quote'] for x in rows]
        mid=statistics.median(mids); spread=statistics.median(spreads)
        imb=statistics.median(imbs); edge=statistics.median(edges)
        dispersion=(max(mids)-min(mids))/mid*10000.0 if mid>0 else 999.0
        direction=1 if imb>0 else (-1 if imb<0 else 0)
        agreement=sum(1 for x in imbs if (x>0 and direction>0) or (x<0 and direction<0))
        return {'symbol':symbol,'venues':rows,'venue_count':len(rows),'mid':mid,
                'spread_bps':spread,'imbalance':imb,'micro_edge_bps':edge,
                'dispersion_bps':dispersion,'agreement':agreement,
                'depth_quote_median':statistics.median(depths)}

    def _temporal(self,symbol,current):
        hist=[]
        for row in self.recent[-10:]:
            x=(row.get('symbols') or {}).get(symbol)
            if x is not None:hist.append(float(x.get('imbalance') or 0.0))
        vals=(hist+[float(current['imbalance'])])[-10:]
        fast=statistics.mean(vals[-3:]); slow=statistics.mean(vals)
        sign=1 if vals[-1]>0 else (-1 if vals[-1]<0 else 0)
        same=sum(1 for v in vals if (v>0 and sign>0) or (v<0 and sign<0))
        persistence=same/len(vals) if vals else 0.0
        delta=vals[-1]-vals[-5] if len(vals)>=5 else 0.0
        micro_ratio=max(-1.0,min(1.0,float(current['micro_edge_bps'])/max(float(current['spread_bps']),0.25)))
        score=0.40*float(current['imbalance'])+0.30*fast+0.20*slow+0.10*micro_ratio
        tradable=(int(current['venue_count'])==len(VENUES) and abs(score)>=0.30 and
                  persistence>=0.70 and int(current['agreement'])>=2 and
                  float(current['spread_bps'])<=3.0 and float(current['dispersion_bps'])<=6.0 and
                  float(current['depth_quote_median'])>=50_000.0)
        return {'imb_fast':fast,'imb_slow':slow,'imb_delta':delta,
                'persistence':persistence,'score_v2':score,
                'signal_v2':(1 if score>0 else -1) if tradable else 0}

    def _snapshot(self,now):
        symbols={}
        for symbol in SYMBOLS:
            x=self._symbol_features(symbol)
            if not x:continue
            x.update(self._temporal(symbol,x)); symbols[symbol]=x
        full=sum(1 for x in symbols.values() if int(x['venue_count'])==len(VENUES))
        return {'ts':now,'coverage':len(symbols),'full_coverage':full,'symbols':symbols}

    def _append_snapshot(self,snap):
        DATA_DIR.mkdir(parents=True,exist_ok=True)
        day=datetime.fromtimestamp(snap['ts'],tz=timezone.utc).strftime('%Y%m%d')
        path=DATA_DIR/f'microstructure_v2_{day}.jsonl'
        with path.open('a',encoding='utf-8') as f:
            f.write(json.dumps(snap,ensure_ascii=False,separators=(',',':'))+'\n')
        self.recent.append(snap); self.recent=self.recent[-40:]

    def _resolve_trials(self,snap):
        now=float(snap['ts']); keep=[]; per_h={h:0.0 for h in HORIZONS}
        for t in self.state.get('open_trials') or []:
            h=int(t.get('horizon') or 0)
            if now-float(t.get('entry_ts') or 0)<h-5: keep.append(t); continue
            x=snap['symbols'].get(str(t.get('symbol') or ''))
            if not x: keep.append(t); continue
            entry=float(t['entry_mid']); exit_=float(x['mid']); sig=int(t['signal'])
            raw=sig*(exit_/entry-1.0)
            cost_bps=BASE_ROUND_TRIP_BPS+SLIPPAGE_RESERVE_BPS+0.5*(float(t['entry_spread_bps'])+float(x['spread_bps']))
            net=raw-cost_bps/10000.0; per_h[h]+=CAPITAL_PER_SIGNAL*net
            hs=self.state['horizons'][str(h)]; hs['resolved']+=1
            hs['wins']+=int(net>0); hs['net_sum']+=net; hs['cost_sum']+=cost_bps/10000.0
        self.state['open_trials']=keep
        for h,r in per_h.items():
            if not r:continue
            hs=self.state['horizons'][str(h)]; eq=float(hs['equity'])*(1.0+r)
            hs['equity']=eq; hs['peak']=max(float(hs['peak']),eq)
            hs['max_dd_pct']=min(float(hs['max_dd_pct']),(eq/hs['peak']-1.0)*100.0)
    def _new_trials(self,snap):
        now=float(snap['ts'])
        active={(str(t.get('symbol')),int(t.get('horizon') or 0)) for t in self.state.get('open_trials') or []}
        opened=0
        for sym,x in snap['symbols'].items():
            sig=int(x.get('signal_v2') or 0)
            if not sig:continue
            for h in HORIZONS:
                if (sym,h) in active:continue
                self.state['open_trials'].append({'symbol':sym,'horizon':h,'signal':sig,
                    'entry_ts':now,'entry_mid':float(x['mid']),'entry_spread_bps':float(x['spread_bps']),
                    'score':float(x['score_v2']),'persistence':float(x['persistence'])})
                opened+=1
        return opened

    def _decision(self,snap,bucket):
        self._resolve_trials(snap); self._new_trials(snap)
        self.state['decision_count']=int(self.state.get('decision_count') or 0)+1
        self.state['last_decision_bucket']=bucket

    async def refresh(self):
        try:
            now=time.time(); sb=int(now//SAMPLE_SECONDS)
            hot_book_cache.watch_legs(self._legs(),priority=220,ttl=3600.0)
            if self.state.get('last_sample_bucket')==sb:return self.status()
            snap=self._snapshot(now)
            if snap['coverage']<2:raise RuntimeError(f'V63V2_BOOK_COVERAGE_LOW:{snap["coverage"]}/3')
            self._append_snapshot(snap); self.state['sample_count']=int(self.state.get('sample_count') or 0)+1
            self.state['last_sample_bucket']=sb; db=int(now//DECISION_SECONDS)
            if self.state.get('last_decision_bucket')!=db:self._decision(snap,db)
            self._save(); self.latest=snap; self.last_error=None; self.last_refresh=now
        except Exception as exc:
            self.last_error=str(exc)[:500]; self.last_refresh=time.time()
        return self.status()

    def status(self):
        age=max(0.0,time.time()-float(self.state.get('started_at') or time.time()))
        samples=int(self.state.get('sample_count') or 0)
        mins=min(int(self.state['horizons'][str(h)].get('resolved') or 0) for h in HORIZONS)
        if age<86400 or samples<1000:phase='COLLECTING'
        elif age<259200 or mins<100:phase='FUTURE_WARMUP'
        else:phase='FUTURE_VALIDATION'
        hs={}
        for h in HORIZONS:
            x=self.state['horizons'][str(h)]; n=int(x.get('resolved') or 0); eq=float(x.get('equity') or 100.0)
            hs[str(h)]={'equity':round(eq,6),'return_pct':round(eq-100.0,4),
                'max_dd_pct':round(float(x.get('max_dd_pct') or 0.0),4),'resolved':n,
                'wins':int(x.get('wins') or 0),'win_rate':round(int(x.get('wins') or 0)/n,4) if n else None,
                'mean_net_signal_pct':round(float(x.get('net_sum') or 0.0)/n*100.0,5) if n else None,
                'mean_cost_bps':round(float(x.get('cost_sum') or 0.0)/n*10000.0,4) if n else None}
        eligible_horizons=[]
        for h in HORIZONS:
            x=hs[str(h)]
            if int(x.get('resolved') or 0)>=100 and float(x.get('mean_net_signal_pct') or -999.0)>0.0:
                eligible_horizons.append(h)
        allocator_eligible=bool(eligible_horizons) and phase=='FUTURE_VALIDATION'
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW','version':'v63.2',
                'rule_id':'v63_temporal_multihorizon_v2','phase':phase,'future_only':True,
                'promotion_eligible':False,'allocator_eligible':allocator_eligible,
                'eligible_horizons_seconds':eligible_horizons,
                'capital_gate':'OPEN' if allocator_eligible else 'CLOSED_INSUFFICIENT_FUTURE_NET_EDGE',
                'live_enabled':False,'grid':False,'martingale':False,'dca':False,
                'sample_seconds':SAMPLE_SECONDS,'decision_seconds':DECISION_SECONDS,'horizons_seconds':list(HORIZONS),
                'base_round_trip_cost_bps':BASE_ROUND_TRIP_BPS,'slippage_reserve_bps':SLIPPAGE_RESERVE_BPS,
                'capital_per_signal_pct':CAPITAL_PER_SIGNAL*100.0,'sample_count':samples,
                'decision_count':int(self.state.get('decision_count') or 0),'open_trials':len(self.state.get('open_trials') or []),
                'horizon_stats':hs,'last_refresh':self.last_refresh,'last_error':self.last_error,
                'coverage':int((self.latest or {}).get('coverage') or 0),'full_coverage':int((self.latest or {}).get('full_coverage') or 0),
                'latest_symbols':{k:{'score_v2':round(float(v.get('score_v2') or 0),5),
                    'signal_v2':int(v.get('signal_v2') or 0),'persistence':round(float(v.get('persistence') or 0),4),
                    'imbalance':round(float(v.get('imbalance') or 0),5),'spread_bps':round(float(v.get('spread_bps') or 0),4),
                    'venue_count':int(v.get('venue_count') or 0)} for k,v in ((self.latest or {}).get('symbols') or {}).items()}}

    async def start(self):
        if self.task and not self.task.done():return
        DATA_DIR.mkdir(parents=True,exist_ok=True); hot_book_cache.watch_legs(self._legs(),priority=220,ttl=3600.0)
        await asyncio.sleep(0.15); await self.refresh()
        self.task=asyncio.create_task(self._loop(),name='raven-v63-microstructure-shadow-v2')

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None; self._save()
    async def _loop(self):
        while True:
            await asyncio.sleep(5.0)
            if not self.enabled:continue
            if int(time.time()//SAMPLE_SECONDS)==self.state.get('last_sample_bucket'):continue
            await self.refresh()

raven_v63_microstructure_shadow_v2=RavenV63MicrostructureShadowV2()
