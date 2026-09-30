from __future__ import annotations
import asyncio, json, math, statistics, time
from datetime import datetime, timezone
from pathlib import Path

from app.services.hot_book_cache import hot_book_cache

ROOT = Path(__file__).parents[2]
DATA_DIR = ROOT / 'data' / 'microstructure_v63'
STATE = ROOT / 'data' / 'raven_v63_microstructure_shadow.json'
VENUES = ('Binance', 'Bybit', 'OKX')
SYMBOLS = ('BTCUSDT', 'ETHUSDT', 'SOLUSDT')
SAMPLE_SECONDS = 30
DECISION_SECONDS = 300
ROUND_TRIP_COST = 0.0012
CAPITAL_PER_SIGNAL = 0.10
MIN_VENUES = 2
RETENTION_DAYS = 30

class RavenV63MicrostructureShadow:
    def __init__(self):
        self.enabled = True
        self.task = None
        self.last_error = None
        self.last_refresh = None
        self.latest = {}
        self.state = self._load()
    @staticmethod
    def _blank():
        return {
            'mode':'PAPER_SHADOW','version':'v63','future_only':True,
            'rule_id':'v63_full3_fixed_v1','strict_full_venue_gate':True,
            'equity':100.0,'peak':100.0,'max_dd_pct':0.0,
            'sample_count':0,'decision_count':0,'resolved_signals':0,
            'wins':0,'net_signal_sum':0.0,'cost_sum':0.0,
            'started_at':time.time(),'last_sample_bucket':None,
            'last_decision_bucket':None,'open_trials':[],'history':[]
        }

    def _load(self):
        try:
            x=json.loads(STATE.read_text(encoding='utf-8'))
            return x if isinstance(x,dict) else self._blank()
        except Exception:
            return self._blank()

    def _save(self):
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')

    @staticmethod
    def _legs():
        return [{'venue':v,'symbol':s} for v in VENUES for s in SYMBOLS]
    @staticmethod
    def _book_features(bids, asks, age_ms):
        b=bids[:5]; a=asks[:5]
        if not b or not a:return None
        bid,ask=float(b[0][0]),float(a[0][0])
        if bid<=0 or ask<=bid:return None
        mid=(bid+ask)/2.0
        spread=(ask-bid)/mid*10000.0
        bq=sum(float(p)*float(q) for p,q in b)
        aq=sum(float(p)*float(q) for p,q in a)
        denom=bq+aq
        imb=(bq-aq)/denom if denom>0 else 0.0
        b1=float(b[0][1]); a1=float(a[0][1])
        micro=(ask*b1+bid*a1)/(b1+a1) if b1+a1>0 else mid
        edge=(micro-mid)/mid*10000.0
        return {'mid':mid,'spread_bps':spread,'imbalance':imb,
                'micro_edge_bps':edge,'depth_quote':denom,'age_ms':float(age_ms)}

    def _symbol_features(self, symbol):
        rows=[]
        for venue in VENUES:
            hit=hot_book_cache.get(venue,symbol,max_age=2.0)
            if not hit:continue
            x=self._book_features(hit[0],hit[1],hit[2])
            if x:rows.append({'venue':venue,**x})
        if len(rows)<MIN_VENUES:return None
        mids=[x['mid'] for x in rows]
        spreads=[x['spread_bps'] for x in rows]
        imbs=[x['imbalance'] for x in rows]
        edges=[x['micro_edge_bps'] for x in rows]
        depths=[x['depth_quote'] for x in rows]
        mid=statistics.median(mids); spread=statistics.median(spreads)
        imb=statistics.median(imbs); edge=statistics.median(edges)
        dispersion=(max(mids)-min(mids))/mid*10000.0 if mid>0 else 999.0
        ratio=edge/max(spread,0.25)
        score=0.80*imb+0.40*max(-1.0,min(1.0,ratio))
        direction=1 if score>0 else (-1 if score<0 else 0)
        agreement=sum(1 for x in imbs if (x>0 and direction>0) or (x<0 and direction<0))
        liquid=statistics.median(depths)>=50_000.0
        tradable=(len(rows)==len(VENUES) and abs(score)>=0.22 and agreement>=2 and spread<=3.0 and dispersion<=6.0 and liquid)
        signal=direction if tradable else 0
        return {'symbol':symbol,'venues':rows,'venue_count':len(rows),'mid':mid,
                'spread_bps':spread,'imbalance':imb,'micro_edge_bps':edge,
                'dispersion_bps':dispersion,'agreement':agreement,'score':score,
                'signal':signal,'depth_quote_median':statistics.median(depths)}

    def _snapshot(self, now):
        symbols={}
        for symbol in SYMBOLS:
            x=self._symbol_features(symbol)
            if x:symbols[symbol]=x
        full=sum(1 for x in symbols.values() if int(x.get('venue_count') or 0)==len(VENUES))
        return {'ts':now,'symbols':symbols,'coverage':len(symbols),'full_coverage':full,'required':len(SYMBOLS)}
    def _cleanup_data(self):
        cutoff=time.time()-RETENTION_DAYS*86400.0
        for p in DATA_DIR.glob('microstructure_*.jsonl'):
            try:
                if p.stat().st_mtime<cutoff:p.unlink()
            except OSError:pass

    def _append_snapshot(self, snap):
        DATA_DIR.mkdir(parents=True,exist_ok=True)
        day=datetime.fromtimestamp(snap['ts'],tz=timezone.utc).strftime('%Y%m%d')
        path=DATA_DIR/f'microstructure_{day}.jsonl'
        compact={'ts':snap['ts'],'coverage':snap['coverage'],'full_coverage':snap.get('full_coverage',0),'symbols':{}}
        for sym,x in snap['symbols'].items():
            compact['symbols'][sym]={k:x[k] for k in (
                'mid','spread_bps','imbalance','micro_edge_bps','dispersion_bps',
                'agreement','score','signal','depth_quote_median','venue_count')}
            compact['symbols'][sym]['venues']=[{
                'venue':v['venue'],'mid':v['mid'],'spread_bps':v['spread_bps'],
                'imbalance':v['imbalance'],'micro_edge_bps':v['micro_edge_bps'],
                'depth_quote':v['depth_quote'],'age_ms':v['age_ms']
            } for v in x['venues']]
        with path.open('a',encoding='utf-8') as f:
            f.write(json.dumps(compact,ensure_ascii=False,separators=(',',':'))+'\n')

    def _resolve_trials(self, snap):
        due=[]; keep=[]; now=float(snap['ts'])
        for t in self.state.get('open_trials') or []:
            if now-float(t.get('entry_ts') or 0)>=DECISION_SECONDS-5:due.append(t)
            else:keep.append(t)
        portfolio_ret=0.0; resolved=[]
        for t in due:
            x=snap['symbols'].get(str(t.get('symbol') or ''))
            entry=float(t.get('entry_mid') or 0); signal=int(t.get('signal') or 0)
            if not x or entry<=0 or signal==0:
                keep.append(t); continue
            raw=signal*(float(x['mid'])/entry-1.0)
            net=raw-ROUND_TRIP_COST
            contribution=CAPITAL_PER_SIGNAL*net
            portfolio_ret+=contribution
            self.state['resolved_signals']=int(self.state.get('resolved_signals') or 0)+1
            if net>0:self.state['wins']=int(self.state.get('wins') or 0)+1
            self.state['net_signal_sum']=float(self.state.get('net_signal_sum') or 0.0)+net
            self.state['cost_sum']=float(self.state.get('cost_sum') or 0.0)+ROUND_TRIP_COST*CAPITAL_PER_SIGNAL
            resolved.append({'symbol':t['symbol'],'signal':signal,'entry_ts':t['entry_ts'],
                             'exit_ts':now,'raw_ret_pct':raw*100.0,'net_ret_pct':net*100.0,
                             'score':t.get('score'),'entry_mid':entry,'exit_mid':float(x['mid'])})
        self.state['open_trials']=keep
        if resolved:
            eq=float(self.state.get('equity') or 100.0)*max(0.01,1.0+portfolio_ret)
            peak=max(float(self.state.get('peak') or 100.0),eq)
            dd=(eq/peak-1.0)*100.0
            self.state['equity']=eq; self.state['peak']=peak
            self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0.0),dd)
        return resolved,portfolio_ret
    def _new_trials(self, snap):
        opened=[]; now=float(snap['ts'])
        active={str(x.get('symbol')) for x in (self.state.get('open_trials') or [])}
        for sym,x in snap['symbols'].items():
            signal=int(x.get('signal') or 0)
            if not signal or sym in active or int(x.get('venue_count') or 0)!=len(VENUES):continue
            t={'symbol':sym,'signal':signal,'entry_ts':now,'entry_mid':float(x['mid']),
               'score':float(x['score']),'imbalance':float(x['imbalance']),
               'spread_bps':float(x['spread_bps']),'agreement':int(x['agreement']),
               'venue_count':int(x['venue_count'])}
            self.state.setdefault('open_trials',[]).append(t); opened.append(t)
        return opened

    def _decision(self, snap, bucket):
        resolved,portfolio_ret=self._resolve_trials(snap)
        opened=self._new_trials(snap)
        self.state['decision_count']=int(self.state.get('decision_count') or 0)+1
        self.state['last_decision_bucket']=bucket
        eq=float(self.state.get('equity') or 100.0)
        peak=max(float(self.state.get('peak') or 100.0),eq)
        dd=(eq/peak-1.0)*100.0
        h=self.state.get('history') or []
        h.append({'ts':snap['ts'],'equity':eq,'drawdown_pct':dd,
                  'portfolio_ret_pct':portfolio_ret*100.0,
                  'opened':[{'symbol':x['symbol'],'signal':x['signal'],'score':x['score']} for x in opened],
                  'resolved':resolved,'coverage':snap['coverage']})
        self.state['history']=h[-1000:]
    async def refresh(self):
        try:
            now=time.time(); sample_bucket=int(now//SAMPLE_SECONDS)
            hot_book_cache.watch_legs(self._legs(),priority=220,ttl=3600.0)
            if self.state.get('last_sample_bucket')==sample_bucket:
                return self.status()
            snap=self._snapshot(now)
            if snap['coverage']<2:
                raise RuntimeError(f'V63_BOOK_COVERAGE_LOW:{snap["coverage"]}/{len(SYMBOLS)}')
            self._append_snapshot(snap)
            self.state['sample_count']=int(self.state.get('sample_count') or 0)+1
            self.state['last_sample_bucket']=sample_bucket
            decision_bucket=int(now//DECISION_SECONDS)
            if self.state.get('last_decision_bucket')!=decision_bucket:
                self._decision(snap,decision_bucket)
            self._save()
            self.latest=snap; self.last_error=None; self.last_refresh=now
        except Exception as exc:
            self.last_error=str(exc)[:500]; self.last_refresh=time.time()
        return self.status()

    def status(self):
        resolved=int(self.state.get('resolved_signals') or 0)
        wins=int(self.state.get('wins') or 0)
        age=max(0.0,time.time()-float(self.state.get('started_at') or time.time()))
        if age<3600 or int(self.state.get('sample_count') or 0)<60:phase='COLLECTING'
        elif age<86400 or resolved<30:phase='FUTURE_WARMUP'
        else:phase='FUTURE_VALIDATION'
        eq=float(self.state.get('equity') or 100.0)
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW',
                'version':'v63','rule_id':'v63_full3_fixed_v1','strict_full_venue_gate':True,
                'phase':phase,'future_only':True,'promotion_eligible':False,
                'live_enabled':False,'grid':False,'martingale':False,'dca':False,
                'sample_seconds':SAMPLE_SECONDS,'decision_seconds':DECISION_SECONDS,
                'retention_days':RETENTION_DAYS,
                'round_trip_cost_bps':ROUND_TRIP_COST*10000.0,
                'capital_per_signal_pct':CAPITAL_PER_SIGNAL*100.0,
                'equity':round(eq,6),'return_pct':round(eq-100.0,4),
                'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0.0),4),
                'sample_count':int(self.state.get('sample_count') or 0),
                'decision_count':int(self.state.get('decision_count') or 0),
                'resolved_signals':resolved,'wins':wins,
                'win_rate':round(wins/resolved,4) if resolved else None,
                'mean_net_signal_pct':round(float(self.state.get('net_signal_sum') or 0.0)/resolved*100.0,5) if resolved else None,
                'open_trials':len(self.state.get('open_trials') or []),
                'last_refresh':self.last_refresh,'last_error':self.last_error,
                'coverage':int((self.latest or {}).get('coverage') or 0),
                'full_coverage':int((self.latest or {}).get('full_coverage') or 0),
                'latest_symbols':{k:{'score':round(float(v['score']),5),'signal':int(v['signal']),
                    'spread_bps':round(float(v['spread_bps']),4),'imbalance':round(float(v['imbalance']),5),
                    'agreement':int(v['agreement']),'venue_count':int(v['venue_count'])}
                    for k,v in ((self.latest or {}).get('symbols') or {}).items()}}

    async def start(self):
        if self.task and not self.task.done():return
        DATA_DIR.mkdir(parents=True,exist_ok=True)
        self._cleanup_data()
        hot_book_cache.watch_legs(self._legs(),priority=220,ttl=3600.0)
        await asyncio.sleep(0.15)
        await self.refresh()
        self.task=asyncio.create_task(self._loop(),name='raven-v63-microstructure-shadow')
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

raven_v63_microstructure_shadow=RavenV63MicrostructureShadow()

