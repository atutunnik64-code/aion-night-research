from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.execution_engine import execution_engine

ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'raven_v105_bk_finite_arbitrage_shadow.json'
CAPITAL=100.0;RESERVE_PCT=0.20;HOLD_SECONDS=3600.0;EPISODE_GAP=65.0

class RavenV105BKFiniteArbitrageShadow:
    def __init__(self):
        self.enabled=True;self.interval=30.0;self.task=None
        self.last_error=None;self.last_refresh=None;self.latest={}
        self.refresh_lock=asyncio.Lock();self.state=self._load()
    @staticmethod
    def _base(max_id):
        return {'start_id':max_id,'last_processed_id':max_id,'equity':CAPITAL,'peak':CAPITAL,
                'max_dd_pct':0.0,'accepted_events':0,'positive_events':0,'negative_events':0,
                'profit_usdt':0.0,'duplicates':0,'skipped_capital':0,'last_episode_ts':{},
                'locks':[],'accepted_signatures':[],'first_accept_ts':None,'last_accept_ts':None,
                'history':[],'started_at':time.time()}
    def _load(self):
        try:return json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:
            r=execution_engine.db.execute('SELECT COALESCE(MAX(id),0) m FROM executions').fetchone()
            return self._base(int(r['m'] or 0))
    def _save(self):
        STATE.parent.mkdir(parents=True,exist_ok=True)
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _route_meta(payload):
        try:p=json.loads(payload or '{}');row=p.get('row') or {}
        except Exception:return set(),False
        v=set(row.get('execution_venues') or [])
        for z in row.get('steps') or []:
            if z.get('venue'):v.add(str(z['venue']))
        for k in ('buy_venue','sell_venue'):
            if row.get(k):v.add(str(row[k]))
        return v,bool(row.get('full_loop_confirmed'))
    def _accept(self,r):
        t=float(r['ts']);sig=str(r['signature'] or '')
        last=float((self.state.get('last_episode_ts') or {}).get(sig) or 0.0)
        m=dict(self.state.get('last_episode_ts') or {});m[sig]=t;self.state['last_episode_ts']=m
        if last and t-last<=EPISODE_GAP:
            self.state['duplicates']=int(self.state.get('duplicates') or 0)+1;return None
        locks=[x for x in (self.state.get('locks') or []) if float(x.get('until') or 0)>t]
        notional=float(r['notional'] or 0);need=2.0*notional
        used=sum(float(x.get('capital') or 0) for x in locks);available=CAPITAL*(1-RESERVE_PCT)-used
        if notional<=0 or need>available:
            self.state['skipped_capital']=int(self.state.get('skipped_capital') or 0)+1
            self.state['locks']=locks;return None
        pnl=float(r['realized_profit'] or 0);eq=float(self.state.get('equity') or CAPITAL)+pnl
        self.state['equity']=eq;self.state['profit_usdt']=float(self.state.get('profit_usdt') or 0)+pnl
        self.state['accepted_events']=int(self.state.get('accepted_events') or 0)+1
        if pnl>0:self.state['positive_events']=int(self.state.get('positive_events') or 0)+1
        elif pnl<0:self.state['negative_events']=int(self.state.get('negative_events') or 0)+1
        self.state['peak']=max(float(self.state.get('peak') or eq),eq)
        self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0.0),(eq/self.state['peak']-1)*100.0)
        locks.append({'until':t+HOLD_SECONDS,'capital':need,'signature':sig});self.state['locks']=locks
        sigs=list(self.state.get('accepted_signatures') or [])
        if sig not in sigs:sigs.append(sig)
        self.state['accepted_signatures']=sigs
        if not self.state.get('first_accept_ts'):self.state['first_accept_ts']=t
        self.state['last_accept_ts']=t
        row={'id':int(r['id']),'ts':t,'signature':sig,'notional':notional,
             'net_pct':float(r['net_pct'] or 0),'profit':pnl,'equity':eq}
        h=list(self.state.get('history') or []);h.append(row);self.state['history']=h[-200:]
        return row

    def _refresh_sync(self):
        try:
            last_id=int(self.state.get('last_processed_id') or self.state.get('start_id') or 0)
            q="SELECT id,ts,strategy,signature,status,notional,net_pct,realized_profit,payload FROM executions WHERE id>? AND environment='DEMO' ORDER BY id"
            rows=execution_engine.db.execute(q,(last_id,)).fetchall();accepted=[]
            for r in rows:
                self.state['last_processed_id']=max(int(self.state.get('last_processed_id') or 0),int(r['id']))
                if str(r['strategy'])!='FULL_NOW' or str(r['status'])!='FILLED_BOTH':continue
                venues,confirmed=self._route_meta(r['payload'])
                if not confirmed or venues!={'Binance','KuCoin'}:continue
                x=self._accept(r)
                if x:accepted.append(x)
            self._save();self.last_error=None;self.last_refresh=time.time()
            self.latest={'strategy':'v105_bk_finite_arbitrage','new_rows':len(rows),'new_accepted':accepted,
                         'last_processed_id':self.state.get('last_processed_id')}
        except Exception as exc:
            self.last_error=str(exc)[:400];self.last_refresh=time.time()
        return self.status()

    async def refresh(self):
        async with self.refresh_lock:return self._refresh_sync()
    @staticmethod
    def _median(vals):
        z=sorted(vals);n=len(z)
        if not n:return 0.0
        return z[n//2] if n%2 else (z[n//2-1]+z[n//2])/2.0
    def status(self):
        eq=float(self.state.get('equity') or CAPITAL);n=int(self.state.get('accepted_events') or 0)
        pos=int(self.state.get('positive_events') or 0);hist=list(self.state.get('history') or [])
        nets=[float(x.get('net_pct') or 0) for x in hist];med=self._median(nets)
        first=self.state.get('first_accept_ts');last=self.state.get('last_accept_ts')
        span=((float(last)-float(first))/3600.0) if first and last else 0.0
        unique=len(self.state.get('accepted_signatures') or []);pr=pos/n if n else 0.0
        gate={'required_events':12,'accepted_events':n,'required_unique_routes':3,'unique_routes':unique,
              'required_span_hours':48.0,'span_hours':span,'positive_ratio':pr,'median_net_pct':med,
              'profit_usdt':float(self.state.get('profit_usdt') or 0.0),
              'ready_for_review':n>=12 and unique>=3 and span>=48.0 and pr>=0.8 and med>=0.2 and float(self.state.get('profit_usdt') or 0)>0}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW',
                'strategy':'v105_bk_finite_arbitrage','future_only':True,'promotion_eligible':False,
                'live_enabled':False,'grid':False,'martingale':False,'dca':False,
                'locked_config':{'venues':['Binance','KuCoin'],'capital':CAPITAL,'reserve_pct':RESERVE_PCT,
                                 'hold_seconds':HOLD_SECONDS,'episode_gap_seconds':EPISODE_GAP,'full_loop_confirmed_only':True},
                'equity':round(eq,6),'return_pct':round((eq/CAPITAL-1)*100,4),
                'profit_usdt':round(float(self.state.get('profit_usdt') or 0),6),
                'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0),4),
                'accepted_events':n,'positive_events':pos,'negative_events':int(self.state.get('negative_events') or 0),
                'duplicates':int(self.state.get('duplicates') or 0),'skipped_capital':int(self.state.get('skipped_capital') or 0),
                'unique_routes':unique,'future_validation_gate':gate,
                'phase':'WARMUP' if not gate['ready_for_review'] else 'FUTURE_VALIDATION_REVIEW_READY',
                'latest':self.latest,'last_refresh':self.last_refresh,'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='raven-v105-bk-finite-arbitrage-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None;self._save()
    async def _loop(self):
        while True:
            await self.refresh();await asyncio.sleep(max(15.0,self.interval))

raven_v105_bk_finite_arbitrage_shadow=RavenV105BKFiniteArbitrageShadow()
