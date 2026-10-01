from __future__ import annotations
import asyncio,json,time,statistics,hashlib
from pathlib import Path
ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
STRUCT=DATA/'moex_universe_structure_v1.json'
STATE=DATA/'moex_universe_features_v1.json'; HIST=DATA/'moex_universe_features_v1.jsonl'
INTERVAL=300.0; REQUIRED_SAMPLES=288; REQUIRED_HOURS=24.0

def _load(p,default):
    try:return json.loads(p.read_text(encoding='utf-8-sig'))
    except Exception:return default

def _stats(values):
    vals=[float(x) for x in values if x is not None]
    if not vals:return {'n':0,'last':None,'mean':None,'median':None,'stdev':None,'min':None,'max':None}
    return {'n':len(vals),'last':vals[-1],'mean':sum(vals)/len(vals),'median':statistics.median(vals),
            'stdev':statistics.pstdev(vals) if len(vals)>1 else 0.0,'min':min(vals),'max':max(vals)}

def _source():
    st=_load(STRUCT,{}); row=st.get('last') or {}
    return row.get('ts'), list(row.get('liquid_curves') or [])

def _fingerprint(curves):
    rows=[]
    for c in curves:
        rows.append((str(c.get('asset') or ''),str(c.get('front') or ''),str(c.get('next') or ''),
                     round(float(c.get('front_px') or 0),10),round(float(c.get('next_px') or 0),10),
                     round(float(c.get('curve_pct') or 0),10),round(float(c.get('front_value_today') or 0),4),
                     round(float(c.get('next_value_today') or 0),4)))
    raw=json.dumps(sorted(rows),separators=(',',':'),ensure_ascii=True).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()

def _compact_existing(state):
    if int(state.get('quality_version') or 0)>=2:return state
    removed=0
    for _,r in (state.get('assets') or {}).items():
        c=list(r.get('curve_pct') or []);f=list(r.get('front_value_today') or []);n=list(r.get('next_value_today') or [])
        m=min(len(c),len(f),len(n));nc=[];nf=[];nn=[];last=None
        for i in range(m):
            sig=(float(c[i]),float(f[i]),float(n[i]))
            if sig==last:
                removed+=1;continue
            nc.append(c[i]);nf.append(f[i]);nn.append(n[i]);last=sig
        if m:
            r['curve_pct']=nc[-REQUIRED_SAMPLES:];r['front_value_today']=nf[-REQUIRED_SAMPLES:];r['next_value_today']=nn[-REQUIRED_SAMPLES:]
            r['samples']=len(nc)
    state['samples']=max([int(x.get('samples') or 0) for x in (state.get('assets') or {}).values()] or [0])
    state['quality_version']=2;state['legacy_duplicate_samples_compacted']=removed
    state['last_market_fingerprint']=None
    return state

class MoexUniverseFeaturesV1:
    def __init__(self):
        self.enabled=True; self.live_enabled=False; self.task=None; self.last_error=None
        self.state=_compact_existing(_load(STATE,{'version':'MOEX_UNIVERSE_FEATURES_V1','started_at':time.time(),'samples':0,'last_source_ts':None,'assets':{}}))
        self._save()
    def _save(self):
        DATA.mkdir(parents=True,exist_ok=True)
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    async def refresh(self):
        try:
            source_ts,curves=_source()
            if not source_ts:return self.status()
            fp=_fingerprint(curves)
            if fp==self.state.get('last_market_fingerprint'):
                self.state['last_source_ts']=source_ts;self.state['last_unchanged_skip']=time.time();self._save();return self.status()
            assets=self.state.setdefault('assets',{})
            snap={'ts':time.time(),'source_ts':source_ts,'market_fingerprint':fp,'assets':{}}
            for c in curves:
                a=str(c.get('asset') or '')
                if not a:continue
                rec=assets.setdefault(a,{'curve_pct':[],'front_value_today':[],'next_value_today':[],'samples':0})
                for key in ('curve_pct','front_value_today','next_value_today'):
                    v=c.get(key)
                    if v is not None:
                        rec[key].append(float(v)); rec[key]=rec[key][-REQUIRED_SAMPLES:]
                rec['samples']=int(rec.get('samples') or 0)+1
                rec['front']=c.get('front');rec['next']=c.get('next');rec['front_tier']=c.get('front_tier');rec['next_tier']=c.get('next_tier')
                snap['assets'][a]={'curve_pct':c.get('curve_pct'),'front_px':c.get('front_px'),'next_px':c.get('next_px'),'front':c.get('front'),'next':c.get('next')}
            self.state['samples']=int(self.state.get('samples') or 0)+1
            self.state['last_source_ts']=source_ts;self.state['last_market_fingerprint']=fp;self.state['last_refresh']=snap['ts'];self.last_error=None
            self._save()
            with HIST.open('a',encoding='utf-8') as f:f.write(json.dumps(snap,ensure_ascii=False,separators=(',',':'))+'\n')
        except Exception as exc:self.last_error=str(exc)[:500]
        return self.status()
    def status(self):
        assets=self.state.get('assets') or {}; now=time.time(); started=float(self.state.get('started_at') or now)
        rows=[]
        for a,r in assets.items():
            s=_stats(r.get('curve_pct') or [])
            rows.append({'asset':a,'samples':int(r.get('samples') or 0),'front':r.get('front'),'next':r.get('next'),
                         'front_tier':r.get('front_tier'),'next_tier':r.get('next_tier'),'curve':s})
        rows.sort(key=lambda x:(x['samples'],abs(float((x['curve'] or {}).get('last') or 0))),reverse=True)
        gate={'collection_hours':max(0.0,(now-started)/3600.0),'required_hours':REQUIRED_HOURS,
              'samples':int(self.state.get('samples') or 0),'required_samples':REQUIRED_SAMPLES}
        gate['ready_for_rule_design']=gate['collection_hours']>=REQUIRED_HOURS and gate['samples']>=REQUIRED_SAMPLES
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FEATURE_COLLECTION_ONLY','live_enabled':False,'paper_only':True,
                'universe_assets_seen':len(rows),'samples':gate['samples'],'future_gate':gate,'assets':rows[:80],
                'quality_version':self.state.get('quality_version',2),'legacy_duplicate_samples_compacted':self.state.get('legacy_duplicate_samples_compacted',0),
                'predeclared_hypotheses':['CURVE_MEAN_REVERSION_PER_ASSET','CURVE_CONTINUATION_PER_ASSET'],
                'rule_selection_locked':not gate['ready_for_rule_design'],'last_refresh':self.state.get('last_refresh'),'last_error':self.last_error,
                'policy':{'market_fingerprint_required':True,'unchanged_market_state_skipped':True,'no_threshold_tuning_before_gate':True,
                          'no_live_orders':True,'no_grid':True,'no_martingale':True,'no_dca':True}}
    async def start(self):
        if not self.task or self.task.done():self.task=asyncio.create_task(self._loop(),name='moex-universe-features-v1')
    async def stop(self):
        if self.task and not self.task.done():self.task.cancel()
        self.task=None
    async def _loop(self):
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(INTERVAL)

moex_universe_features_v1=MoexUniverseFeaturesV1()
