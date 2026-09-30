from __future__ import annotations
import json,time,statistics
from pathlib import Path
ROOT=Path(__file__).parents[2]; STATE=ROOT/'data'/'latency_leadlag_tracker_v2.json'
MIN_LAG_MS=250.0; MAX_SLOW_AGE_MS=120000.0; MAX_RAW_NET_PCT=5.0

def _load():
    try:return json.loads(STATE.read_text(encoding='utf-8'))
    except Exception:return {'version':'LATENCY_LEADLAG_TRACKER_V2','started_at':time.time(),'pairs':{},'samples':0,'rejected':{}}

def _quality(z):
    reasons=[]
    if not z.get('identity_verified'): reasons.append('IDENTITY')
    if not z.get('latency_evidence'): reasons.append('NO_LAG_EVIDENCE')
    if float(z.get('observed_lag_ms') or 0)<MIN_LAG_MS: reasons.append('LAG_TOO_SMALL')
    if float(z.get('slow_quote_age_ms') or 0)>MAX_SLOW_AGE_MS: reasons.append('STALE_SLOW_QUOTE')
    raw=float(z.get('net_pct') or -999)
    if raw<=0 or raw>MAX_RAW_NET_PCT: reasons.append('RAW_SPREAD_IMPLAUSIBLE')
    return not reasons,reasons
class LatencyLeadLagTrackerV2:
    def __init__(self): self.state=_load(); self.last_error=None
    def _save(self): STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def observe(self,rows):
        now=time.time(); self.state['samples']=int(self.state.get('samples') or 0)+1; pairs=self.state.setdefault('pairs',{}); rej=self.state.setdefault('rejected',{})
        for z in rows:
            ok,reasons=_quality(z); sig=str(z.get('signature') or '')
            if not sig: continue
            if not ok:
                for x in reasons: rej[x]=int(rej.get(x) or 0)+1
                continue
            r=pairs.setdefault(sig,{'hits':0,'positive_batched':0,'lags_ms':[],'batched_net_pct':[]})
            r['hits']+=1; r['last_seen']=now; r['slow_venue']=z.get('slow_venue'); r['fast_venue']=z.get('fast_venue'); r['base']=z.get('base'); r['quote']=z.get('quote'); r['direction']=z.get('direction')
            lag=float(z.get('observed_lag_ms') or 0); bn=float(z.get('all_in_batched_net_pct') or -999)
            r['lags_ms']=(r.get('lags_ms') or [])[-199:]+[lag]; r['batched_net_pct']=(r.get('batched_net_pct') or [])[-199:]+[bn]
            if bn>0:r['positive_batched']+=1
        self._save(); return self.status()
    def status(self):
        out=[]
        for sig,r in (self.state.get('pairs') or {}).items():
            vals=r.get('batched_net_pct') or []; lags=r.get('lags_ms') or []; hits=int(r.get('hits') or 0)
            out.append({'signature':sig,'hits':hits,'positive_rate':round(int(r.get('positive_batched') or 0)/hits,4) if hits else 0,'avg_batched_net_pct':round(sum(vals)/len(vals),5) if vals else None,'median_lag_ms':round(statistics.median(lags),1) if lags else None,'slow_venue':r.get('slow_venue'),'fast_venue':r.get('fast_venue'),'base':r.get('base'),'quote':r.get('quote'),'direction':r.get('direction')})
        out.sort(key=lambda x:(x['positive_rate'],x['hits'],x['avg_batched_net_pct'] or -999),reverse=True)
        return {'ok':True,'mode':'FUTURE_ONLY_LEAD_LAG_V2','paper_only':True,'live_enabled':False,'samples':self.state.get('samples',0),'pairs':len(out),'top_pairs':out[:50],'rejected':self.state.get('rejected') or {},'quality_gate':{'min_lag_ms':MIN_LAG_MS,'max_slow_quote_age_ms':MAX_SLOW_AGE_MS,'max_raw_net_pct':MAX_RAW_NET_PCT},'policy':{'qualified_only':True,'all_in_batched_net_required':True,'no_live_orders':True,'no_grid':True,'no_martingale':True,'no_dca':True}}
latency_leadlag_tracker_v2=LatencyLeadLagTrackerV2()
