from __future__ import annotations
import asyncio,json,time,datetime as dt,statistics,itertools
from pathlib import Path
ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'; UNIVERSE=DATA/'moex_futures_universe_v1.json'; STATE=DATA/'moex_calendar_matrix_shadow_v1.json'
INTERVAL=60.0; WARMUP=12; ENTRY_Z=2.0; EXIT_Z=0.5; STOP_Z=4.0; HOLD_SEC=4*3600.0; RT_COST_PCT=0.10; EXPIRY_GUARD_DAYS=3
ELIGIBLE={'CORE','LIQUID','WATCH'}

def _load(p,default):
    try:return json.loads(p.read_text(encoding='utf-8-sig'))
    except Exception:return default

def _px(r):
    for k in ('LAST','SETTLEPRICE'):
        try:
            v=float(r.get(k) or 0)
            if v>0:return v
        except Exception:pass
    return None

def _date(r):
    try:return dt.date.fromisoformat(str(r.get('LASTTRADEDATE') or ''))
    except Exception:return None

class MoexCalendarMatrixShadowV1:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None
        self.state=_load(STATE,{'version':'MOEX_CALENDAR_MATRIX_SHADOW_V1','started_at':time.time(),'last_source_ts':None,'history':{},'pending':[],'resolved':[],
            'last_event_ts':{},'mean_reversion_equity':100.0,'mean_reversion_peak':100.0,'mean_reversion_max_dd_pct':0.0,
            'continuation_equity':100.0,'continuation_peak':100.0,'continuation_max_dd_pct':0.0,'last_new_events':0})
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _eq(self,kind,net):
        ek=f'{kind}_equity';pk=f'{kind}_peak';dk=f'{kind}_max_dd_pct';eq=float(self.state.get(ek) or 100)+net;peak=max(float(self.state.get(pk) or 100),eq);dd=(eq/peak-1)*100 if peak else 0
        self.state[ek]=eq;self.state[pk]=peak;self.state[dk]=min(float(self.state.get(dk) or 0),dd)
    def _pairs(self,rows):
        groups={};today=dt.date.today()
        for r in rows:
            if str(r.get('tier') or '') not in ELIGIBLE:continue
            d=_date(r);p=_px(r)
            if not d or not p or (d-today).days<=EXPIRY_GUARD_DAYS:continue
            groups.setdefault(str(r.get('ASSETCODE') or 'UNKNOWN'),[]).append((d,p,r))
        out=[]
        for asset,items in groups.items():
            items=sorted(items,key=lambda x:x[0])
            for a,b in itertools.combinations(items,2):
                fp,np=a[1],b[1]
                if fp<=0 or np<=0:continue
                out.append({'asset':asset,'front':a[2].get('SECID'),'next':b[2].get('SECID'),'front_expiry':str(a[0]),'next_expiry':str(b[0]),
                    'front_px':fp,'next_px':np,'curve_pct':(np/fp-1.0)*100.0,'front_tier':a[2].get('tier'),'next_tier':b[2].get('tier')})
        return out
    def _z(self,key,cur):
        vals=[float(x['curve']) for x in (self.state.get('history') or {}).get(key,[])]
        if len(vals)<WARMUP:return None
        m=sum(vals)/len(vals);sd=statistics.pstdev(vals)
        return (cur-m)/sd if sd>1e-12 else None
    def _resolve(self,curves,now):
        cmap={f"{c['front']}|{c['next']}":c for c in curves};keep=[];res=self.state.setdefault('resolved',[])
        for p in self.state.get('pending') or []:
            c=cmap.get(p['pair']);
            if not c:
                keep.append(p);continue
            z=self._z(p['pair'],c['curve_pct']);done=(z is not None and abs(z)<=EXIT_Z) or (z is not None and abs(z)>=STOP_Z) or now>=float(p['due_ts'])
            if not done:
                keep.append(p);continue
            raw=float(p['side'])*(float(c['curve_pct'])-float(p['entry_curve_pct']))
            net=raw-RT_COST_PCT;out={**p,'exit_curve_pct':c['curve_pct'],'exit_z':z,'resolved_at':now,'gross_return_pct':raw,'net_return_pct':net}
            res.append(out);self._eq(p['kind'],net)
        self.state['pending']=keep;self.state['resolved']=res[-5000:]
    async def refresh(self):
        try:
            u=_load(UNIVERSE,{});last=u.get('last') or {};src=float(last.get('ts') or 0)
            if not src or src==self.state.get('last_source_ts'):return self.status()
            curves=self._pairs(last.get('contracts') or []);now=time.time();self._resolve(curves,now)
            active={(p['pair'],p['kind']) for p in self.state.get('pending') or []};new=0;hist=self.state.setdefault('history',{})
            for c in curves:
                key=f"{c['front']}|{c['next']}";z=self._z(key,float(c['curve_pct']))
                h=hist.setdefault(key,[]);h.append({'ts':src,'curve':float(c['curve_pct'])});hist[key]=h[-72:]
                if z is None or abs(z)<ENTRY_Z:continue
                direction=-1 if z>0 else 1
                for kind,side in (('mean_reversion',direction),('continuation',-direction)):
                    if (key,kind) in active:continue
                    lk=f'{key}:{kind}';prev=float((self.state.get('last_event_ts') or {}).get(lk) or 0)
                    if now-prev<3600:continue
                    p={'id':f'{key}:{kind}:{int(now)}','pair':key,'asset':c['asset'],'front':c['front'],'next':c['next'],'kind':kind,'side':side,
                       'opened_at':now,'due_ts':now+HOLD_SEC,'entry_curve_pct':c['curve_pct'],'entry_z':z,'round_trip_cost_pct':RT_COST_PCT}
                    self.state.setdefault('pending',[]).append(p);self.state.setdefault('last_event_ts',{})[lk]=now;active.add((key,kind));new+=1
            self.state['last_source_ts']=src;self.state['last_refresh']=now;self.state['eligible_pairs']=len(curves);self.state['last_new_events']=new;self._save();self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:500]
        return self.status()
    def status(self):
        r=self.state.get('resolved') or []
        def stat(k):
            x=[a for a in r if a.get('kind')==k];return {'resolved':len(x),'wins':sum(1 for a in x if float(a.get('net_return_pct') or 0)>0),'equity':round(float(self.state.get(f'{k}_equity') or 100),5),'max_dd_pct':round(float(self.state.get(f'{k}_max_dd_pct') or 0),5)}
        return {'ok':self.last_error is None,'strategy':'MOEX_CALENDAR_MATRIX_SHADOW_V1','mode':'FUTURE_ONLY_PAPER_RESEARCH','paper_only':True,'live_enabled':False,
            'eligible_pairs':self.state.get('eligible_pairs',0),'pending':len(self.state.get('pending') or []),'resolved':len(r),'last_new_events':self.state.get('last_new_events',0),
            'mean_reversion':stat('mean_reversion'),'continuation':stat('continuation'),
            'locked_rule':{'warmup_samples':WARMUP,'entry_abs_z':ENTRY_Z,'exit_abs_z':EXIT_Z,'stop_abs_z':STOP_Z,'hold_sec':HOLD_SEC,'round_trip_cost_pct':RT_COST_PCT,'expiry_guard_days':EXPIRY_GUARD_DAYS},
            'promotion_blocked':True,'blocking_reason':'BROKER_SPECIFIC_MOEX_COSTS_AND_LONGER_FUTURE_ONLY_EVIDENCE_REQUIRED','last_error':self.last_error,
            'policy':{'all_pair_combinations_within_asset':True,'expiry_guard':True,'no_grid':True,'no_martingale':True,'no_dca':True,'no_live_orders':True,'no_parameter_tuning':True}}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='moex-calendar-matrix-shadow-v1')
    async def stop(self):
        if self.task and not self.task.done():self.task.cancel();await asyncio.gather(self.task,return_exceptions=True)
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(INTERVAL)
            if self.enabled:await self.refresh()

moex_calendar_matrix_shadow_v1=MoexCalendarMatrixShadowV1()
