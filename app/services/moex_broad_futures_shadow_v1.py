from __future__ import annotations
import asyncio,json,time
from pathlib import Path
ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
UNIVERSE=DATA/'moex_futures_universe_v1.json';STATE=DATA/'moex_broad_futures_shadow_v1.json'
INTERVAL=60.0;LOOKBACK_SAMPLES=3;HOLD_SEC=1800.0;MOVE_TRIGGER_PCT=0.35;COOLDOWN_SEC=1800.0
ALL_TIERS={'CORE','LIQUID','WATCH','THIN'}
COST_BY_TIER={'CORE':0.12,'LIQUID':0.16,'WATCH':0.28,'THIN':0.60}

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

class MoexBroadFuturesShadowV1:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None
        self.state=_load(STATE,{'version':'MOEX_BROAD_FUTURES_SHADOW_V1','started_at':time.time(),'last_source_ts':None,'history':{},'pending':[],'resolved':[],'last_event_ts':{},'continuation_equity':100.0,'continuation_peak':100.0,'continuation_max_dd_pct':0.0,'reversal_equity':100.0,'reversal_peak':100.0,'reversal_max_dd_pct':0.0,'last_new_events':0})
        self.state['version']='MOEX_BROAD_FUTURES_SHADOW_V2_ALL_ACTIVE'
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _update_eq(self,kind,net):
        ek=f'{kind}_equity';pk=f'{kind}_peak';dk=f'{kind}_max_dd_pct';eq=float(self.state.get(ek) or 100.0)+float(net);peak=max(float(self.state.get(pk) or 100.0),eq);dd=(eq/peak-1.0)*100.0 if peak else 0.0
        self.state[ek]=eq;self.state[pk]=peak;self.state[dk]=min(float(self.state.get(dk) or 0.0),dd)
    def _resolve(self,pxmap,now):
        keep=[];resolved=self.state.setdefault('resolved',[])
        for p in self.state.get('pending') or []:
            px=pxmap.get(p['secid'])
            if not px or now<float(p['due_ts']):keep.append(p);continue
            raw=float(p['side'])*(px/float(p['entry'])-1.0)*100.0;net=raw-float(p.get('round_trip_cost_pct') or COST_BY_TIER.get(p.get('tier'),0.60))
            out={**p,'exit':px,'resolved_at':now,'gross_return_pct':raw,'net_return_pct':net};resolved.append(out);self._update_eq(p['kind'],net)
        self.state['pending']=keep;self.state['resolved']=resolved[-10000:]
    async def refresh(self):
        try:
            u=_load(UNIVERSE,{});last=u.get('last') or {};src=float(last.get('ts') or 0)
            if not src or src==self.state.get('last_source_ts'):return self.status()
            rows=[r for r in (last.get('contracts') or []) if str(r.get('tier') or '') in ALL_TIERS]
            now=time.time();pxmap={}
            for r in rows:
                sec=str(r.get('SECID') or '');px=_px(r)
                if sec and px:pxmap[sec]=px
            self._resolve(pxmap,now)
            hist=self.state.setdefault('history',{});new_events=0;active={(p['secid'],p['kind']) for p in (self.state.get('pending') or [])}
            tier_counts={t:0 for t in ALL_TIERS}
            for r in rows:tier_counts[str(r.get('tier'))]=tier_counts.get(str(r.get('tier')),0)+1
            for r in rows:
                sec=str(r.get('SECID') or '');px=pxmap.get(sec);tier=str(r.get('tier') or 'THIN')
                if not sec or not px:continue
                h=hist.setdefault(sec,[]);h.append({'ts':src,'px':px});hist[sec]=h[-36:];h=hist[sec]
                if len(h)<=LOOKBACK_SAMPLES:continue
                old=float(h[-1-LOOKBACK_SAMPLES]['px']);move=(px/old-1.0)*100.0 if old else 0.0
                if abs(move)<MOVE_TRIGGER_PCT:continue
                direction=1 if move>0 else -1;cost=COST_BY_TIER.get(tier,0.60)
                for kind,side in (('continuation',direction),('reversal',-direction)):
                    if (sec,kind) in active:continue
                    key=f'{sec}:{kind}';prev=float((self.state.get('last_event_ts') or {}).get(key) or 0)
                    if now-prev<COOLDOWN_SEC:continue
                    p={'id':f'{sec}:{kind}:{int(now)}','secid':sec,'asset':r.get('ASSETCODE'),'tier':tier,'kind':kind,'opened_at':now,'due_ts':now+HOLD_SEC,'entry':px,'side':side,'trigger_move_pct':move,'round_trip_cost_pct':cost,'liquidity_value_today':float(r.get('VALTODAY') or 0),'liquidity_volume_today':float(r.get('VOLTODAY') or 0)}
                    self.state.setdefault('pending',[]).append(p);self.state.setdefault('last_event_ts',{})[key]=now;active.add((sec,kind));new_events+=1
            self.state['last_source_ts']=src;self.state['last_refresh']=now;self.state['observed_contracts']=len(rows);self.state['priced_contracts']=len(pxmap);self.state['tier_counts']=tier_counts;self.state['last_new_events']=new_events;self._save();self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:500]
        return self.status()
    def status(self):
        r=self.state.get('resolved') or []
        by={k:[x for x in r if x.get('kind')==k] for k in ('continuation','reversal')}
        def stat(k):return {'resolved':len(by[k]),'wins':sum(1 for x in by[k] if float(x.get('net_return_pct') or 0)>0),'equity':round(float(self.state.get(f'{k}_equity') or 100),5),'max_dd_pct':round(float(self.state.get(f'{k}_max_dd_pct') or 0),5)}
        return {'ok':self.last_error is None,'strategy':'MOEX_BROAD_FUTURES_SHADOW_V2_ALL_ACTIVE','mode':'FUTURE_ONLY_PAPER_RESEARCH','paper_only':True,'live_enabled':False,'observed_contracts':self.state.get('observed_contracts',0),'priced_contracts':self.state.get('priced_contracts',0),'tier_counts':self.state.get('tier_counts') or {},'pending':len(self.state.get('pending') or []),'resolved':len(r),'last_new_events':self.state.get('last_new_events',0),'continuation':stat('continuation'),'reversal':stat('reversal'),'locked_rule':{'lookback_samples':LOOKBACK_SAMPLES,'move_trigger_pct':MOVE_TRIGGER_PCT,'hold_sec':HOLD_SEC,'tier_round_trip_cost_pct':COST_BY_TIER},'promotion_blocked':True,'blocking_reason':'BROKER_SPECIFIC_COSTS_DEPTH_AND_LONGER_FUTURE_ONLY_EVIDENCE_REQUIRED','last_error':self.last_error,'policy':{'all_active_forts_contracts_observed':True,'thin_contracts_high_cost_stress':True,'no_grid':True,'no_martingale':True,'no_dca':True,'no_live_orders':True,'no_parameter_tuning':True}}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='moex-broad-futures-shadow-v2')
    async def stop(self):
        if self.task and not self.task.done():self.task.cancel();await asyncio.gather(self.task,return_exceptions=True)
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(INTERVAL)
            if self.enabled:await self.refresh()

moex_broad_futures_shadow_v1=MoexBroadFuturesShadowV1()
