from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.perp_funding_spread import perp_funding_spread_scanner,VENUES

ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'; STATE=DATA/'crossvenue_momentum_divergence_shadow_v1.json'
LOOKBACK_SEC=300; HOLD_SEC=1800; COOLDOWN_SEC=7200
MIN_LEADER_MOVE_PCT=0.35; MIN_DISPERSION_PCT=0.20; ROUND_TRIP_COST=0.0025; ALLOC=0.05
MIN_HOURS=72.0; MIN_RESOLVED=30; MIN_SYMBOLS=6

def _blank():
    now=time.time()
    return {'version':'CROSSVENUE_MOMENTUM_DIVERGENCE_V2_DYNAMIC','rule_frozen_at':now,'history':{},'last_event_ts':{},'pending':[],'resolved':[],
            'continuation_equity':100.0,'continuation_peak':100.0,'continuation_max_dd_pct':0.0,
            'snapback_equity':100.0,'snapback_peak':100.0,'snapback_max_dd_pct':0.0,'scan_count':0}

class CrossVenueMomentumDivergenceShadowV1:
    def __init__(self):
        self.enabled=True; self.live_enabled=False; self.task=None; self.last_error=None; self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8-sig'))
        except Exception:self.state=_blank()
        self.state['version']='CROSSVENUE_MOMENTUM_DIVERGENCE_V2_DYNAMIC'
    def _save(self):
        tmp=STATE.with_suffix(STATE.suffix+'.tmp');tmp.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8');tmp.replace(STATE)
    @staticmethod
    def _old(hist,target):
        for x in reversed(hist):
            if float(x[0])<=target:return x
        return None
    @staticmethod
    def _mark(mkt,base):
        vals=[float(v) for v in (mkt.get(base) or {}).values() if v]
        if not vals:return 0.0
        vals.sort();return vals[len(vals)//2]
    async def refresh(self):
        now=time.time()
        try:
            if not perp_funding_spread_scanner.market or not perp_funding_spread_scanner.last_refresh or now-float(perp_funding_spread_scanner.last_refresh)>240:
                await perp_funding_spread_scanner.refresh()
            raw_market=perp_funding_spread_scanner.market
            mkt={}
            for base,vm in raw_market.items():
                vals={}
                for venue,row in vm.items():
                    bid=float(row.get('bid') or 0);ask=float(row.get('ask') or 0)
                    if bid>0 and ask>0:vals[venue]=(bid+ask)/2.0
                if len(vals)>=2:mkt[base]=vals
            hist=self.state.setdefault('history',{})
            active_keys=set()
            for a,vm in mkt.items():
                for v,px in vm.items():
                    k=f'{a}|{v}';active_keys.add(k);h=hist.setdefault(k,[]);h.append([now,px]);hist[k]=[z for z in h if now-float(z[0])<=10800][-800:]
            for k in list(hist):
                if k not in active_keys and hist[k] and now-float(hist[k][-1][0])>21600:hist.pop(k,None)
            new=0
            for a,vm in mkt.items():
                rets={}
                for v,cur in vm.items():
                    h=hist.get(f'{a}|{v}') or [];old=self._old(h,now-LOOKBACK_SEC)
                    if old and float(old[1])>0:rets[v]=(float(cur)/float(old[1])-1.0)*100.0
                if len(rets)<2:continue
                leader=max(rets,key=lambda x:abs(rets[x]));lead=rets[leader]
                if abs(lead)<MIN_LEADER_MOVE_PCT:continue
                others=[x for v,x in rets.items() if v!=leader];lag=sum(others)/len(others) if others else 0.0;dispersion=abs(lead-lag)
                if dispersion<MIN_DISPERSION_PCT:continue
                prev=float((self.state.get('last_event_ts') or {}).get(a) or 0)
                if now-prev<COOLDOWN_SEC:continue
                entry=self._mark(mkt,a)
                if entry<=0:continue
                direction=1.0 if lead>0 else -1.0
                self.state.setdefault('pending',[]).append({'id':f'{a}:{int(now)}','base':a,'ts':now,'due_ts':now+HOLD_SEC,'entry':entry,
                    'leader':leader,'venue_returns_5m_pct':rets,'leader_return_5m_pct':lead,'lagger_mean_return_5m_pct':lag,'dispersion_pct':dispersion,
                    'continuation_side':direction,'snapback_side':-direction,'round_trip_cost':ROUND_TRIP_COST})
                self.state.setdefault('last_event_ts',{})[a]=now;new+=1
            keep=[]
            for p in self.state.get('pending') or []:
                if now<float(p.get('due_ts') or 0):keep.append(p);continue
                exit_px=self._mark(mkt,p['base'])
                if exit_px<=0:keep.append(p);continue
                ret=exit_px/float(p['entry'])-1.0;out={**p,'exit':exit_px,'exit_ts':now}
                for name in ('continuation','snapback'):
                    net=float(p[name+'_side'])*ret-ROUND_TRIP_COST;out[name+'_net_return_pct']=net*100.0
                    ek=name+'_equity';pk=name+'_peak';dk=name+'_max_dd_pct'
                    eq=float(self.state.get(ek) or 100.0)*(1.0+ALLOC*net);peak=max(float(self.state.get(pk) or 100.0),eq)
                    self.state[ek]=eq;self.state[pk]=peak;self.state[dk]=min(float(self.state.get(dk) or 0.0),(eq/peak-1.0)*100.0)
                self.state.setdefault('resolved',[]).append(out);self.state['resolved']=self.state['resolved'][-3000:]
            self.state['pending']=keep;self.state['last_new_events']=new;self.state['scan_count']=int(self.state.get('scan_count') or 0)+1;self.state['last_scan_ts']=now
            self.state['dynamic_universe_bases']=len(mkt);self.state['venue_asset_counts']=perp_funding_spread_scanner.venue_asset_counts;self._save();self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:500]
        self.last_refresh=now;return self.status()
    def status(self):
        r=self.state.get('resolved') or [];start=float(self.state.get('rule_frozen_at') or time.time());hours=max(0.0,(time.time()-start)/3600.0)
        syms=sorted({x.get('base') for x in r if x.get('base')})
        out={'ok':self.last_error is None,'strategy':'CROSSVENUE_MOMENTUM_DIVERGENCE_V2_DYNAMIC','mode':'FUTURE_ONLY_PAPER','paper_only':True,'live_enabled':False,
             'venues':VENUES,'dynamic_universe_bases':self.state.get('dynamic_universe_bases',0),'venue_asset_counts':self.state.get('venue_asset_counts') or {},
             'locked_rule':{'lookback_minutes':5,'min_leader_move_pct':MIN_LEADER_MOVE_PCT,'min_dispersion_pct':MIN_DISPERSION_PCT,'hold_minutes':30,'round_trip_cost':ROUND_TRIP_COST,'allocation_fraction':ALLOC},
             'scan_count':self.state.get('scan_count',0),'pending_count':len(self.state.get('pending') or []),'resolved_count':len(r),'last_new_events':self.state.get('last_new_events',0),
             'future_gate':{'collection_hours':hours,'required_hours':MIN_HOURS,'resolved_events':len(r),'required_resolved_events':MIN_RESOLVED,'resolved_symbols':len(syms),'required_symbols':MIN_SYMBOLS,
                            'sample_ready':bool(hours>=MIN_HOURS and len(r)>=MIN_RESOLVED and len(syms)>=MIN_SYMBOLS)},
             'promotion_eligible':False,'last_error':self.last_error,'policy':{'dynamic_perp_universe':True,'atomic_state_write':True,'no_grid':True,'no_martingale':True,'no_dca':True,'no_parameter_tuning':True,'no_live_orders':True}}
        for name in ('continuation','snapback'):
            wins=sum(1 for x in r if float(x.get(name+'_net_return_pct') or 0)>0)
            out[name]={'return_pct':float(self.state.get(name+'_equity',100.0))-100.0,'equity':self.state.get(name+'_equity',100.0),'max_dd_pct':self.state.get(name+'_max_dd_pct',0.0),
                       'wins':wins,'win_rate':wins/len(r) if r else None}
        return out
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='crossvenue-momentum-divergence-v2')
    async def stop(self):
        if self.task and not self.task.done():self.task.cancel();await asyncio.gather(self.task,return_exceptions=True)
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(30)
            if self.enabled:await self.refresh()

crossvenue_momentum_divergence_shadow_v1=CrossVenueMomentumDivergenceShadowV1()
