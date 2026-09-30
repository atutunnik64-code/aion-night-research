from __future__ import annotations
import asyncio,json,time
from pathlib import Path

ROOT=Path(__file__).parents[2];DATA=ROOT/'data'
HIST=DATA/'options_perp_hedge_history.jsonl';STATE=DATA/'options_gamma_rebalance_shadow_v1.json'
BANDS=[.015,.03,.06];COST_RATE=.0007
MIN_HOURS=48.;MIN_SNAPSHOTS=96;MIN_BASE_REBALANCES=12

def _blank():
    now=time.time();return {'version':'OPTIONS_GAMMA_REBALANCE_SHADOW_V1','rule_frozen_at':now,'last_ts':now,
        'snapshot_count':0,'package_ids':[],'gamma_observations':0,'positive_gamma_obs':0,'negative_gamma_obs':0,
        'sum_abs_gamma':0.0,'policies':{str(b):{'held':{},'rebalances':0,'cost_usdt':0.0,'residual_sum':0.0,
        'residual_max':0.0,'observations':0} for b in BANDS}}

def _rows():
    if not HIST.exists():return []
    out=[]
    for line in HIST.read_text(encoding='utf-8').splitlines()[-3000:]:
        try:out.append(json.loads(line))
        except Exception:pass
    return sorted(out,key=lambda x:float(x.get('ts') or 0))

class OptionsGammaRebalanceShadow:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:self.state=_blank()
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _process(self,rows):
        last=float(self.state.get('last_ts') or self.state.get('rule_frozen_at') or 0);max_ts=last;pk=set(self.state.get('package_ids') or [])
        for snap in rows:
            ts=float(snap.get('ts') or 0);max_ts=max(max_ts,ts)
            if ts<=last or ts<float(self.state.get('rule_frozen_at') or 0):continue
            self.state['snapshot_count']=int(self.state.get('snapshot_count') or 0)+1
            for pos in snap.get('positions') or []:
                pid=f"{pos.get('source')}|{pos.get('position_id')}";pk.add(pid)
                for asset,t in (pos.get('targets') or {}).items():
                    if 'option_net_gamma' not in t:continue
                    gamma=float(t.get('option_net_gamma') or 0);desired=float(t.get('target_base_units') or 0)
                    mark=float(((t.get('perp') or {}).get('mark')) or 0);key=f'{pid}|{asset}'
                    self.state['gamma_observations']=int(self.state.get('gamma_observations') or 0)+1
                    self.state['positive_gamma_obs']=int(self.state.get('positive_gamma_obs') or 0)+int(gamma>0)
                    self.state['negative_gamma_obs']=int(self.state.get('negative_gamma_obs') or 0)+int(gamma<0)
                    self.state['sum_abs_gamma']=float(self.state.get('sum_abs_gamma') or 0)+abs(gamma)
                    for b in BANDS:
                        z=self.state['policies'].setdefault(str(b),{'held':{},'rebalances':0,'cost_usdt':0.0,'residual_sum':0.0,'residual_max':0.0,'observations':0})
                        held=float((z.get('held') or {}).get(key) or 0);diff=desired-held
                        if abs(diff)>b:
                            z['rebalances']=int(z.get('rebalances') or 0)+1;z['cost_usdt']=float(z.get('cost_usdt') or 0)+abs(diff)*mark*COST_RATE;held=desired
                        residual=abs(held-desired);z.setdefault('held',{})[key]=held;z['observations']=int(z.get('observations') or 0)+1
                        z['residual_sum']=float(z.get('residual_sum') or 0)+residual;z['residual_max']=max(float(z.get('residual_max') or 0),residual)
        self.state['last_ts']=max_ts;self.state['package_ids']=sorted(pk);return max_ts
    def _refresh_sync(self):
        rows=_rows();self._process(rows);self._save();return rows
    async def refresh(self):
        try:rows=await asyncio.to_thread(self._refresh_sync);self.last_error=None
        except Exception as exc:rows=[];self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status(rows)
    def status(self,rows=None):
        end=float(self.state.get('last_ts') or self.state.get('rule_frozen_at') or time.time());start=float(self.state.get('rule_frozen_at') or end)
        hours=max(0.0,(end-start)/3600);pol={}
        for b in BANDS:
            z=self.state.get('policies',{}).get(str(b),{});obs=int(z.get('observations') or 0)
            pol[str(b)]={'delta_band':b,'rebalances':int(z.get('rebalances') or 0),'estimated_cost_usdt':float(z.get('cost_usdt') or 0),
                'avg_abs_residual_delta':(float(z.get('residual_sum') or 0)/obs if obs else None),'max_abs_residual_delta':float(z.get('residual_max') or 0),'observations':obs}
        base=pol[str(.03)];gate={'collection_hours':hours,'required_hours':MIN_HOURS,'snapshots':int(self.state.get('snapshot_count') or 0),
            'required_snapshots':MIN_SNAPSHOTS,'baseline_rebalances':base['rebalances'],'required_baseline_rebalances':MIN_BASE_REBALANCES,
            'ready_for_review':bool(hours>=MIN_HOURS and int(self.state.get('snapshot_count') or 0)>=MIN_SNAPSHOTS and base['rebalances']>=MIN_BASE_REBALANCES)}
        n=int(self.state.get('gamma_observations') or 0)
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURE_ONLY_GAMMA_REBALANCE_EVIDENCE','strategy':'OPTIONS_PERP_GAMMA_REBALANCE_V1',
            'live_enabled':False,'paper_only':True,'rule_frozen_at':self.state.get('rule_frozen_at'),'future_gate':gate,
            'gamma_observations':n,'positive_gamma_obs':int(self.state.get('positive_gamma_obs') or 0),'negative_gamma_obs':int(self.state.get('negative_gamma_obs') or 0),
            'mean_abs_gamma':(float(self.state.get('sum_abs_gamma') or 0)/n if n else None),'package_count':len(self.state.get('package_ids') or []),
            'policies':pol,'promotion_eligible':False,'last_refresh':self.last_refresh,'last_error':self.last_error,
            'policy':{'predeclared_delta_bands':BANDS,'fee_plus_slippage_rate':COST_RATE,'no_live_orders':True,'no_historical_selection':True,'live_requires_separate_review':True}}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='options-gamma-rebalance-shadow')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(900)
            if self.enabled:await self.refresh()

options_gamma_rebalance_shadow=OptionsGammaRebalanceShadow()
