from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.raven_v70_regime_shadow import raven_v70_regime_shadow
from app.services.raven_v83_sparse_squeeze_shadow import raven_v83_sparse_squeeze_shadow
from app.services.raven_smart_positioning_v61_shadow import raven_smart_positioning_v61_shadow
from app.services.raven_smartpos_g2_drop_toppos_shadow import raven_smartpos_g2_drop_toppos_shadow
from app.services.aftershock_v2_shadow import aftershock_v2_shadow
from app.services.options_surface_shadow import options_surface_shadow
from app.services.perp_funding_spread import perp_funding_spread_scanner
from app.services.perp_funding_spread_paper import perp_funding_spread_paper
ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
STATE=DATA/'alpha_network_state.json'; JOURNAL=DATA/'alpha_trade_reviews.jsonl'; QUEUE=DATA/'alpha_branch_queue.json'; OPTIONS_TRACK=DATA/'options_edge_future_tracker.json'
FAMILIES={'v70':'directional_regime','v83':'sparse_squeeze','smartpos':'positioning_ls','aftershock':'liquidation_reversal','options':'volatility','funding':'carry_basis','smartpos_g2':'positioning_ls'}

class AlphaNetworkManager:
    def __init__(self):
        self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_refresh=None;self.state=self._load()
    def _load(self):
        base={'generation':1,'branches':{},'reviewed_ids':[],'branch_queue':[],'last_branch_spawn':0.,'network_history':[]}
        try:base.update(json.loads(STATE.read_text(encoding='utf-8')))
        except Exception:pass
        return base
    def _save(self):
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
        QUEUE.write_text(json.dumps(self.state.get('branch_queue') or [],ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _snap(name,status):
        eq=float(status.get('equity') or 100.0);ret=float(status.get('return_pct') or 0.0);dd=float(status.get('max_dd_pct') or 0.0)
        obs=int(status.get('observation_count') or 0);resolved=int(status.get('resolved_count') or 0)
        active=int(status.get('active_observation_count') or status.get('trade_count') or resolved or 0)
        if name in {'options','funding'}: stage='RESEARCH'
        elif obs<24 or active<3: stage='WARMUP'
        elif ret>0 and dd>-8 and (active>=8 or resolved>=8): stage='PROFIT_BRANCH'
        elif ret<-2 or dd<=-10: stage='QUARANTINE'
        else: stage='SHADOW'
        return {'name':name,'family':FAMILIES[name],'stage':stage,'ok':bool(status.get('ok')),
                'equity':round(eq,6),'return_pct':round(ret,4),'max_dd_pct':round(dd,4),'observations':obs,
                'active':active,'resolved':resolved,'win_rate':status.get('win_rate'),'live_enabled':bool(status.get('live_enabled',False))}
    def _statuses(self):
        return {'v70':raven_v70_regime_shadow.status(),'v83':raven_v83_sparse_squeeze_shadow.status(),
                'smartpos':raven_smart_positioning_v61_shadow.status(),'smartpos_g2':raven_smartpos_g2_drop_toppos_shadow.status(),'aftershock':aftershock_v2_shadow.status(),
                'options':options_surface_shadow.status(),'funding':perp_funding_spread_paper.status()}
    def _review_aftershock(self):
        reviewed=set(self.state.get('reviewed_ids') or []);new=[]
        for x in (aftershock_v2_shadow.state.get('resolved') or []):
            rid='aftershock:'+str(x.get('id'))
            if rid in reviewed:continue
            net=float(x.get('net_return_pct') or 0);row={'review_id':rid,'ts':time.time(),'branch':'aftershock','family':FAMILIES['aftershock'],
                'result':'WIN' if net>0 else 'LOSS','net_return_pct':net,'score':x.get('score'),'symbol':x.get('symbol'),
                'entry':x.get('entry'),'exit':x.get('exit'),'lesson':'KEEP_FILTER' if net>0 else 'REVIEW_FALSE_POSITIVE'}
            new.append(row);reviewed.add(rid)
        if new:
            with JOURNAL.open('a',encoding='utf-8') as f:
                for r in new:f.write(json.dumps(r,ensure_ascii=False)+'\n')
        self.state['reviewed_ids']=list(reviewed)[-5000:];return new
    def _review_external(self):
        reviewed=set(self.state.get('reviewed_ids') or []); new=[]
        for x in (perp_funding_spread_paper.state.get('closed') or []):
            rid='funding:'+str(x.get('signature'))+':'+str(int(float(x.get('closed_at') or 0)))
            if rid in reviewed: continue
            pnl=float(x.get('realized_pnl') or 0); row={'review_id':rid,'ts':time.time(),'branch':'funding','family':'carry_basis',
                'result':'WIN' if pnl>0 else 'LOSS','realized_pnl_usdt':pnl,'signature':x.get('signature'),'close_reason':x.get('close_reason'),
                'funding_realized':x.get('funding_realized'),'funding_events':x.get('funding_events'),
                'lesson':'KEEP_CARRY_FILTER' if pnl>0 else 'REVIEW_FEE_BASIS_OR_STOP'}
            new.append(row); reviewed.add(rid)
        try: opt=json.loads(OPTIONS_TRACK.read_text(encoding='utf-8'))
        except Exception: opt={}
        for x in (opt.get('resolved') or []):
            rid='options:'+str(x.get('id'))
            if rid in reviewed: continue
            ok=bool(x.get('correct')); row={'review_id':rid,'ts':time.time(),'branch':'options','family':'volatility',
                'result':'CORRECT' if ok else 'WRONG','asset':x.get('asset'),'days':x.get('days'),'state':x.get('state'),
                'surface_iv':x.get('surface_iv'),'realized_vol':x.get('realized_vol'),'lesson':'KEEP_VOL_SIGNAL' if ok else 'REVIEW_VOL_FORECAST'}
            new.append(row); reviewed.add(rid)
        if new:
            with JOURNAL.open('a',encoding='utf-8') as f:
                for r in new:f.write(json.dumps(r,ensure_ascii=False)+'\n')
        self.state['reviewed_ids']=list(reviewed)[-5000:]; return new
    def _spawn_spec(self,branches):
        now=time.time()
        if now-float(self.state.get('last_branch_spawn') or 0)<21600:return None
        eligible=[b for b in branches.values() if b['stage']=='PROFIT_BRANCH' and b['ok']]
        if not eligible:return None
        eligible.sort(key=lambda b:(b['stage']=='PROFIT_BRANCH',b['return_pct'],-abs(b['max_dd_pct'])),reverse=True);p=eligible[0]
        axes={'directional_regime':['breadth_gate','momentum_gate','risk_sleeves'],
              'sparse_squeeze':['strength_threshold','persistence_bars','overlay_scale'],
              'positioning_ls':['feature_ablation','rebalance_horizon','gross_cap'],
              'liquidation_reversal':['cascade_components','hold_hours','event_cooldown']}
        seq=axes.get(p['family'],['cost_stress','entry_gate','risk_cap']);q=list(self.state.get('branch_queue') or [])
        axis=seq[len(q)%len(seq)];bid=f"g{int(self.state.get('generation') or 1)+1}_{p['name']}_{axis}_{int(now)}"
        spec={'id':bid,'created_at':now,'parent':p['name'],'family':p['family'],'mutation_axis':axis,
              'objective':'improve_future_net_return_without_worse_drawdown','selected_from_future_metrics':True,
              'requires_causal_research':True,'requires_cost_stress':True,'requires_shadow_before_capital':True,
              'live_allowed':False,'grid':False,'martingale':False,'dca':False,'status':'RESEARCH_QUEUED'}
        q.append(spec);self.state['branch_queue']=q[-200:];self.state['generation']=int(self.state.get('generation') or 1)+1;self.state['last_branch_spawn']=now
        return spec
    async def refresh(self):
        try:
            raw=self._statuses();branches={k:self._snap(k,v) for k,v in raw.items()};reviews=self._review_aftershock()+self._review_external();spawn=self._spawn_spec(branches)
            self.state['branches']=branches;hist=list(self.state.get('network_history') or [])
            hist.append({'ts':time.time(),'branches':{k:{'stage':v['stage'],'return_pct':v['return_pct'],'max_dd_pct':v['max_dd_pct']} for k,v in branches.items()}})
            self.state['network_history']=hist[-2000:];self.last_refresh=time.time();self.last_error=None;self._save()
            return {'new_reviews':reviews,'spawned':spawn,**self.status()}
        except Exception as exc:self.last_error=str(exc)[:500];self.last_refresh=time.time();return self.status()
    def status(self):
        b=self.state.get('branches') or {};profit=[k for k,v in b.items() if v.get('stage')=='PROFIT_BRANCH'];quar=[k for k,v in b.items() if v.get('stage')=='QUARANTINE']
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_NETWORK_EVOLUTION','future_only_promotion':True,
                'live_enabled':False,'generation':int(self.state.get('generation') or 1),'branch_count':len(b),'profit_branches':profit,
                'quarantined':quar,'branches':b,'queued_children':len(self.state.get('branch_queue') or []),
                'last_queued':(self.state.get('branch_queue') or [])[-1:] or [],'trade_reviews':len(self.state.get('reviewed_ids') or []),
                'last_refresh':self.last_refresh,'last_error':self.last_error,'grid':False,'martingale':False,'dca':False}
    async def start(self):
        if self.task and not self.task.done():return
        asyncio.create_task(self.refresh(),name='initial-refresh-'+self.__class__.__name__);self.task=asyncio.create_task(self._loop(),name='alpha-network-manager')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None;self._save()
    async def _loop(self):
        while True:
            await asyncio.sleep(max(300.,self.interval));await self.refresh()

alpha_network_manager=AlphaNetworkManager()
