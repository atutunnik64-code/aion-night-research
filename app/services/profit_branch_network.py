from __future__ import annotations
import asyncio,json,time,hashlib
from pathlib import Path
from app.services.raven_v70_regime_shadow import raven_v70_regime_shadow
from app.services.raven_v42_funding_shadow import raven_v42_funding_shadow
from app.services.raven_v76_robust_short_veto_shadow import raven_v76_robust_short_veto_shadow
from app.services.raven_v83_sparse_squeeze_shadow import raven_v83_sparse_squeeze_shadow
from app.services.raven_smart_positioning_v61_shadow import raven_smart_positioning_v61_shadow
from app.services.raven_smartpos_g2_drop_toppos_shadow import raven_smartpos_g2_drop_toppos_shadow
from app.services.raven_v63_microstructure_shadow_v2 import raven_v63_microstructure_shadow_v2
from app.services.raven_v64_short_veto_shadow import raven_v64_short_veto_shadow
from app.services.aftershock_v2_shadow import aftershock_v2_shadow
from app.services.options_surface_shadow import options_surface_shadow
from app.services.options_ivrv_package_shadow import options_ivrv_package_shadow
from app.services.options_relative_vol_shadow import options_relative_vol_shadow
from app.services.options_term_structure_shadow import options_term_structure_shadow
from app.services.options_skew_shadow import options_skew_shadow
from app.services.perp_funding_spread_paper import perp_funding_spread_paper
from app.services.raven_v70_variant_pool import raven_v70_variant_pool
from app.services.raven_v83_variant_pool import raven_v83_variant_pool
from app.services.raven_v94_bybit_short_sizing_shadow import raven_v94_bybit_short_sizing_shadow
from app.services.raven_v98_crowding_funding_shadow import raven_v98_crowding_funding_shadow
from app.services.raven_v102_sparse_cash_filler_shadow import raven_v102_sparse_cash_filler_shadow
from app.services.raven_v103_regime_composite_shadow import raven_v103_regime_composite_shadow
from app.services.raven_v105_bk_finite_arbitrage_shadow import raven_v105_bk_finite_arbitrage_shadow
from app.services.raven_v79_cash_aware_ensemble_shadow import raven_v79_cash_aware_ensemble_shadow
from app.services.raven_v80_slow_micro_shadow import raven_v80_slow_micro_shadow
from app.services.crowding_dislocation_v2_shadow import crowding_dislocation_v2_shadow
from app.services.idio_vol_rotation_shadow import idio_vol_rotation_shadow
from app.services.idio_skew_rotation_shadow import idio_skew_rotation_shadow
from app.services.liquidity_migration_perp_shadow import liquidity_migration_perp_shadow
from app.services.funding_oi_state_transition_shadow import funding_oi_state_transition_shadow
from app.services.crossvenue_oi_migration_shadow_v2 import crossvenue_oi_migration_shadow_v2
from app.services.crossvenue_taker_imbalance_divergence_shadow_v3 import crossvenue_taker_imbalance_divergence_shadow_v3
from app.services.moex_futures_shadow import moex_futures_shadow
from app.services.futures_extension import futures_extension_policy
from app.services.futures_forward_worker import futures_forward_worker

ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
STATE=DATA/'profit_branch_network.json'; EVENTS=DATA/'profit_branch_events.jsonl'; VARIANTS=DATA/'profit_branch_variants.json'; OPTIONS_TRACK=DATA/'options_edge_future_tracker.json'

BRANCHES={
 'v70':('directional',raven_v70_regime_shadow),
 'v42':('funding_confirmation',raven_v42_funding_shadow),
 'v76':('directional_veto',raven_v76_robust_short_veto_shadow),
 'v83':('cash_squeeze',raven_v83_sparse_squeeze_shadow),
 'smartpos':('market_neutral_positioning',raven_smart_positioning_v61_shadow),
 'smartpos_g2':('market_neutral_positioning_child',raven_smartpos_g2_drop_toppos_shadow),
 'v63_2':('microstructure',raven_v63_microstructure_shadow_v2),
 'v64':('directional_veto',raven_v64_short_veto_shadow),
 'aftershock_v2':('liquidation_aftershock',aftershock_v2_shadow),
 'options_v2':('options_volatility',options_surface_shadow),
 'options_ivrv':('options_volatility',options_ivrv_package_shadow),
 'options_relvol':('options_volatility',options_relative_vol_shadow),
 'options_term':('options_volatility',options_term_structure_shadow),
 'options_skew':('options_volatility',options_skew_shadow),
 'funding_spread':('funding_basis',perp_funding_spread_paper),
 'v94':('directional_crowding',raven_v94_bybit_short_sizing_shadow),
 'v98':('directional_crowding',raven_v98_crowding_funding_shadow),
 'v102':('cash_squeeze_child',raven_v102_sparse_cash_filler_shadow),
 'v103':('regime_composite',raven_v103_regime_composite_shadow),
 'v105':('finite_arbitrage',raven_v105_bk_finite_arbitrage_shadow),
 'v79':('positioning_ensemble',raven_v79_cash_aware_ensemble_shadow),
 'v80':('slow_microstructure',raven_v80_slow_micro_shadow),
 'crowding_dislocation':('market_neutral_crowding_dislocation',crowding_dislocation_v2_shadow),
 'idio_vol_rotation':('market_neutral_idio_vol',idio_vol_rotation_shadow),
 'idio_skew_rotation':('market_neutral_idio_skew',idio_skew_rotation_shadow),
 'liquidity_migration':('perp_liquidity_structure',liquidity_migration_perp_shadow),
 'funding_oi_follow':('funding_oi_state_follow',funding_oi_state_transition_shadow),
 'funding_oi_fade':('funding_oi_state_fade',funding_oi_state_transition_shadow),
 'oi_migration_follow':('crossvenue_oi_migration_follow',crossvenue_oi_migration_shadow_v2),
 'oi_migration_fade':('crossvenue_oi_migration_fade',crossvenue_oi_migration_shadow_v2),
 'taker_binance':('crossvenue_taker_binance',crossvenue_taker_imbalance_divergence_shadow_v3),
 'taker_bybit':('crossvenue_taker_bybit',crossvenue_taker_imbalance_divergence_shadow_v3),
 'moex_structure':('moex_futures_structure',moex_futures_shadow),
}
LINEAGE={'v42':'v70','v64':'v70','options_skew':'options_v2','options_term':'options_v2','v76':'v70','v98':'v70','options_relvol':'options_v2','options_ivrv':'options_v2','smartpos_g2':'smartpos','v94':'v70','v102':'v83','v103':'v70','v79':'smartpos'}

class ProfitBranchNetwork:
    def __init__(self):
        self.enabled=True; self.interval=900.0; self.task=None; self.last_error=None
        self.state=self._load(); self.latest={}
    def _load(self):
        base={'generation':1,'branches':{},'event_ids':[],'last_refresh':None,'last_variant_cycle':{}}
        try:
            old=json.loads(STATE.read_text(encoding='utf-8'))
            if isinstance(old,dict):base.update(old)
        except Exception:pass
        return base
    def _save(self):
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _num(x,*keys,default=0.0):
        for k in keys:
            cur=x
            try:
                for part in k.split('.'):cur=cur.get(part) if isinstance(cur,dict) else None
                if cur is not None:return float(cur)
            except Exception:pass
        return float(default)
    @staticmethod
    def _int(x,*keys,default=0):
        return int(ProfitBranchNetwork._num(x,*keys,default=default))
    def _canonical_status(self,name,s):
        if name=='moex_structure':
            g=dict(s.get('future_gate') or {}); n=int(s.get('sample_count') or 0)
            return {'ok':bool(s.get('ok',True)),'mode':s.get('mode'),'strategy':'MOEX_METALS_SBER_STRUCTURE_V1',
                    'future_only':True,'live_enabled':False,'grid':False,'martingale':False,'dca':False,
                    'equity':100.0,'return_pct':0.0,'max_dd_pct':0.0,'observation_count':n,'resolved_count':0,
                    'future_gate':g,'futures_evaluated':True,'futures_promotion_eligible':False,
                    'latest':{'bar_ts':(s.get('latest') or {}).get('source_ts'),'source':'MOEX_ISS_DELAYED_RESEARCH'},
                    'locked_config':{'baseline_samples':288,'required_hours':24.0,'paper_only':True}}
        if name in {'oi_migration_follow','oi_migration_fade'}:
            side='follow' if name.endswith('follow') else 'fade'; x=dict(s.get(side) or {}); g=dict(s.get('future_gate') or {})
            return {'ok':bool(s.get('ok',True)),'mode':s.get('mode'),'strategy':name,'future_only':True,'live_enabled':False,'grid':False,'martingale':False,'dca':False,
                    'equity':float(x.get('equity') or 100.0),'return_pct':float(x.get('return_pct') or 0.0),'max_dd_pct':float(x.get('max_dd_pct') or 0.0),
                    'resolved_count':int(s.get('resolved_count') or 0),'wins':int(x.get('wins') or 0),'win_rate':x.get('win_rate'),'future_gate':g,
                    'futures_evaluated':True,'futures_promotion_eligible':bool(g.get('ready_for_review')),
                    'latest':{'bar_ts':s.get('last_refresh'),'source':'EXACT_CROSSVENUE_OI_FUTURE_ONLY'},'recent_resolved':[dict(r,net_return_pct=r.get(side+'_net_return_pct')) for r in (s.get('recent_resolved') or []) if isinstance(r,dict)]}
        if name in {'taker_binance','taker_bybit'}:
            side='binance' if name.endswith('binance') else 'bybit'; x=dict(s.get(side) or {}); g=dict(s.get('future_gate') or {})
            return {'ok':bool(s.get('ok',True)),'mode':s.get('mode'),'strategy':name,'future_only':True,'live_enabled':False,'grid':False,'martingale':False,'dca':False,
                    'equity':float(x.get('equity') or 100.0),'return_pct':float(x.get('return_pct') or 0.0),'max_dd_pct':float(x.get('max_dd_pct') or 0.0),
                    'resolved_count':int(s.get('resolved_count') or 0),'wins':int(x.get('wins') or 0),'win_rate':x.get('win_rate'),'future_gate':g,
                    'futures_evaluated':True,'futures_promotion_eligible':bool(g.get('ready_for_review')),
                    'latest':{'bar_ts':s.get('last_refresh'),'source':'EXACT_CROSSVENUE_TAKER_FUTURE_ONLY'},'recent_resolved':[dict(r,net_return_pct=r.get(side+'_net_return_pct')) for r in (s.get('recent_resolved') or []) if isinstance(r,dict)]}
        if name in {'funding_oi_follow','funding_oi_fade'}:
            side='follow' if name.endswith('follow') else 'fade'
            x=dict(s.get(side) or {}); g=dict(s.get('future_gate') or {})
            ready=bool((g.get('hypothesis_ready') or {}).get(side))
            return {'ok':bool(s.get('ok',True)),'mode':s.get('mode'),'strategy':name,'future_only':True,
                    'live_enabled':False,'grid':False,'martingale':False,'dca':False,
                    'equity':float(x.get('equity') or 100.0),'return_pct':float(x.get('return_pct') or 0.0),
                    'max_dd_pct':float(x.get('max_dd_pct') or 0.0),'resolved_count':int(s.get('resolved_count') or 0),
                    'wins':int(x.get('wins') or 0),'win_rate':x.get('win_rate'),'future_gate':g,
                    'futures_evaluated':True,'futures_promotion_eligible':ready,
                    'latest':{'bar_ts':s.get('last_refresh'),'source':'EXACT_BINANCE_PERP_OI_FUNDING_FUTURE_ONLY'},
                    'recent_resolved':[dict(r,net_return_pct=r.get(side+'_net_return_pct')) for r in (s.get('recent_resolved') or []) if isinstance(r,dict)]}
        exact={'v70':'baseline','v76':'v76','v94':'v94','v98':'v98','v103':'v103'}
        if name not in set(exact)|{'v42'}:return s
        fw=futures_forward_worker.status()
        if name=='v42':
            x=fw.get('v42_tracker') or {};g=x.get('gate') or {};ret=float(x.get('candidate_return_pct') or 0);dd=float(x.get('candidate_max_dd_pct') or 0)
            return {'ok':bool(fw.get('ok')),'mode':'FUTURES_FORWARD_RESEARCH_ONLY','strategy':'v42_funding_confirmation','future_only':True,'live_enabled':False,'grid':False,'martingale':False,'dca':False,'equity':100*(1+ret/100),'return_pct':ret,'max_dd_pct':dd,'observation_count':int(x.get('observations') or 0),'future_gate':g,'latest':{'bar_ts':x.get('data_end'),'source':'EXACT_BINANCE_PERP_PLUS_BINANCE_BYBIT_FUNDING'}}
        tr=fw.get('tracker') or {};mode=exact[name];vs=(tr.get('strategies') or {}).get(mode) or {}
        g=(tr.get('directional_gate') or {}) if name=='v103' else ((tr.get('mode_gates') or {}).get(mode) or {})
        ret=float(vs.get('return_pct') or 0);dd=float(vs.get('max_dd_pct') or 0)
        out={'ok':bool(fw.get('ok')),'mode':'FUTURES_FORWARD_RESEARCH_ONLY','strategy':name,'future_only':True,'live_enabled':False,'grid':False,'martingale':False,'dca':False,'equity':100*(1+ret/100),'return_pct':ret,'max_dd_pct':dd,'observation_count':int(vs.get('bars') or 0),'latest':{'bar_ts':tr.get('data_end'),'source':'EXACT_BINANCE_PERP_FORWARD','mode':mode}}
        if name=='v103':out['future_validation_gate']=g
        else:out['future_gate']=g
        return out

    def _normalize(self,name,family,s):
        latest=s.get('latest') or {}
        eq=self._num(s,'equity','latest.research_equity','latest.meta_equity',default=100.0)
        ret=self._num(s,'return_pct','latest.research_return_pct','latest.meta_return_pct',default=(eq/100-1)*100)
        dd=self._num(s,'max_dd_pct','latest.max_dd_pct',default=0.0)
        obs=self._int(s,'observation_count','sample_count','decision_count','latest.closed_count','latest.observation_count')
        resolved=self._int(s,'resolved_count','resolved','latest.closed_count','edge_tracker.resolved')
        wins=self._int(s,'wins','edge_tracker.correct')
        win_rate=s.get('win_rate')
        if win_rate is None and resolved>0:win_rate=wins/resolved
        forbidden=bool(s.get('grid') or s.get('martingale') or s.get('dca'))
        future=bool(s.get('future_only')) or family in {'funding_basis'}
        live=bool(s.get('live_enabled')); gate=s.get('future_short_gate') or s.get('future_gate') or s.get('future_validation_gate') or {}
        hs=s.get('horizon_stats') or {}; hv=[float(v.get('mean_net_signal_pct') or 0) for v in hs.values() if isinstance(v,dict) and int(v.get('resolved') or 0)>=20]
        negative_net_horizons=bool(hv) and all(v<0 for v in hv)
        fut=futures_extension_policy.profile(name,family,s)
        return {'name':name,'family':family,'ok':bool(s.get('ok',True)),'mode':s.get('mode'),
                'future_only':future,'live_enabled':live,'forbidden_style':forbidden,
                'futures':fut,'futures_required':True,'futures_promotion_ready':bool(fut.get('promotion_ready')),
                'live_promotion_blocked_by_futures':not bool(fut.get('promotion_ready')),
                'equity':round(eq,6),'return_pct':round(ret,6),'max_dd_pct':round(dd,6),
                'observations':obs,'resolved':resolved,'wins':wins,'win_rate':win_rate,
                'raw_strategy':s.get('strategy'),'evidence_source':latest.get('source'),'evidence_bar_ts':latest.get('bar_ts'),'locked_config':s.get('locked_config') or {},'allocator_eligible':s.get('allocator_eligible'),
                'capital_gate':s.get('capital_gate'),'negative_net_horizons':negative_net_horizons,'external_gate_required':bool(gate),
                'external_gate_ready':bool(gate.get('ready') or gate.get('ready_for_review')) if isinstance(gate,dict) and gate else True,'ts':time.time()}

    @staticmethod
    def _health_action(b):
        h=dict(b.get('health') or {}); n=int(h.get('trades') or 0)
        wr=h.get('win_rate'); streak=int(h.get('streak') or 0)
        mean=float(h.get('mean_net_pct') or 0.0); recent=float(h.get('recent5_mean_net_pct') or 0.0)
        degrading=bool(h.get('degrading'))
        severe=bool(n>=10 and (streak<=-5 or (wr is not None and float(wr)<=.25 and mean<0) or (degrading and recent<0)))
        warn=bool(n>=8 and (streak<=-3 or degrading or (wr is not None and float(wr)<.40 and mean<0)))
        return 'QUARANTINE' if severe else ('WATCH' if warn else 'NORMAL')

    def _health_action_stateful(self,name,b):
        raw=self._health_action(b); h=dict(b.get('health') or {})
        n=int(b.get('resolved') or h.get('trades') or 0); wr=h.get('win_rate'); recent=float(h.get('recent5_mean_net_pct') or 0.0)
        controls=dict(self.state.get('health_controls') or {}); c=dict(controls.get(name) or {})
        prev=str(c.get('action') or 'NORMAL'); entered=int(c.get('entered_resolved') or n); action=raw
        if prev=='QUARANTINE' and raw!='QUARANTINE':
            recovered=(n-entered)>=5 and wr is not None and float(wr)>=.50 and recent>0 and not bool(h.get('degrading'))
            action='NORMAL' if recovered else 'QUARANTINE'
        elif prev=='WATCH' and raw=='NORMAL':
            recovered=(n-entered)>=2 and recent>=0 and int(h.get('streak') or 0)>-2
            action='NORMAL' if recovered else 'WATCH'
        if action!=prev:
            entered=n
            self._event(name,'HEALTH_STATE_CHANGE',{'from':prev,'to':action,'resolved':n,'raw':raw,'health':h})
        controls[name]={'action':action,'entered_resolved':entered,'raw_action':raw,'updated_at':time.time()}
        self.state['health_controls']=controls
        return action

    @staticmethod
    def _stage(b):
        if b.get('health_action')=='QUARANTINE': return 'QUARANTINE_HEALTH'
        if not b['ok'] or b['live_enabled'] or b['forbidden_style']:return 'BLOCKED'
        if str(b.get('family') or '').startswith('moex_') and b.get('external_gate_required') and not b.get('external_gate_ready'):return 'SHADOW_GATE'
        if b.get('futures_required') and not b.get('futures_promotion_ready'):return 'FUTURES_GATE'
        if b.get('external_gate_required') and not b.get('external_gate_ready'):return 'SHADOW_GATE'
        if b.get('family')=='perp_liquidity_structure' and b.get('external_gate_ready') and b.get('max_dd_pct',0)>=-8:return 'PORTFOLIO_ELIGIBLE'
        if b['family']=='microstructure' and max(b['observations'],b['resolved'])>=100 and b.get('allocator_eligible') is False and b.get('negative_net_horizons'):return 'QUARANTINE'
        if b['family']=='options_volatility':
            return 'VALIDATED_SIGNAL' if b['resolved']>=20 and (b['win_rate'] or 0)>=.60 else 'SHADOW_SIGNAL'
        n=max(b['observations'],b['resolved'])
        wr=b.get('win_rate')
        if n>=50 and b['return_pct']<=-0.25 and wr is not None and float(wr)<.35:return 'QUARANTINE'
        if n<8:return 'SEED_SHADOW'
        if b['return_pct']<=0:return 'SHADOW'
        if b['max_dd_pct']<-10:return 'SHADOW_RISK_FAIL'
        if n>=48 and b['max_dd_pct']>=-8:return 'PORTFOLIO_ELIGIBLE'
        return 'PROFIT_BRANCH'
    @staticmethod
    def _score(b):
        n=max(b['observations'],b['resolved']); conf=min(1.0,n/48.0)
        quality=b['return_pct']-0.75*abs(min(0.0,b['max_dd_pct']))
        if b['win_rate'] is not None:quality+=4.0*(float(b['win_rate'])-.5)
        return round(conf*quality,6)
    def _event(self,branch,kind,payload):
        raw=json.dumps({'branch':branch,'kind':kind,'payload':payload},sort_keys=True,ensure_ascii=False)
        eid=hashlib.sha1(raw.encode()).hexdigest()[:16]; seen=set(self.state.get('event_ids') or [])
        if eid in seen:return
        row={'id':eid,'ts':time.time(),'branch':branch,'kind':kind,'payload':payload}
        with EVENTS.open('a',encoding='utf-8') as f:f.write(json.dumps(row,ensure_ascii=False)+'\n')
        ids=list(seen);ids.append(eid);self.state['event_ids']=ids[-5000:]
    def _postmortems(self,name,s,b,prev):
        oldeq=float((prev or {}).get('equity') or b['equity'])
        if abs(b['equity']-oldeq)>1e-9:
            delta=b['equity']-oldeq
            self._event(name,'EQUITY_STEP',{'delta':round(delta,8),'equity':b['equity'],
                'result':'GAIN' if delta>0 else 'LOSS','stage':b['stage'],'score':b['score']})
        rows=list(s.get('recent_resolved') or [])+list((s.get('latest') or {}).get('recent_closed') or [])
        for r in rows[-20:]:
            self._event(name,'CLOSED_TRADE',r)
    @staticmethod
    def _branch_health(s):
        rows=list(s.get('recent_resolved') or [])+list((s.get('latest') or {}).get('recent_closed') or [])
        rows=rows[-20:]; vals=[]; symbols=[]
        for r in rows:
            if not isinstance(r,dict): continue
            v=None
            for k in ('net_return_pct','follow_net_return_pct','binance_net_return_pct','continuation_net_return_pct','cycle_return_pct'):
                if r.get(k) is not None: v=float(r.get(k)); break
            if v is not None: vals.append(v)
            if r.get('symbol'): symbols.append(str(r.get('symbol')))
        n=len(vals); wins=sum(v>0 for v in vals); losses=sum(v<0 for v in vals); streak=0
        if vals:
            sign=1 if vals[-1]>0 else (-1 if vals[-1]<0 else 0)
            for v in reversed(vals):
                cur=1 if v>0 else (-1 if v<0 else 0)
                if sign and cur==sign: streak+=sign
                else: break
        recent=sum(vals[-5:])/min(5,n) if n else 0.0
        prevs=vals[-10:-5]; prior=sum(prevs)/len(prevs) if prevs else recent
        deg=recent-prior
        return {'trades':n,'wins':wins,'losses':losses,'win_rate':wins/n if n else None,'streak':streak,
                'mean_net_pct':sum(vals)/n if n else 0.0,'recent5_mean_net_pct':recent,'prior5_mean_net_pct':prior,
                'recent_vs_prior_pp':deg,'symbol_breadth':len(set(symbols)),'degrading':bool(n>=10 and deg<-.25)}

    def _smartpos_cycle_postmortems(self,name,obj):
        h=list((obj.state or {}).get('history') or [])
        rebs=[i for i,x in enumerate(h) if bool(x.get('rebalanced'))]
        for j in range(1,len(rebs)):
            a=h[rebs[j-1]]; b=h[rebs[j]]; ea=float(a.get('equity') or 0); eb=float(b.get('equity') or 0)
            if ea<=0: continue
            payload={'start_bar':a.get('bar_ts'),'end_bar':b.get('bar_ts'),'start_equity':ea,'end_equity':eb,
                     'cycle_return_pct':round((eb/ea-1)*100,6),'closing_turnover':b.get('turnover'),
                     'result':'WIN' if eb>ea else 'LOSS'}
            self._event(name,'CLOSED_REBALANCE_CYCLE',payload)
    def _options_postmortems(self):
        try: st=json.loads(OPTIONS_TRACK.read_text(encoding='utf-8'))
        except Exception: st={}
        for x in (st.get('resolved') or [])[-100:]:
            self._event('options_v2','RESOLVED_VOL_SIGNAL',x)

    def _recipes(self,key,b):
        cfg=dict(b.get('locked_config') or {});phase=(int(self.state.get('generation') or 1)-1)%4
        sets={
          'v70':[[('breadth_040',{'breadth_gate':.40}),('breadth_050',{'breadth_gate':.50}),('slots_2',{'slots':2})],[('breadth_0425',{'breadth_gate':.425}),('breadth_0475',{'breadth_gate':.475}),('mom_001',{'momentum_gate':.01})],[('ratio_065',{'ratio_gate':.65}),('ratio_085',{'ratio_gate':.85}),('sleeves_2',{'sleeves':2})],[('breadth_0375',{'breadth_gate':.375}),('breadth_0525',{'breadth_gate':.525}),('sleeves_4',{'sleeves':4})]],
          'v83':[[('alpha_020',{'alpha':.20}),('alpha_030',{'alpha':.30}),('persist_3',{'persistence_bars':3})],[('alpha_0225',{'alpha':.225}),('alpha_0275',{'alpha':.275}),('persist_1',{'persistence_bars':1})],[('alpha_0175',{'alpha':.175}),('alpha_0325',{'alpha':.325}),('persist_4',{'persistence_bars':4})],[('alpha_015',{'alpha':.15}),('alpha_035',{'alpha':.35}),('persist_5',{'persistence_bars':5})]],
          'smartpos':[[('ridge_5',{'ridge_alpha':5.}),('ridge_20',{'ridge_alpha':20.}),('horizon_8',{'horizon_bars':8})],[('ridge_75',{'ridge_alpha':7.5}),('ridge_15',{'ridge_alpha':15.}),('horizon_5',{'horizon_bars':5})],[('ridge_25',{'ridge_alpha':2.5}),('ridge_30',{'ridge_alpha':30.}),('horizon_4',{'horizon_bars':4})],[('ridge_125',{'ridge_alpha':12.5}),('horizon_7',{'horizon_bars':7}),('horizon_9',{'horizon_bars':9})]],
          'aftershock_v2':[[('hold_6',{'hold_hours':6}),('hold_18',{'hold_hours':18}),('threshold_hi',{'cascade_threshold':round(float(cfg.get('cascade_threshold',.863658))*1.10,6)})],[('hold_9',{'hold_hours':9}),('hold_15',{'hold_hours':15}),('threshold_lo',{'cascade_threshold':round(float(cfg.get('cascade_threshold',.863658))*.95,6)})],[('hold_3',{'hold_hours':3}),('hold_24',{'hold_hours':24}),('threshold_105',{'cascade_threshold':round(float(cfg.get('cascade_threshold',.863658))*1.05,6)})],[('hold_8',{'hold_hours':8}),('hold_16',{'hold_hours':16}),('threshold_115',{'cascade_threshold':round(float(cfg.get('cascade_threshold',.863658))*1.15,6)})]],
        }
        if key in sets:return sets[key][phase]
        if key=='options_v2':return [('har_rv',{'forecast_model':'HAR_RV'}),('ewma_rv',{'forecast_model':'EWMA_RV'}),('consensus_rv',{'forecast_model':'CONSENSUS_RV'})]
        if key=='funding_spread':return [('maker_only',{'execution':'MAKER_ONLY'}),('carry_48h',{'min_hold_hours':48}),('basis_guard',{'basis_guard':True})]
        if key=='v63_2':return [('slow_only',{'horizons':[1800,3600]}),('h1_only',{'horizons':[3600]})]
        return []
    def _spawn_variants(self,name,b,now,root=None):
        last=float((self.state.get('last_variant_cycle') or {}).get(name) or 0);key=root or name
        if now-last<21600 or b['stage'] in {'BLOCKED','QUARANTINE','SHADOW_RISK_FAIL'}:return 0
        try:variants=json.loads(VARIANTS.read_text(encoding='utf-8'))
        except Exception:variants=[]
        existing={json.dumps(x.get('config') or {},sort_keys=True) for x in variants if (x.get('root_parent') or x.get('parent'))==key};added=0
        for tag,delta in self._recipes(key,b):
            base=dict(b.get('locked_config') or {});base.update(delta);canon=json.dumps(base,sort_keys=True)
            if canon in existing:continue
            vid=f'{name}:{tag}:g{self.state.get("generation",1)}';variants.append({'id':vid,'parent':name,'root_parent':key,'parent_stage':b['stage'],'parent_score':b['score'],'config':base,'status':'PENDING_RESEARCH','selection':'DEVELOPMENT_ONLY','future_gate_required':True,'futures':futures_extension_policy.variant_profile(name,b.get('family') or ''),'live_enabled':False,'created_at':now});existing.add(canon);added+=1
        VARIANTS.write_text(json.dumps(variants[-500:],ensure_ascii=False,indent=2),encoding='utf-8');cyc=dict(self.state.get('last_variant_cycle') or {});cyc[name]=now;self.state['last_variant_cycle']=cyc;return added
    @staticmethod
    def _family_bucket(b):
        name=str(b.get('name') or ''); fam=str(b.get('family') or '')
        if name.startswith('funding_oi_'): return 'funding_oi_state'
        if name.startswith('oi_migration_'): return 'crossvenue_oi_migration'
        if name.startswith('taker_'): return 'crossvenue_taker'
        root=str(b.get('root_parent') or b.get('parent') or '')
        return root or fam or name

    def _portfolio_allocator(self,ranked):
        eligible=[x for x in ranked if x.get('stage')=='PORTFOLIO_ELIGIBLE']
        selected=[];seen=set()
        for b in eligible:
            bucket=self._family_bucket(b)
            if bucket in seen: continue
            if float(b.get('max_dd_pct') or 0)<-8: continue
            selected.append(b);seen.add(bucket)
            if len(selected)>=5: break
        signature={b['name']:int(b.get('resolved') or 0) for b in selected}
        prev=dict(self.state.get('portfolio_allocator') or {})
        if prev and signature==dict(prev.get('resolved_signature') or {}): return prev
        if not selected:return {'mode':'PAPER_ONLY','selected':[],'weights':{},'family_buckets':{},'resolved_signature':signature,'live_enabled':False}
        raw={}
        for b in selected:
            score=max(.01,float(b.get('score') or .01))
            dd=abs(min(0.0,float(b.get('max_dd_pct') or 0.0)))
            h=dict(b.get('health') or {}); hp=1.0
            if b.get('health_action')=='WATCH': hp*=.50
            if h.get('degrading'): hp*=.60
            if int(h.get('streak') or 0)<=-3: hp*=.70
            if int(h.get('trades') or 0)>=10 and int(h.get('symbol_breadth') or 0)<3: hp*=.75
            raw[b['name']]=score*hp/(1.0+dd)
        total=sum(raw.values()) or 1.0
        w={k:v/total for k,v in raw.items()}
        cap=.35
        for _ in range(8):
            over={k:v for k,v in w.items() if v>cap+1e-12}
            if not over: break
            fixed=sum(cap for _ in over); free=[k for k in w if k not in over]
            free_total=sum(w[k] for k in free)
            for k in over:w[k]=cap
            if free and free_total>0:
                scale=(1.0-fixed)/free_total
                for k in free:w[k]*=scale
        return {'mode':'PAPER_ONLY','selected':[b['name'] for b in selected],
                'weights':{k:round(v,6) for k,v in w.items()},
                'family_buckets':{b['name']:self._family_bucket(b) for b in selected},
                'resolved_signature':signature,'max_family_weight':cap,'live_enabled':False,
                'policy':{'one_branch_per_family':True,'rebalance_on_new_closed_trade_only':True,
                          'drawdown_penalty':True,'no_grid':True,'no_martingale':True,'no_dca':True}}

    async def refresh(self):
        now=time.time(); out={}; spawned=0
        try:
            prev=dict(self.state.get('branches') or {})
            for name,(family,obj) in BRANCHES.items():
                s=self._canonical_status(name,obj.status()); b=self._normalize(name,family,s); b['health']=self._branch_health(s); b['health_action']=self._health_action_stateful(name,b); b['stage']=self._stage(b); b['score']=self._score(b)
                if name in LINEAGE: b['parent']=LINEAGE[name]; b['root_parent']=LINEAGE[name]
                self._postmortems(name,s,b,prev.get(name))
                if b['stage'] in {'PROFIT_BRANCH','PORTFOLIO_ELIGIBLE'} and b.get('health_action')!='QUARANTINE':
                    root=LINEAGE.get(name) if name in LINEAGE else None
                    spawned+=self._spawn_variants(name,b,now,root)
                out[name]=b
            for vid,cs in (raven_v70_variant_pool.status().get('children') or {}).items():
                b=self._normalize(vid,'directional_child',cs);b['stage']=self._stage(b);b['score']=self._score(b);b['parent']='v70';b['root_parent']='v70'
                self._postmortems(vid,cs,b,prev.get(vid));out[vid]=b
                if b['stage'] in {'PROFIT_BRANCH','PORTFOLIO_ELIGIBLE'}:spawned+=self._spawn_variants(vid,b,now,'v70')
            for vid,cs in (raven_v83_variant_pool.status().get('children') or {}).items():
                b=self._normalize(vid,'cash_squeeze_child',cs);b['stage']=self._stage(b);b['score']=self._score(b);b['parent']='v83';b['root_parent']='v83'
                self._postmortems(vid,cs,b,prev.get(vid));out[vid]=b
                if b['stage'] in {'PROFIT_BRANCH','PORTFOLIO_ELIGIBLE'}:spawned+=self._spawn_variants(vid,b,now,'v83')
            self._smartpos_cycle_postmortems('smartpos',raven_smart_positioning_v61_shadow)
            self._smartpos_cycle_postmortems('smartpos_g2',raven_smartpos_g2_drop_toppos_shadow)
            self._options_postmortems()
            ranked=sorted(out.values(),key=lambda x:x['score'],reverse=True)
            portfolio=[x['name'] for x in ranked if x['stage']=='PORTFOLIO_ELIGIBLE']
            profit=[x['name'] for x in ranked if x['stage'] in {'PROFIT_BRANCH','PORTFOLIO_ELIGIBLE'}]
            self.state['branches']=out; self.state['last_refresh']=now
            self.state['generation']=int(self.state.get('generation') or 1)+(1 if spawned else 0)
            alloc=self._portfolio_allocator(ranked)
            self.state['portfolio_eligible']=portfolio;self.state['profit_branches']=profit
            self.state['portfolio_allocator']=alloc
            self.state['last_spawned_variants']=spawned;self._save();self.last_error=None
            self.latest={'generation':self.state['generation'],'branches':out,'ranking':[x['name'] for x in ranked],
                         'profit_branches':profit,'portfolio_eligible':portfolio,'portfolio_allocator':alloc,'health_controls':dict(self.state.get('health_controls') or {}),'spawned_variants':spawned}
        except Exception as exc:self.last_error=str(exc)[:500]
        return self.status()
    def status(self):
        st=self.latest or self.state;branches=st.get('branches') or {}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'CONTROLLED_PAPER_EVOLUTION',
                'future_only_promotion':True,'futures_standard':futures_extension_policy.status(),'live_promotion_enabled':False,'grid':False,'martingale':False,'dca':False,
                'interval_seconds':self.interval,'event_count':len(self.state.get('event_ids') or []),
                'quarantined':[k for k,v in branches.items() if str(v.get('stage','')).startswith('QUARANTINE')],
                'last_error':self.last_error,'state':st}
    async def start(self):
        if self.task and not self.task.done():return
        asyncio.create_task(self.refresh(),name='initial-refresh-'+self.__class__.__name__);self.task=asyncio.create_task(self._loop(),name='profit-branch-network')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(max(300.0,self.interval));await self.refresh()

profit_branch_network=ProfitBranchNetwork()
