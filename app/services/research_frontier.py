import json,time
from pathlib import Path
from app.services.raven_v103_regime_composite_shadow import raven_v103_regime_composite_shadow
from app.services.idio_skew_rotation_shadow import idio_skew_rotation_shadow
from app.services.crowding_dislocation_v2_shadow import crowding_dislocation_v2_shadow
from app.services.aftershock_v2_shadow import aftershock_v2_shadow
from app.services.aftershock_expanded_shadow import aftershock_expanded_shadow
from app.services.raven_v105_bk_finite_arbitrage_shadow import raven_v105_bk_finite_arbitrage_shadow
from app.services.options_perp_hedge_shadow import options_perp_hedge_shadow
from app.services.options_ivrv_package_shadow import options_ivrv_package_shadow
from app.services.options_relative_vol_shadow import options_relative_vol_shadow
from app.services.options_term_structure_shadow import options_term_structure_shadow
from app.services.options_skew_shadow import options_skew_shadow
from app.services.raven_v42_funding_shadow import raven_v42_funding_shadow
from app.services.profit_portfolio_allocator import profit_portfolio_allocator
from app.services.futures_forward_worker import futures_forward_worker
from app.services.futures_data_integrity import futures_data_integrity

ROOT=Path(__file__).parents[2];DATA=ROOT/'data';INDEX=DATA/'profit_history_index.json'

class ResearchFrontier:
    version='RESEARCH_FRONTIER_V1'
    @staticmethod
    def _json(path,default=None):
        try:return json.loads(path.read_text(encoding='utf-8'))
        except Exception:return {} if default is None else default
    @staticmethod
    def _hist_map():
        x=ResearchFrontier._json(INDEX,{})
        return {str(r.get('cluster')):r for r in (x.get('top_trusted_independent_holdout') or []) if r.get('cluster')}
    @staticmethod
    def _progress(g):
        out=[]
        pairs={'minimum_observations':'observations','required_observations':'observations','required_active_observations':'active_observations','required_resolved_events':'resolved_events','required_events':'accepted_events','required_unique_routes':'unique_routes','required_span_hours':'span_hours','required_rebalances':'rebalances','required_cycles':'cycles','required_affected_events':'affected_events'}
        for rk,ck in pairs.items():
            if rk not in g:continue
            req=float(g.get(rk) or 0);cur=float(g.get(ck) or 0);out.append({'metric':ck,'current':cur,'required':req,'pct':min(100.0,(cur/req*100.0 if req>0 else 0.0))})
        for name,z in g.items():
            if isinstance(z,dict) and 'required_episodes' in z:
                req=float(z.get('required_episodes') or 0);cur=float(z.get('completed_episodes') or 0);out.append({'metric':name+'_episodes','current':cur,'required':req,'pct':min(100.0,(cur/req*100.0 if req>0 else 0.0))})
        return out

    @staticmethod
    def _forward(st):
        g=st.get('future_gate') or st.get('future_validation_gate') or {}
        return {'phase':st.get('phase'),'observations':st.get('observation_count',g.get('observations')),
                'events':st.get('event_count',g.get('accepted_events')),'resolved':st.get('resolved_count'),
                'return_pct':st.get('return_pct'),'max_dd_pct':st.get('max_dd_pct'),'gate':g,'progress':ResearchFrontier._progress(g),
                'ready':bool(g.get('ready') or g.get('ready_for_review'))}
    @staticmethod
    def _row(cluster,name,st,hist=None,kind='TRUSTED_HISTORY'):
        h=hist or {};f=ResearchFrontier._forward(st);ready=f['ready']
        blocker=None
        if not ready:
            if cluster=='directional_v70':blocker='FUTURE_ONLY_SIGNAL_AND_PORTFOLIO_GATE_PENDING'
            elif cluster=='idio_skew':blocker='FUTURE_CYCLE_AND_COST_GATE_PENDING'
            elif cluster=='crowding_dislocation':blocker='FUTURE_CROWDING_EVENTS_PENDING'
            elif cluster=='aftershock':blocker='SHORT_SAMPLE_FUTURE_EVENTS_PENDING'
            elif cluster=='finite_arbitrage':blocker='FULL_LOOP_EVENTS_ROUTES_AND_48H_PENDING'
            elif cluster=='options':blocker='RESOLVED_OPTION_CYCLES_AND_HEDGE_EVIDENCE_PENDING'
        return {'cluster':cluster,'representative':name,'kind':kind,
                'historical':{'return_pct':h.get('holdout_return_pct'),'max_dd_pct':h.get('holdout_max_dd_pct'),
                              'double_cost_return_pct':h.get('double_cost_return_pct'),'triple_cost_return_pct':h.get('triple_cost_return_pct'),
                              'review_status':h.get('review_status')},
                'forward':f,'capital_ready':False,'live_enabled':False,'blocker':blocker}

    def status(self):
        hm=self._hist_map();rows=[]
        fw=futures_forward_worker.status();tr=fw.get('tracker') or {};vs=(tr.get('strategies') or {}).get('v103') or {};dg=tr.get('directional_gate') or {}
        exact={'phase':'WARMUP' if int(dg.get('observations') or 0)<24 else 'FUTURE_VALIDATION','observation_count':int(dg.get('observations') or 0),'return_pct':vs.get('return_pct'),'max_dd_pct':vs.get('max_dd_pct'),'future_validation_gate':dg,'source':'EXACT_BINANCE_PERP_FORWARD','data_end':tr.get('data_end'),'full_8h_only':bool(tr.get('full_8h_only'))}
        rows.append(self._row('directional_v70','v103_REGIME_COMPOSITE',exact,hm.get('directional_v70')))
        rows.append(self._row('idio_skew','IDIOSYNCRATIC_SKEW_ROTATION_V1',idio_skew_rotation_shadow.status(),hm.get('idio_skew')))
        rows.append(self._row('crowding_dislocation','CROWDING_DISLOCATION_BYBIT_V2',crowding_dislocation_v2_shadow.status(),hm.get('crowding_dislocation')))
        rows.append(self._row('aftershock','aftershock_v2',aftershock_v2_shadow.status(),None,'EXPERIMENTAL_FUTURE_ONLY'))
        rows.append(self._row('finite_arbitrage','v105_bk_finite_arbitrage',raven_v105_bk_finite_arbitrage_shadow.status(),None,'EXPERIMENTAL_FUTURE_ONLY'))
        opt=options_perp_hedge_shadow.status();opt['phase']='PAPER_DELTA_HEDGE';opt['observation_count']=opt.get('snapshots')
        orow=self._row('options','options_perp_delta_hedge_v1',opt,None,'EXPERIMENTAL_FUTURE_ONLY')
        pk={'ivrv':options_ivrv_package_shadow.status(),'relative_vol':options_relative_vol_shadow.status(),
            'term_structure':options_term_structure_shadow.status(),'skew':options_skew_shadow.status()}
        orow['option_packages']={k:{'mode':v.get('mode'),'resolved':int(v.get('resolved_count') or 0),'last_error':v.get('last_error')} for k,v in pk.items()}
        orow['option_resolved_total']=sum(x['resolved'] for x in orow['option_packages'].values())
        et=self._json(DATA/'options_edge_future_tracker.json',{});er=et.get('resolved') or [];ep=et.get('pending') or [];correct=sum(1 for x in er if bool(x.get('correct')))
        orow['surface_forecast_tracker']={'pending':len(ep),'resolved':len(er),'correct':correct,'accuracy':(correct/len(er) if er else None),'note':'forecast evidence only; not package PnL or capital gate'}
        rows.append(orow)
        port=profit_portfolio_allocator.status();eligible=list(port.get('eligible') or [])
        v42legacy=raven_v42_funding_shadow.status();v42x=fw.get('v42_tracker') or self._json(DATA/'v42_perp_forward_tracker_v1.json',{});expanded=aftershock_expanded_shadow.status();arbdep=self._json(DATA/'arbitrage_venue_dependency_v1.json',{})
        v42st={'phase':'WARMUP' if int(v42x.get('observations') or 0)<24 else 'FUTURE_VALIDATION','observation_count':int(v42x.get('observations') or 0),'return_pct':v42x.get('candidate_return_pct'),'max_dd_pct':v42x.get('candidate_max_dd_pct'),'future_gate':v42x.get('gate') or {},'source':'EXACT_BINANCE_PERP_WITH_BINANCE_BYBIT_FUNDING','data_end':v42x.get('data_end'),'full_8h_only':bool(v42x.get('full_8h_only'))}
        return {'ok':True,'version':self.version,'mode':'RESEARCH_ONLY','generated_at':time.time(),
                'summary':{'trusted_independent_clusters':sum(1 for r in rows if r['kind']=='TRUSTED_HISTORY'),
                           'experimental_future_only':sum(1 for r in rows if r['kind']=='EXPERIMENTAL_FUTURE_ONLY'),
                           'portfolio_eligible_count':len(eligible),'portfolio_weights':port.get('weights') or {'cash':1.0},
                           'portfolio_equity':port.get('meta_equity',100.0),'live_enabled':False},
                'rows':rows,
                'overlays':[{'name':'v42_funding_confirmation','parent_cluster':'directional_v70',
                             'forward':self._forward(v42st),'source':'EXACT_PERP_FORWARD','capital_ready':False,'live_enabled':False,
                             'blocker':'FUTURE_ONLY_FUNDING_AFFECTED_EVENTS_PENDING'}],
                'diagnostics':[{'name':'legacy_v42_spot_runtime','status':v42legacy,'promotion_source':False,'note':'legacy v42 runtime retained for audit only; exact perp v42 is canonical'},{'name':'legacy_v103_spot_runtime','status':raven_v103_regime_composite_shadow.status(),'promotion_source':False,'note':'legacy runtime retained for audit only; canonical promotion evidence is exact Binance perpetual forward'},{'name':'aftershock_expanded_v1','universe_size':expanded.get('universe_size'),
                                'observations':expanded.get('observation_count'),'events':expanded.get('event_count'),
                                'resolved':expanded.get('resolved_count'),'symbol_errors':expanded.get('symbol_errors') or {},
                                'note':'same frozen aftershock rule; separate future-only evidence; never auto-promote'},
                               {'name':'arbitrage_venue_dependency','all':arbdep.get('all') or {},'recent':arbdep.get('recent') or {},
                                'capital_policy':arbdep.get('capital_policy'),'live_enabled':False}],
                'data_integrity':futures_data_integrity.status(),
                'policy':{'no_grid':True,'no_martingale':True,'no_dca':True,
                          'holdout_not_used_for_selection':True,'live_requires_future_evidence':True,
                          'correlated_variants_count_once':True}}

research_frontier=ResearchFrontier()
