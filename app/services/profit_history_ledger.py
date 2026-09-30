from __future__ import annotations
import asyncio, hashlib, json, time
from pathlib import Path
from app.services.profit_branch_network import profit_branch_network

ROOT=Path(__file__).parents[2]; DATA=ROOT/'data'
LEDGER=DATA/'profit_history_ledger.jsonl'
STATE=DATA/'profit_history_ledger_state.json'
INDEX=DATA/'profit_history_index.json'
EVENTS=DATA/'profit_branch_events.jsonl'

class ProfitHistoryLedger:
    def __init__(self):
        self.enabled=True; self.interval=900.; self.task=None
        self.last_error=None; self.last_refresh=None
        self.state=self._load_state(); self.latest={}
    def _load_state(self):
        base={'seen_ids':[],'branch_signatures':{},'research_shadow_signatures':{},'created_at':time.time()}
        try:
            old=json.loads(STATE.read_text(encoding='utf-8'))
            if isinstance(old,dict): base.update(old)
        except Exception: pass
        return base
    def _save_state(self):
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _hash(obj):
        raw=json.dumps(obj,sort_keys=True,ensure_ascii=False,default=str)
        return hashlib.sha1(raw.encode()).hexdigest()
    def _append(self,kind,payload,stable_id=None):
        eid=stable_id or self._hash({'kind':kind,'payload':payload})
        seen=set(self.state.get('seen_ids') or [])
        if eid in seen:return False
        row={'id':eid,'ts':time.time(),'kind':kind,'payload':payload}
        with LEDGER.open('a',encoding='utf-8') as f:
            f.write(json.dumps(row,ensure_ascii=False,default=str)+'\n')
        ids=list(self.state.get('seen_ids') or []);ids.append(eid)
        self.state['seen_ids']=ids[-200000:]
        return True
    @staticmethod
    def _research_payload(path,x):
        h=x.get('holdout_180d') if isinstance(x.get('holdout_180d'),dict) else {}
        d=x.get('holdout_double_cost') if isinstance(x.get('holdout_double_cost'),dict) else (x.get('double_cost') if isinstance(x.get('double_cost'),dict) else {})
        t=x.get('holdout_triple_cost') if isinstance(x.get('holdout_triple_cost'),dict) else (x.get('triple_cost') if isinstance(x.get('triple_cost'),dict) else {})
        return {'file':str(path.relative_to(ROOT)),'strategy':x.get('strategy') or x.get('name') or path.stem,
                'holdout_return_pct':h.get('return_pct'),'holdout_max_dd_pct':h.get('max_dd_pct'),
                'double_cost_return_pct':d.get('return_pct'),'triple_cost_return_pct':t.get('return_pct'),
                'selection_uses_holdout':x.get('selection_uses_holdout'),'grid':x.get('grid',x.get('grid_trading')),
                'martingale':x.get('martingale'),'dca':x.get('dca'),'generated_at':x.get('generated_at')}
    def _backfill_research(self):
        added=0
        for p in DATA.rglob('*.json'):
            if p in {STATE,INDEX}:continue
            try:x=json.loads(p.read_text(encoding='utf-8'))
            except Exception:continue
            if not isinstance(x,dict) or not isinstance(x.get('holdout_180d'),dict):continue
            payload=self._research_payload(p,x);rid='research:'+str(p.relative_to(ROOT))+':'+self._hash(x)
            added+=int(self._append('RESEARCH_RESULT',payload,rid))
        return added
    def _backfill_events(self):
        if not EVENTS.exists():return 0
        added=0
        for line in EVENTS.read_text(encoding='utf-8').splitlines():
            try:r=json.loads(line)
            except Exception:continue
            eid='event:'+str(r.get('id') or self._hash(r))
            added+=int(self._append('BRANCH_EVENT',r,eid))
        return added
    def _backfill_shadow_histories(self):
        added=0;keys=('history','resolved','resolved_cycles','closed_trades','recent_resolved','trades')
        for p in DATA.glob('*.json'):
            low=p.name.lower()
            if not any(x in low for x in ('shadow','paper','portfolio')):continue
            try:x=json.loads(p.read_text(encoding='utf-8'))
            except Exception:continue
            if not isinstance(x,dict):continue
            for key in keys:
                rows=x.get(key)
                if not isinstance(rows,list):continue
                for item in rows:
                    if not isinstance(item,dict):continue
                    payload={'file':str(p.relative_to(ROOT)),'series':key,'item':item}
                    eid='shadowhist:'+str(p.relative_to(ROOT))+':'+key+':'+self._hash(item)
                    added+=int(self._append('SHADOW_HISTORY',payload,eid))
        return added
    def _backfill_aux_jsonl(self):
        specs=[('futures_forward_history_v1.jsonl','FUTURES_FORWARD_HISTORY'),('futures_forward_invalidations_v1.jsonl','FUTURES_FORWARD_INVALIDATION'),('v42_perp_forward_history_v1.jsonl','FUTURES_FORWARD_HISTORY'),('research_frontier_history.jsonl','FRONTIER_SNAPSHOT')]
        added=0
        for name,kind in specs:
            p=DATA/name
            if not p.exists():continue
            for line in p.read_text(encoding='utf-8').splitlines():
                try:r=json.loads(line)
                except Exception:continue
                if not isinstance(r,dict):continue
                key=str(r.get('id') or r.get('signature') or r.get('logical_id') or self._hash(r))
                payload={'file':str(p.relative_to(ROOT)),'record':r}
                added+=int(self._append(kind,payload,'aux:'+kind+':'+name+':'+key))
        return added

    def _snapshot_research_shadows(self):
        names=('liquidation_sweep_shadow_v1.json','liquidity_migration_perp_shadow_v1.json','funding_oi_state_transition_shadow_v1.json','futures_curve_shadow_v1.json','options_gamma_rebalance_shadow_v1.json','options_funding_regime_mismatch_shadow_v1.json','crossvenue_liquidation_asymmetry_shadow_v1.json','crossvenue_oi_migration_shadow_v1.json','major_alt_liquidation_contagion_shadow_v1.json','crossvenue_taker_imbalance_divergence_shadow_v2.json','liquidation_absorption_shadow_v1.json','funding_reset_drift_shadow_v1.json')
        sigmap=dict(self.state.get('research_shadow_signatures') or {});added=0
        for name in names:
            p=DATA/name
            if not p.exists():continue
            try:x=json.loads(p.read_text(encoding='utf-8'))
            except Exception:continue
            if not isinstance(x,dict):continue
            pending=x.get('pending') if isinstance(x.get('pending'),list) else []
            resolved=x.get('resolved') if isinstance(x.get('resolved'),list) else []
            equity=x.get('equity');return_pct=x.get('return_pct')
            if return_pct is None and isinstance(equity,(int,float)):return_pct=float(equity)-100.0
            payload={'file':str(p.relative_to(ROOT)),'strategy':x.get('strategy') or x.get('version') or p.stem,'mode':x.get('mode') or 'FUTURE_ONLY_SHADOW','live_enabled':False if x.get('live_enabled') is None else x.get('live_enabled'),'promotion_eligible':False if x.get('promotion_eligible') is None else x.get('promotion_eligible'),'future_gate':x.get('future_gate') or x.get('gate'),'pending_count':x.get('pending_count',len(pending)),'resolved_count':x.get('resolved_count',len(resolved)),'observations':x.get('observations'),'events':x.get('events'),'return_pct':return_pct,'max_dd_pct':x.get('max_dd_pct'),'equity':equity,'last_error':x.get('last_error')}
            for k in ('follow','fade','continuation','recovery','contagion','relief','absorption','revert','continue','options','perp','long','short'):
                if isinstance(x.get(k),dict):payload[k]=x.get(k)
            for name2 in ('follow','fade','continuation','recovery','contagion','relief','absorption','revert','continue','long','short'):
                eq=x.get(name2+'_equity')
                if isinstance(eq,(int,float)):
                    payload[name2]={'equity':eq,'return_pct':float(eq)-100.0,'max_dd_pct':x.get(name2+'_max_dd_pct')}
            sig=self._hash(payload)
            if sigmap.get(name)==sig:continue
            row={**payload,'captured_at':time.time()}
            self._append('RESEARCH_SHADOW_SNAPSHOT',row,'research-shadow:'+name+':'+sig)
            sigmap[name]=sig;added+=1
        self.state['research_shadow_signatures']=sigmap
        return added

    def _snapshot_branches(self):
        st=profit_branch_network.status().get('state') or {}
        branches=st.get('branches') or {};added=0;sigmap=dict(self.state.get('branch_signatures') or {})
        now=time.time()
        for name,b in branches.items():
            payload={'branch':name,'family':b.get('family'),'parent':b.get('parent'),'root_parent':b.get('root_parent'),
                     'stage':b.get('stage'),'equity':b.get('equity'),'return_pct':b.get('return_pct'),
                     'max_dd_pct':b.get('max_dd_pct'),'observations':b.get('observations'),'resolved':b.get('resolved'),
                     'wins':b.get('wins'),'win_rate':b.get('win_rate'),'score':b.get('score'),
                     'future_only':b.get('future_only'),'live_enabled':b.get('live_enabled'),
                     'futures_required':b.get('futures_required'),'futures_promotion_ready':b.get('futures_promotion_ready'),
                     'futures_stage':((b.get('futures') or {}).get('stage')),'captured_at':now}
            sig=self._hash({k:v for k,v in payload.items() if k!='captured_at'})
            if sigmap.get(name)==sig:continue
            self._append('BRANCH_SNAPSHOT',payload,'branch:'+name+':'+sig);sigmap[name]=sig;added+=1
        self.state['branch_signatures']=sigmap
        return added
    def _iter_ledger(self):
        if not LEDGER.exists():return
        with LEDGER.open('r',encoding='utf-8') as f:
            for line in f:
                try:yield json.loads(line)
                except Exception:continue
    def _checkpoint(self):
        st=profit_branch_network.status().get('state') or {};branches=st.get('branches') or {}
        stages={}
        for b in branches.values():
            s=str(b.get('stage') or 'UNKNOWN');stages[s]=stages.get(s,0)+1
        try:port=json.loads((DATA/'profit_portfolio_allocator.json').read_text(encoding='utf-8'))
        except Exception:port={}
        bucket=int(time.time()//900)
        payload={'branch_count':len(branches),'stages':stages,'profit_branches':st.get('profit_branches') or [],
                 'portfolio_eligible':st.get('portfolio_eligible') or [],'portfolio_weights':port.get('weights') or {'cash':1.0},
                 'portfolio_equity':port.get('meta_equity',100.0),'generation':st.get('generation')}
        return int(self._append('NETWORK_CHECKPOINT',payload,f'checkpoint:{bucket}'))
    def _build_index(self):
        kinds={};branches={};research=[];events={};entries=0
        for row in self._iter_ledger() or []:
            entries+=1;k=str(row.get('kind') or 'UNKNOWN');kinds[k]=kinds.get(k,0)+1;p=row.get('payload') or {}
            if k=='BRANCH_SNAPSHOT':
                n=str(p.get('branch'));b=branches.setdefault(n,{'snapshots':0,'first_seen':row.get('ts'),'last_seen':row.get('ts'),'stages':[]})
                b['snapshots']+=1;b['last_seen']=row.get('ts');b['latest']=p
                if p.get('stage') not in b['stages']:b['stages'].append(p.get('stage'))
                r=p.get('return_pct')
                if r is not None:b['best_future_return_pct']=max(float(r),float(b.get('best_future_return_pct',r)))
            elif k=='RESEARCH_RESULT':research.append(p)
            elif k=='BRANCH_EVENT':
                n=str(p.get('branch') or 'unknown');events[n]=events.get(n,0)+1
        try: decisions=json.loads((DATA/'research_archive_decisions.json').read_text(encoding='utf-8'))
        except Exception: decisions={}
        try: clusters=json.loads((DATA/'research_cluster_registry.json').read_text(encoding='utf-8')).get('clusters') or {}
        except Exception: clusters={}
        cmap={m:c for c,v in clusters.items() for m in (v.get('members') or [])}
        for x in research:
            n=str(x.get('strategy') or '');d=decisions.get(n) or {};x['review_status']=d.get('status');x['review_reason']=d.get('reason');x['cluster']=cmap.get(n,n)
        valid=[x for x in research if x.get('holdout_return_pct') is not None]
        valid.sort(key=lambda x:float(x.get('holdout_return_pct') or -1e9),reverse=True)
        trusted=[x for x in valid if x.get('review_status')!='REJECTED' and x.get('double_cost_return_pct') is not None and x.get('triple_cost_return_pct') is not None and float(x.get('double_cost_return_pct') or 0)>0 and float(x.get('triple_cost_return_pct') or 0)>0 and x.get('selection_uses_holdout') is not True and not x.get('grid') and not x.get('martingale') and not x.get('dca')]
        trusted.sort(key=lambda x:float(x.get('holdout_return_pct') or -1e9),reverse=True)
        independent=[];seen=set()
        for x in trusted:
            c=x.get('cluster') or str(x.get('strategy') or '')
            if c in seen: continue
            seen.add(c);independent.append(x)
        idx={'updated_at':time.time(),'ledger_entries':entries,'kinds':kinds,'branches':branches,
             'research_result_count':len(research),'branch_event_counts':events,'top_research_holdout':valid[:25],'top_trusted_research_holdout':trusted[:25],'top_trusted_independent_holdout':independent[:25]}
        INDEX.write_text(json.dumps(idx,ensure_ascii=False,indent=2),encoding='utf-8');self.latest=idx
        return idx
    async def refresh(self):
        try:
            added_research=await asyncio.to_thread(self._backfill_research)
            added_events=await asyncio.to_thread(self._backfill_events)
            added_shadow=await asyncio.to_thread(self._backfill_shadow_histories)
            added_aux=await asyncio.to_thread(self._backfill_aux_jsonl)
            added_research_shadows=self._snapshot_research_shadows()
            added_snapshots=self._snapshot_branches();added_checkpoint=self._checkpoint()
            idx=await asyncio.to_thread(self._build_index)
            self.state['last_refresh']=time.time();self._save_state();self.last_error=None
            self.latest={**idx,'last_added':{'research':added_research,'events':added_events,'shadow_history':added_shadow,
                         'aux_forward_history':added_aux,'branch_snapshots':added_snapshots,'checkpoint':added_checkpoint}}
        except Exception as exc:self.last_error=str(exc)[:500]
        self.last_refresh=time.time();return self.status()
    def status(self):
        idx=self.latest
        if not idx:
            try:idx=json.loads(INDEX.read_text(encoding='utf-8'))
            except Exception:idx={}
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'APPEND_ONLY_PROFIT_HISTORY',
                'append_only':True,'live_enabled':False,'interval_seconds':self.interval,
                'ledger_entries':idx.get('ledger_entries',0),'research_results':idx.get('research_result_count',0),
                'branch_count':len(idx.get('branches') or {}),'kinds':idx.get('kinds') or {},
                'top_research_holdout':(idx.get('top_research_holdout') or [])[:10],
                'top_trusted_research_holdout':(idx.get('top_trusted_research_holdout') or [])[:10],
                'top_trusted_independent_holdout':(idx.get('top_trusted_independent_holdout') or [])[:10],
                'last_added':idx.get('last_added') or {},'last_refresh':self.last_refresh,'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='profit-history-ledger')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None;self._save_state()
    async def _loop(self):
        while True:
            await asyncio.sleep(max(300.,self.interval))
            if self.enabled:await self.refresh()

profit_history_ledger=ProfitHistoryLedger()