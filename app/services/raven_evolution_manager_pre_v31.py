from __future__ import annotations
import asyncio, json, time
from pathlib import Path

ROOT=Path(__file__).parents[2]
DATA=ROOT/'data'
STATE=DATA/'raven_evolution_state.json'
CHAMPION_RUNTIME=DATA/'raven_champion_runtime.json'
RESULT_FILES={
    'v24':DATA/'raven_turbo_highbeta_v24_results.json',
    'v25':DATA/'raven_turbo_highbeta_v25_results.json',
    'v26':DATA/'raven_turbo_v26_cash_overlay_results.json',
    'v27':DATA/'raven_meta_v27_results.json',
    'v28':DATA/'raven_turbo_v28_risk_parity_results.json',
    'v29':DATA/'raven_turbo_v29_pair_quality_results.json',
}
CHALLENGER_DIR=DATA/'raven_challengers'
POLICY={
    'target_monthly_pct':40.0,
    'max_holdout_dd_pct':12.0,
    'min_holdout_months':6,
    'min_positive_month_ratio':0.70,
    'min_paper_score_improvement':3.0,
    'max_dd_worsening_pct':2.0,
    'live_promotion_enabled':False,
    'shadow_hours':48.0,'shadow_min_observations':96,'shadow_min_active_observations':8,
    'shadow_min_edge_pct':1.0,'shadow_max_dd_worsening_pct':2.0,
}

class RavenEvolutionManager:
    def __init__(self):
        self.interval=21600.0
        self.task=None
        self.last_error=None
        self.state=self._load()
    def _load(self):
        base={
            'paper_champion':'v24','live_champion':None,'generation':24,
            'candidates':{},'promotion_history':[],'rollback_history':[],
            'last_scan':None,'target_40_achieved':False,
        }
        try:
            old=json.loads(STATE.read_text(encoding='utf-8'))
            if isinstance(old,dict): base.update(old)
        except Exception:
            pass
        return base

    def _save(self):
        STATE.parent.mkdir(parents=True,exist_ok=True)
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')

    @staticmethod
    def _kpi(monthly):
        vals=[float(x) for x in (monthly or [])]
        ge40=sum(1 for x in vals if x>=40.0)
        return {
            'months_total':len(vals),'months_ge_40':ge40,
            'ratio_ge_40':round(ge40/max(1,len(vals)),4),
            'all_months_ge_40':bool(vals and all(x>=40.0 for x in vals)),
        }

    @staticmethod
    def _score(h,kpi):
        return float(h.get('return_pct',0))+30*float(h.get('monthly_positive_ratio',0))+40*float(kpi['ratio_ge_40'])-2*abs(float(h.get('max_dd_pct',0)))+float(h.get('monthly_median_pct',0))
    def _candidate(self,name,path):
        if not path.exists():
            return {'name':name,'available':False,'reason':'RESULT_MISSING'}
        try:
            raw=json.loads(path.read_text(encoding='utf-8'))
        except Exception as exc:
            return {'name':name,'available':False,'reason':f'READ_ERROR:{str(exc)[:80]}'}
        h=raw.get('holdout_180d') or {}; monthly=h.get('monthly') or []
        train=raw.get('train') or {}; wf=raw.get('walkforward') or {}
        kpi=self._kpi(monthly); score=self._score(h,kpi)
        selected_on=str(raw.get('selected_on') or '')
        auto_challenger=selected_on=='train_walkforward_only' or name.startswith('challenger:')
        dd=float(h.get('max_dd_pct',0)); pos=float(h.get('monthly_positive_ratio',0)); months=int(h.get('months',len(monthly)) or 0)
        gates={
            'train_selected':selected_on in {'train_only','train_walkforward_only'},
            'months':months>=POLICY['min_holdout_months'],
            'drawdown':dd>=-POLICY['max_holdout_dd_pct'],
            'positive_ratio':pos>=POLICY['min_positive_month_ratio'],
            'positive_return':float(h.get('return_pct',0))>0,
        }
        return {
            'name':name,'available':True,'strategy':raw.get('strategy'),
            'holdout':h,'kpi40':kpi,'score':round(score,6),
            'research_gate_pass':all(gates.values()),'gates':gates,
            'target_40_pass':kpi['all_months_ge_40'],
            'selected_on':selected_on,'auto_challenger':auto_challenger,
            'train':train,'train_score':raw.get('train_score'),'walkforward':wf,
            'chosen_cfg':raw.get('chosen_cfg') or {},'source_file':str(path),
        }

    def _promote_paper_if_better(self,cands):
        # Research/backtest never promotes a running PAPER champion directly.
        return None

    def _write_runtime_champion(self,name,cfg,source):
        payload={'champion_name':name,'config':cfg,'promoted_at':time.time(),'source':source}
        CHAMPION_RUNTIME.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8')

    def _clear_shadow(self):
        self.state['shadow_nominee']=None
        self.state['shadow_nominee_started_at']=None
        self.state['shadow_nominee_lock_until']=None
        self.state['shadow_nominee_locked']=False

    def review_shadow(self,shadow):
        nominee=self.state.get('shadow_nominee') or {};name=nominee.get('name')
        if not name or shadow.get('nominee_name')!=name:return {'status':'WAIT_NOMINEE'}
        now=time.time();started=float(self.state.get('shadow_nominee_started_at') or now)
        elapsed=now-started;obs=int(shadow.get('observation_count') or 0);active=int(shadow.get('active_observation_count') or 0)
        lock=float(self.state.get('shadow_nominee_lock_seconds') or POLICY['shadow_hours']*3600)
        if elapsed<lock or obs<POLICY['shadow_min_observations']:
            return {'status':'WAIT_WINDOW','hours':round(elapsed/3600,2),'observations':obs,'active':active}
        if active<POLICY['shadow_min_active_observations']:
            max_until=started+7*86400;self.state['shadow_nominee_lock_until']=min(max_until,now+86400);self._save()
            return {'status':'WAIT_ACTIVITY','active':active,'required':POLICY['shadow_min_active_observations']}
        edge=float(shadow.get('relative_edge_pct') or 0);cdd=float(shadow.get('max_dd_pct') or 0);bdd=float(shadow.get('benchmark_max_dd_pct') or 0)
        dd_worse=abs(min(0.0,cdd))-abs(min(0.0,bdd));passed=edge>=POLICY['shadow_min_edge_pct'] and dd_worse<=POLICY['shadow_max_dd_worsening_pct'] and float(shadow.get('return_pct') or 0)>0
        result='PROMOTED' if passed else 'REJECTED';review={'ts':now,'name':name,'result':result,'edge_pct':round(edge,4),'challenger_dd_pct':round(cdd,4),'benchmark_dd_pct':round(bdd,4),'active_observations':active,'observations':obs}
        reviews=dict(self.state.get('shadow_reviews') or {});reviews[name]=review;self.state['shadow_reviews']=reviews
        if passed:
            self._write_runtime_champion(name,nominee.get('chosen_cfg') or {},'SHADOW_OUTPERFORMED_CHAMPION')
            old=self.state.get('paper_champion') or 'v24';self.state['paper_champion']=name;self.state['generation']=int(self.state.get('generation') or 0)+1
            hist=list(self.state.get('promotion_history') or []);hist.append({'ts':now,'from':old,'to':name,'reason':'SHADOW_OUTPERFORMED_CHAMPION','live_changed':False,**review});self.state['promotion_history']=hist[-100:]
        self._clear_shadow();self.state['last_shadow_review']=review;self._save();return review

    def _select_shadow_nominee(self,cands):
        rows=[]
        reviewed=self.state.get('shadow_reviews') or {}
        for x in cands.values():
            if not x.get('available') or not x.get('auto_challenger') or x.get('name') in reviewed: continue
            wf=x.get('walkforward') or {}; tr=x.get('train') or {}
            pos=float(wf.get('positive_ratio',0)); med=float(wf.get('median_return',-999)); worst=float(wf.get('worst_return',-999))
            tdd=float(tr.get('max_dd_pct',-999)); tret=float(tr.get('return_pct',-999))
            if pos<0.75 or worst<-5 or tdd<-30 or tret<=0: continue
            rscore=40*pos+2*med+worst-0.5*abs(tdd)
            rows.append((rscore,x))
        if not rows:return None
        rows.sort(key=lambda z:z[0],reverse=True);score,x=rows[0]
        return {'name':x['name'],'research_score':round(score,6),'chosen_cfg':x.get('chosen_cfg') or {},
                'walkforward':x.get('walkforward') or {},'train':x.get('train') or {},'source_file':x.get('source_file')}

    async def refresh(self):
        try:
            files=dict(RESULT_FILES)
            if CHALLENGER_DIR.exists():
                for path in sorted(CHALLENGER_DIR.glob('*.json'),key=lambda x:x.stat().st_mtime)[-50:]:
                    files[f'challenger:{path.stem}']=path
            cands={name:self._candidate(name,path) for name,path in files.items()}
            self.state['candidates']=cands
            self.state['target_40_achieved']=any(x.get('target_40_pass') for x in cands.values())
            old_nominee=self.state.get('shadow_nominee') or {};now=time.time()
            lock_started=float(self.state.get('shadow_nominee_started_at') or 0);lock_seconds=float(POLICY['shadow_hours']*3600)
            lock_until=float(self.state.get('shadow_nominee_lock_until') or (lock_started+lock_seconds if lock_started else 0))
            locked=bool(old_nominee.get('name')) and now<lock_until
            nominee=old_nominee if locked else self._select_shadow_nominee(cands)
            if nominee and nominee.get('name')!=old_nominee.get('name'):
                hist=list(self.state.get('nomination_history') or []);hist.append({'ts':now,'from':old_nominee.get('name'),'to':nominee.get('name'),'reason':'TRAIN_WALKFORWARD_NOMINEE'})
                self.state['nomination_history']=hist[-100:];lock_started=now;lock_until=now+lock_seconds
            elif nominee and not lock_started: lock_started=now;lock_until=now+lock_seconds
            self.state['shadow_nominee_started_at']=lock_started or None;self.state['shadow_nominee_lock_until']=lock_until or None
            self.state['shadow_nominee_lock_seconds']=lock_seconds;self.state['shadow_nominee_locked']=bool(nominee and now<lock_until);self.state['shadow_nominee']=nominee
            promotion=None
            self.state['last_scan']=time.time()
            self.state['last_promotion']=promotion
            self.last_error=None
            self._save()
        except Exception as exc:
            self.last_error=str(exc)[:300]
        return self.status()

    def status(self):
        return {
            'ok':self.last_error is None,'mode':'RESEARCH_EVOLUTION',
            'policy':POLICY,'paper_champion':self.state.get('paper_champion'),
            'live_champion':self.state.get('live_champion'),
            'generation':self.state.get('generation'),
            'target_40_achieved':self.state.get('target_40_achieved',False),
            'last_scan':self.state.get('last_scan'),'last_error':self.last_error,
            'candidates':self.state.get('candidates') or {},
            'shadow_nominee':self.state.get('shadow_nominee'),
            'shadow_nominee_started_at':self.state.get('shadow_nominee_started_at'),
            'shadow_nominee_lock_until':self.state.get('shadow_nominee_lock_until'),
            'shadow_nominee_locked':self.state.get('shadow_nominee_locked',False),
            'shadow_lock_hours_left':round(max(0.0,float(self.state.get('shadow_nominee_lock_until') or 0)-time.time())/3600,2),
            'last_shadow_review':self.state.get('last_shadow_review'),
            'shadow_reviews':self.state.get('shadow_reviews') or {},
            'nomination_history':(self.state.get('nomination_history') or [])[-20:],
            'promotion_history':(self.state.get('promotion_history') or [])[-20:],
            'live_promotion_enabled':False,
        }
    async def start(self):
        if self.task and not self.task.done(): return
        self.task=asyncio.create_task(self._loop(),name='raven-evolution-manager')

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try: await self.task
            except BaseException: pass
        self.task=None

    async def _loop(self):
        while True:
            await self.refresh()
            await asyncio.sleep(max(3600.0,self.interval))

raven_evolution_manager=RavenEvolutionManager()
