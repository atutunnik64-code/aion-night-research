from __future__ import annotations
import asyncio,json,time
from pathlib import Path
from app.services.profit_branch_network import profit_branch_network
ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'profit_portfolio_allocator.json'

class ProfitPortfolioAllocator:
    def __init__(self):
        self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_refresh=None;self.state=self._load()
    def _load(self):
        base={'meta_equity':100.0,'peak':100.0,'max_dd_pct':0.0,'weights':{'cash':1.0},'last_equities':{},'history':[]}
        try:base.update(json.loads(STATE.read_text(encoding='utf-8')))
        except Exception:pass
        return base
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _branches():
        st=profit_branch_network.status().get('state') or {}
        return st.get('branches') or {}
    @staticmethod
    def _eligible(branches):
        rows=[]
        for name,b in branches.items():
            if b.get('stage')!='PORTFOLIO_ELIGIBLE' or not b.get('ok'):continue
            if b.get('live_enabled') or b.get('forbidden_style') or not b.get('future_only'):continue
            if b.get('futures_required') and not b.get('futures_promotion_ready'):continue
            rows.append(dict(b,name=name))
        return rows
    @staticmethod
    def _pick_roots(rows):
        best={}
        for b in rows:
            root=b.get('root_parent') or b['name'];cur=best.get(root)
            if cur is None or float(b.get('score') or 0)>float(cur.get('score') or 0):best[root]=b
        return list(best.values())
    @staticmethod
    def _cluster(b):
        f=str(b.get('family') or '')
        if f.startswith('options_'):return 'options'
        if f.startswith('market_neutral_idio'):return 'idio_rotation'
        if 'funding' in f:return 'funding'
        if 'microstructure' in f:return 'microstructure'
        return f or str(b.get('root_parent') or b.get('name'))
    @staticmethod
    def _target(rows):
        rows=sorted(rows,key=lambda b:float(b.get('score') or 0),reverse=True)
        if not rows:return {'cash':1.0}
        branch_cap=.35;cluster_cap=.45;base=min(branch_cap,1.0/len(rows));groups={}
        for b in rows:groups.setdefault(ProfitPortfolioAllocator._cluster(b),[]).append(b)
        w={}
        for items in groups.values():
            each=min(base,cluster_cap/len(items))
            for b in items:w[b['name']]=each
        used=sum(w.values());w['cash']=max(0.0,1.0-used);return w
    @staticmethod
    def _smooth(old,target,step=.10):
        keys=(set(old)|set(target))-{'cash'};out={}
        for k in keys:
            a=float(old.get(k,0));b=float(target.get(k,0));d=max(-step,min(step,b-a));v=max(0.0,a+d)
            if v>1e-9:out[k]=v
        risky=sum(out.values())
        if risky>1.0:
            out={k:v/risky for k,v in out.items()};risky=1.0
        out['cash']=max(0.0,1.0-risky)
        return out
    def _mark(self,branches):
        eq=float(self.state.get('meta_equity') or 100.0);last=self.state.get('last_equities') or {};w=self.state.get('weights') or {'cash':1.0}
        port_ret=0.0;contrib={}
        for name,weight in w.items():
            if name=='cash':continue
            cur=float((branches.get(name) or {}).get('equity') or 0);prev=float(last.get(name) or 0)
            r=(cur/prev-1.0) if cur>0 and prev>0 else 0.0;port_ret+=weight*r;contrib[name]=weight*r
        eq*=max(.01,1+port_ret);self.state['meta_equity']=eq
        self.state['peak']=max(float(self.state.get('peak') or eq),eq);self.state['max_dd_pct']=min(float(self.state.get('max_dd_pct') or 0),(eq/self.state['peak']-1)*100)
        return port_ret,contrib
    async def refresh(self):
        try:
            branches=self._branches();port_ret,contrib=self._mark(branches)
            elig=self._pick_roots(self._eligible(branches));target=self._target(elig);old=self.state.get('weights') or {'cash':1.0};new=self._smooth(old,target)
            self.state['weights']=new;self.state['last_equities']={k:float(v.get('equity') or 100) for k,v in branches.items()}
            hist=list(self.state.get('history') or []);hist.append({'ts':time.time(),'equity':self.state['meta_equity'],'weights':new,'target':target,'eligible':[x['name'] for x in elig],'contribution':contrib,'portfolio_return_step_pct':port_ret*100})
            self.state['history']=hist[-2000:];self._save();self.last_error=None;self.last_refresh=time.time()
        except Exception as exc:self.last_error=str(exc)[:500];self.last_refresh=time.time()
        return self.status()
    def status(self):
        eq=float(self.state.get('meta_equity') or 100.0);branches=self._branches();elig=self._pick_roots(self._eligible(branches))
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_PORTFOLIO_ALLOCATOR','future_only':True,
                'live_enabled':False,'grid':False,'martingale':False,'dca':False,'branch_cap':0.35,'cluster_cap':0.45,'max_weight_step':0.10,
                'meta_equity':round(eq,6),'meta_return_pct':round(eq-100,4),'max_dd_pct':round(float(self.state.get('max_dd_pct') or 0),4),
                'weights':self.state.get('weights') or {'cash':1.0},'eligible':[x['name'] for x in elig],
                'candidate_profit_branches':[k for k,v in branches.items() if v.get('stage')=='PROFIT_BRANCH'],
                'last_refresh':self.last_refresh,'last_error':self.last_error}
    async def start(self):
        if self.task and not self.task.done():return
        asyncio.create_task(self.refresh(),name='initial-refresh-'+self.__class__.__name__);self.task=asyncio.create_task(self._loop(),name='profit-portfolio-allocator')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None;self._save()
    async def _loop(self):
        while True:
            await asyncio.sleep(max(300.,self.interval));await self.refresh()

profit_portfolio_allocator=ProfitPortfolioAllocator()
