from __future__ import annotations
import asyncio, json, time
from pathlib import Path
from app.services.basis_funding import basis_funding_scanner
from app.services.live_executor import live_executor

ROOT=Path(__file__).parents[2]; STATE=ROOT/'data'/'basis_funding_paper.json'

class BasisFundingPaper:
    def __init__(self):
        self.enabled=True; self.interval=60.0; self.notional=25.0; self.max_open=2; self.max_hold_hours=72.0
        self.target_close_pct=.12; self.stop_pct=-1.5; self.task=None; self.last_refresh=None; self.last_error=None; self.latest={}
        self.state=self._load()
    def _load(self):
        base={'closed_pnl':0.0,'deploy_closed_pnl':0.0,'positions':{},'closed':[]}
        try:base.update(json.loads(STATE.read_text(encoding='utf-8')))
        except Exception:pass
        return base
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _sig(row):return f"{row.get('venue')}:{row.get('base')}:USDT"

    def _mark(self,pos,row,now):
        qty=float(pos['qty']); sbid=float(row.get('spot_bid') or 0); pask=float(row.get('perp_ask') or 0)
        if sbid<=0 or pask<=0:return None
        sf=float(pos['spot_fee']); pf=float(pos['perp_fee']); last=float(pos.get('last_mark') or now)
        hours=max(0.0,(now-last)/3600.0); fr=float(row.get('funding_rate_pct') or 0)/100.0
        perp_mid=(float(row.get('perp_bid') or pask)+pask)/2.0
        pos['funding']=float(pos.get('funding') or 0)+qty*perp_mid*fr*(hours/8.0); pos['last_mark']=now
        gross=qty*(sbid-float(pos['spot_entry']))+qty*(float(pos['perp_entry'])-pask)
        exit_fees=qty*sbid*sf+qty*pask*pf; pnl=gross-float(pos['entry_fees'])-exit_fees+float(pos['funding'])
        pct=pnl/float(pos['notional'])*100.0; basis=(pask/max(sbid,1e-12)-1.0)*100.0
        return pnl,pct,basis
    def _close(self,sig,pos,pnl,pct,basis,reason,now):
        rec={'ts':now,'signature':sig,'venue':pos['venue'],'base':pos['base'],'pnl':round(pnl,8),'pnl_pct':round(pct,5),
             'funding':round(float(pos.get('funding') or 0),8),'close_basis_pct':round(basis,6),'reason':reason,'deployable':bool(pos.get('deployable'))}
        self.state['closed_pnl']=float(self.state.get('closed_pnl') or 0)+pnl
        if pos.get('deployable'):self.state['deploy_closed_pnl']=float(self.state.get('deploy_closed_pnl') or 0)+pnl
        closed=list(self.state.get('closed') or []); closed.append(rec); self.state['closed']=closed[-200:]
        self.state['positions'].pop(sig,None)

    async def refresh(self):
        try:
            if not basis_funding_scanner.market:await basis_funding_scanner.refresh()
            now=time.time(); market=basis_funding_scanner.market; live=live_executor.status()
            quarantine=set(live.get('live_quarantined_venues') or []); allowed=set(live.get('verified_ready') or [])-quarantine
            marks={}; positions=self.state.get('positions') or {}
            for sig,pos in list(positions.items()):
                row=market.get(sig)
                if not row:continue
                m=self._mark(pos,row,now)
                if not m:continue
                pnl,pct,basis=m; marks[sig]={'pnl':pnl,'pct':pct,'basis':basis}
                age=(now-float(pos['opened_at']))/3600.0; entry_basis=float(pos.get('entry_raw_basis_pct') or 0)
                reason=None
                if pct<=self.stop_pct:reason='STOP'
                elif age>=self.max_hold_hours:reason='TIME_LIMIT'
                elif pct>=self.target_close_pct and basis<=max(.15,entry_basis*.35):reason='CONVERGENCE'
                if reason:self._close(sig,pos,pnl,pct,basis,reason,now); marks.pop(sig,None)
            positions=self.state.get('positions') or {}
            last_closed={x.get('signature'):float(x.get('ts') or 0) for x in (self.state.get('closed') or [])[-100:]}
            for row in basis_funding_scanner.rows:
                if len(positions)>=self.max_open:break
                sig=self._sig(row)
                if sig in positions or now-last_closed.get(sig,0)<21600:continue
                edge=float(row.get('entry_basis_net_pct') or 0); status=str(row.get('status') or '')
                if edge<.05 and status!='FUNDING_CARRY':continue
                spot=float(row.get('spot_ask') or 0); perp=float(row.get('perp_bid') or 0)
                if spot<=0 or perp<=0:continue
                qty=self.notional/spot; sf=float(row.get('spot_fee_pct') or 0)/100.0; pf=float(row.get('perp_fee_pct') or 0)/100.0
                venue=str(row.get('venue') or ''); deployable=venue in allowed and venue not in quarantine
                positions[sig]={'venue':venue,'base':row.get('base'),'notional':self.notional,'qty':qty,'spot_entry':spot,'perp_entry':perp,
                                'spot_fee':sf,'perp_fee':pf,'entry_fees':self.notional*sf+qty*perp*pf,'funding':0.0,
                                'opened_at':now,'last_mark':now,'entry_raw_basis_pct':float(row.get('raw_basis_pct') or 0),
                                'entry_net_pct':edge,'deployable':deployable}
            open_pnl=0.0; deploy_open=0.0; open_rows=[]
            for sig,pos in positions.items():
                row=market.get(sig); m=self._mark(pos,row,now) if row else None
                if not m:continue
                pnl,pct,basis=m; open_pnl+=pnl
                if pos.get('deployable'):deploy_open+=pnl
                open_rows.append({'signature':sig,'venue':pos['venue'],'base':pos['base'],'pnl':round(pnl,8),'pnl_pct':round(pct,5),
                                  'basis_pct':round(basis,6),'funding':round(float(pos.get('funding') or 0),8),'deployable':bool(pos.get('deployable'))})
            closed=list(self.state.get('closed') or []); dep_closed=sum(1 for x in closed if x.get('deployable'))
            research_eq=100+float(self.state.get('closed_pnl') or 0)+open_pnl; deploy_eq=100+float(self.state.get('deploy_closed_pnl') or 0)+deploy_open
            self.latest={'mode':'PAPER_SHADOW','research_equity':round(research_eq,6),'research_return_pct':round(research_eq-100,4),
                         'deployable_equity':round(deploy_eq,6),'deployable_return_pct':round(deploy_eq-100,4),
                         'open_count':len(positions),'deployable_open_count':sum(1 for x in positions.values() if x.get('deployable')),
                         'closed_count':len(closed),'deployable_closed_count':dep_closed,'allocatable':bool(dep_closed>=5),
                         'allowed_venues':sorted(allowed),'quarantined_venues':sorted(quarantine),'open_positions':open_rows,'recent_closed':closed[-20:]}
            self._save(); self.last_refresh=now; self.last_error=None
        except Exception as exc:self.last_error=str(exc)[:300]
        return self.status()
    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'PAPER_SHADOW','interval_seconds':self.interval,
                'last_refresh':self.last_refresh,'last_error':self.last_error,'latest':self.latest}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='basis-funding-paper')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        await asyncio.sleep(12.0)
        while True:
            await self.refresh(); await asyncio.sleep(max(30.0,self.interval))

basis_funding_paper=BasisFundingPaper()