from __future__ import annotations
import json,time
from pathlib import Path
from app.services.risk_manager import risk_manager
from app.services.execution_engine import execution_engine

PATH=Path(__file__).parents[2]/'data'/'autopilot.json'
DEFAULT={
 'enabled':True,
 'strategies':{'FULL_NOW':True,'BATCH_READY':True,'CLASSIC':True,'LATENCY':True},
 'min_sustainable_net_pct':0.0,
 'max_notional':1000.0,
 'max_daily_loss':50.0,
 'max_trades_per_hour':120,
 'inventory_reserve_pct':15.0,
 'hot_cycle_cooldown_seconds':1.0,
}

class AutoPilot:
    def __init__(self):
        self.cfg=dict(DEFAULT); self.cfg['strategies']=dict(DEFAULT['strategies']); self.updated_at=time.time(); self._load(); self.apply()
    def _load(self):
        try:
            raw=json.loads(PATH.read_text(encoding='utf-8')); self.cfg.update({k:v for k,v in raw.items() if k in DEFAULT})
            if isinstance(raw.get('strategies'),dict): self.cfg['strategies'].update(raw['strategies'])
        except Exception: pass
    def _save(self):
        PATH.parent.mkdir(parents=True,exist_ok=True)
        PATH.write_text(json.dumps({**self.cfg,'updated_at':self.updated_at},ensure_ascii=False,indent=2),encoding='utf-8')
    def apply(self):
        execution_engine.enabled=bool(self.cfg.get('enabled',True))
        execution_engine.max_notional=max(10.0,float(self.cfg.get('max_notional',1000)))
        risk_manager.max_notional=execution_engine.max_notional
        risk_manager.min_net_pct=max(0.0,float(self.cfg.get('min_sustainable_net_pct',0.0)))
        risk_manager.max_daily_loss=max(1.0,float(self.cfg.get('max_daily_loss',50)))
        risk_manager.max_trades_per_hour=max(1,int(self.cfg.get('max_trades_per_hour',120)))
        hot_cd=max(0.25,min(30.0,float(self.cfg.get('hot_cycle_cooldown_seconds',1.0))))
        risk_manager.cooldown_by_mode['CLASSIC']=hot_cd; risk_manager.cooldown_by_mode['LATENCY']=hot_cd
        try:
            from app.services.inventory_guard import inventory_guard
            inventory_guard.reserve_pct=max(0.0,min(90.0,float(self.cfg.get('inventory_reserve_pct',15))))
        except Exception: pass
    def allow_strategy(self,mode):
        return bool(self.cfg.get('enabled',True) and self.cfg.get('strategies',{}).get(mode,True))
    def update(self,payload):
        if 'enabled' in payload:self.cfg['enabled']=bool(payload['enabled'])
        for k in ('min_sustainable_net_pct','max_notional','max_daily_loss','max_trades_per_hour','inventory_reserve_pct','hot_cycle_cooldown_seconds'):
            if k in payload:self.cfg[k]=payload[k]
        if isinstance(payload.get('strategies'),dict):self.cfg['strategies'].update({k:bool(v) for k,v in payload['strategies'].items() if k in self.cfg['strategies']})
        self.updated_at=time.time(); self.apply(); self._save(); return self.status()
    def status(self):
        return {**self.cfg,'updated_at':self.updated_at,'risk_pause_until':risk_manager.failure_pause_until,'consecutive_failures':risk_manager.consecutive_failures}

autopilot=AutoPilot()
