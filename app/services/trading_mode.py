from __future__ import annotations
import json,time
from pathlib import Path

PATH=Path(__file__).parents[2]/'data'/'trading_mode.json'
ALLOWED={'PAPER','DEMO','LIVE'}

class TradingMode:
    def __init__(self):
        self.mode='PAPER'; self.updated_at=time.time(); self._load()
    def _load(self):
        try:
            raw=json.loads(PATH.read_text(encoding='utf-8'))
            mode=str(raw.get('mode') or 'PAPER').upper()
            if mode in ALLOWED:self.mode=mode
        except Exception:pass
    def _save(self):
        PATH.parent.mkdir(parents=True,exist_ok=True)
        PATH.write_text(json.dumps({'mode':self.mode,'updated_at':self.updated_at},indent=2),encoding='utf-8')
    def set(self,mode,confirmation=''):
        mode=str(mode or '').upper()
        if mode not in ALLOWED:return {'ok':False,'status':'INVALID_MODE',**self.status()}
        if mode=='LIVE' and confirmation!='ENABLE LIVE MODE':
            return {'ok':False,'status':'LIVE_CONFIRMATION_REQUIRED',**self.status()}
        self.mode=mode; self.updated_at=time.time(); self._save()
        return {'ok':True,'status':'MODE_UPDATED',**self.status()}
    def status(self):
        from app.services.live_executor import live_executor
        from app.services.demo_executor import demo_executor
        effective='PAPER_AUTO'
        if self.mode=='DEMO':
            effective='DEMO_AUTO' if demo_executor.armed() else 'DEMO_WAITING_KEYS'
        elif self.mode=='LIVE':
            effective='LIVE_AUTO' if live_executor.armed() else 'LIVE_LOCKED'
        return {'mode':self.mode,'effective_mode':effective,'updated_at':self.updated_at,
                'demo':demo_executor.status(),'live':live_executor.status()}

trading_mode=TradingMode()
