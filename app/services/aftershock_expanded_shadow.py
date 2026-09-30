from __future__ import annotations
import json,time
from pathlib import Path
from app.services.aftershock_v2_shadow import AftershockV2Shadow,_blank

ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'aftershock_expanded_shadow.json'
CORE=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','BNBUSDT','DOGEUSDT','ADAUSDT','LINKUSDT','AVAXUSDT','LTCUSDT']
EXTRA=['SUIUSDT','NEARUSDT','APTUSDT','INJUSDT','TIAUSDT','SEIUSDT','WIFUSDT','ARBUSDT','OPUSDT','AAVEUSDT','RUNEUSDT','JUPUSDT']

class AftershockExpandedShadow(AftershockV2Shadow):
    def __init__(self):
        self.symbols=CORE+EXTRA
        self.enabled=True;self.interval=900.;self.task=None;self.last_error=None;self.last_refresh=None;self.state=self._load()
    def _load(self):
        try:return json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:return _blank()
    def _save(self):STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')
    def _refresh_sync(self):
        frames={}
        for sym in self.symbols:
            try:
                z,f=self._frame(sym);frames[sym]=z;self._process_symbol(sym,z,f)
            except Exception as exc:
                self.state.setdefault('symbol_errors',{})[sym]=str(exc)[:180]
        self._resolve(frames);self._save();return self.state
    def status(self):
        s=super().status();s.update({'strategy':'aftershock_expanded_v1','experimental_universe':True,
            'universe_size':len(self.symbols),'core_symbols':len(CORE),'extra_symbols':len(EXTRA),
            'promotion_eligible':False,'live_enabled':False,'paper_only':True,
            'evidence_note':'same frozen v2 rule; expanded universe requires its own future-only evidence'})
        s['symbol_errors']=self.state.get('symbol_errors') or {}
        return s

aftershock_expanded_shadow=AftershockExpandedShadow()