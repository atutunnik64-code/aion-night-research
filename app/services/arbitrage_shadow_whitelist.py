from __future__ import annotations
import json
from pathlib import Path

ROOT=Path(__file__).parents[2]
PATH=ROOT/'data'/'arbitrage_shadow_whitelist_v1.json'

class ArbitrageShadowWhitelist:
    def __init__(self):
        self.enabled=True
        self.modes={'CLASSIC','LATENCY'}
        self.allowed_tiers={'EXECUTION_PROVEN_SHADOW','RECHECK_PROVEN_SHADOW'}
        self._mtime=None; self._routes={}; self.last_error=None

    @staticmethod
    def _norm(mode,sig):
        s=str(sig or ''); p=str(mode)+'|'
        return s[len(p):] if s.startswith(p) else s

    def _load(self):
        try:
            mt=PATH.stat().st_mtime
            if self._mtime==mt:return
            data=json.loads(PATH.read_text(encoding='utf-8'))
            self._routes={(str(x.get('mode')),str(x.get('route'))):x for x in (data.get('routes') or [])}
            self._mtime=mt; self.last_error=None
        except Exception as exc:
            self.last_error=str(exc)[:250]; self._routes={}; self._mtime=None
    def allow(self,row,mode,environment):
        if not self.enabled or str(environment)!='DEMO' or str(mode) not in self.modes:
            return True,'SHADOW_WHITELIST_NOT_APPLICABLE'
        self._load()
        if self.last_error:
            return False,'SHADOW_WHITELIST_UNAVAILABLE'
        sig=self._norm(mode,(row or {}).get('signature'))
        rec=self._routes.get((str(mode),sig))
        if not rec:return False,'SHADOW_WHITELIST_MISS'
        tier=str(rec.get('tier') or '')
        if tier not in self.allowed_tiers:return False,'SHADOW_WHITELIST_TIER_LOW'
        return True,tier

    def status(self):
        self._load(); counts={}
        for x in self._routes.values():
            t=str(x.get('tier') or 'UNKNOWN');counts[t]=counts.get(t,0)+1
        return {'enabled':self.enabled,'modes':sorted(self.modes),'allowed_tiers':sorted(self.allowed_tiers),
                'routes':len(self._routes),'counts':counts,'path':str(PATH),'last_error':self.last_error}

arbitrage_shadow_whitelist=ArbitrageShadowWhitelist()
