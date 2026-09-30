from __future__ import annotations
import time
from app.services.hot_book_cache import hot_book_cache

class ShadowImpactGuard:
    def __init__(self):
        self.last={}; self.marks=0; self.blocked=0; self.released=0

    @staticmethod
    def _sig(signature,legs):
        if signature:return str(signature)
        return '|'.join(f"{x.get('venue')}:{x.get('symbol')}:{x.get('side')}" for x in legs or [])

    @staticmethod
    def _fingerprint(leg):
        hit=hot_book_cache.get(leg.get('venue'),leg.get('symbol'),hot_book_cache.execution_stale_after)
        if not hit:return None
        bids,asks,_=hit
        def compact(rows):
            return tuple((round(float(p),12),round(float(q),12)) for p,q in list(rows or [])[:5])
        return compact(bids),compact(asks)

    def ready(self,signature,legs):
        sig=self._sig(signature,legs); prior=self.last.get(sig)
        current=[self._fingerprint(x) for x in legs or []]
        if not prior:return {'ok':True,'status':'FIRST_CYCLE','signature':sig}
        if not current or any(x is None for x in current):return {'ok':False,'status':'BOOK_UNAVAILABLE','signature':sig}
        changed=[cur!=old for cur,old in zip(current,prior.get('fingerprints') or [])]
        if len(changed)!=len(current) or not all(changed):
            self.blocked+=1
            return {'ok':False,'status':'BOOK_NOT_REFRESHED','signature':sig,'changed':changed,
                    'age_since_fill_ms':round((time.monotonic()-float(prior.get('mono') or time.monotonic()))*1000,1)}
        self.released+=1
        return {'ok':True,'status':'BOOK_REFRESHED','signature':sig,'changed':changed}

    def mark(self,signature,legs):
        sig=self._sig(signature,legs); fingerprints=[self._fingerprint(x) for x in legs or []]
        if fingerprints and all(x is not None for x in fingerprints):
            self.last[sig]={'fingerprints':fingerprints,'mono':time.monotonic(),'wall':time.time()}; self.marks+=1
        return {'signature':sig,'marked':bool(fingerprints and all(x is not None for x in fingerprints))}

    def status(self):
        return {'tracked_routes':len(self.last),'marks':self.marks,'blocked_reuses':self.blocked,
                'released_after_refresh':self.released,'rule':'BOTH_LEGS_TOP5_MUST_CHANGE'}

shadow_impact_guard=ShadowImpactGuard()
