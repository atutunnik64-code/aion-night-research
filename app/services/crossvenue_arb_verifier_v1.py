from __future__ import annotations
import asyncio,json,time
from pathlib import Path
import httpx
from app.http_shared import SHARED_SSL_CONTEXT
from app.services.crossvenue_spot_arb_cloud_v1 import crossvenue_spot_arb_cloud_v1,TRUSTED_QUOTE_VOLUME,NOTIONAL,CANONICAL_MAJOR

ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'crossvenue_arb_verifier_v1.json'
SUPPORTED_META={'Gate','KuCoin','Bitget'}
META_TTL=6*3600.0
MIN_REPEAT_HITS=3
MIN_REPEAT_SPAN_SEC=60.0
MIN_CONSENSUS_VENUES=4
MAX_PROCESS_PER_CYCLE=24

class CrossVenueArbVerifierV1:
    def __init__(self):
        self.enabled=True;self.live_enabled=False;self.interval=30.0;self.task=None;self.last_error=None;self.last_refresh=None
        try:self.state=json.loads(STATE.read_text(encoding='utf-8-sig'))
        except Exception:self.state={'version':'CROSSVENUE_ARB_VERIFIER_V2_ALL_REVIEW','started_at':time.time(),'verified':{},'rejected':{},'watch':{},'scan_count':0}
        self.state['version']='CROSSVENUE_ARB_VERIFIER_V2_ALL_REVIEW';self.meta_cache={}
    def _save(self):
        STATE.parent.mkdir(parents=True,exist_ok=True);tmp=STATE.with_suffix(STATE.suffix+'.tmp')
        tmp.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8');tmp.replace(STATE)
    @staticmethod
    def _addr(v):
        s=str(v or '').strip().lower()
        if len(s)<8 or s in {'null','none','native','-'}:return None
        return s
    def _extract_addresses(self,obj):
        out=set()
        def walk(x):
            if isinstance(x,dict):
                for k,v in x.items():
                    lk=str(k).lower()
                    if lk in {'contractaddress','contract_address','contract','addr','address','tokenaddress','token_address'}:
                        a=self._addr(v)
                        if a:out.add(a)
                    elif isinstance(v,(dict,list)):walk(v)
            elif isinstance(x,list):
                for z in x:walk(z)
        walk(obj);return sorted(out)
    async def _metadata(self,c,venue,base):
        key=(venue,base);now=time.time();cached=self.meta_cache.get(key)
        if cached and now-float(cached['ts'])<META_TTL:return cached
        rec={'ts':now,'venue':venue,'base':base,'supported':venue in SUPPORTED_META,'ok':False,'addresses':[],'error':None}
        if venue not in SUPPORTED_META:
            self.meta_cache[key]=rec;return rec
        try:
            if venue=='Gate':
                r=await c.get(f'https://api.gateio.ws/api/v4/spot/currencies/{base}')
                data=r.json() if r.is_success else {}
            elif venue=='KuCoin':
                r=await c.get(f'https://api.kucoin.com/api/v3/currencies/{base}')
                data=(r.json().get('data') or {}) if r.is_success else {}
            else:
                r=await c.get('https://api.bitget.com/api/v2/spot/public/coins',params={'coin':base})
                d=r.json() if r.is_success else {};data=d.get('data') or []
            rec['addresses']=self._extract_addresses(data);rec['ok']=bool(rec['addresses']);rec['http_ok']=bool(r.is_success)
        except Exception as exc:rec['error']=str(exc)[:160]
        self.meta_cache[key]=rec;return rec
    @staticmethod
    def _sig(row):return f"{row.get('base')}:{row.get('buy_venue')}>{row.get('sell_venue')}"
    def _queue_groups(self):
        # Verify the explicit high-edge queue, every REVIEW_REQUIRED event,
        # and every positive non-major route for venues where public contract
        # metadata is available. The strict $100 portfolio consumes only
        # CONTRACT_ADDRESS_MATCH results for non-majors.
        q=list(crossvenue_spot_arb_cloud_v1.state.get('verification_queue') or [])[-1000:]
        ev=list(crossvenue_spot_arb_cloud_v1.state.get('events') or [])[-1500:]
        q.extend(
            x for x in ev
            if float(x.get('execution_net_pct') or 0)>0
            and (
                x.get('identity_confidence')=='REVIEW_REQUIRED'
                or (
                    str(x.get('base') or '') not in CANONICAL_MAJOR
                    and str(x.get('buy_venue') or '') in SUPPORTED_META
                    and str(x.get('sell_venue') or '') in SUPPORTED_META
                )
            )
        )
        groups={};seen=set()
        for x in q:
            sig=self._sig(x);ts=float(x.get('ts') or 0);dedupe=(sig,ts)
            if not sig or dedupe in seen:continue
            seen.add(dedupe);g=groups.setdefault(sig,{'rows':[],'first_ts':ts,'last_ts':0.0})
            g['rows'].append(x);g['first_ts']=min(g['first_ts'] or ts,ts);g['last_ts']=max(g['last_ts'],ts)
        return groups
    async def refresh(self):
        now=time.time()
        try:
            groups=self._queue_groups();market=crossvenue_spot_arb_cloud_v1.market or {}
            pending=[]
            for sig,g in groups.items():
                if sig in self.state.get('verified',{}) or sig in self.state.get('rejected',{}):continue
                last=g['rows'][-1];pending.append((float(last.get('execution_net_pct') or 0),sig,g,last))
            pending.sort(reverse=True,key=lambda x:x[0]);processed=0;new_verified=0;new_rejected=0
            async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT,timeout=httpx.Timeout(8.0),follow_redirects=True,headers={'User-Agent':'AION-Research/1.0'}) as c:
                for _,sig,g,row in pending[:MAX_PROCESS_PER_CYCLE]:
                    processed+=1;base=str(row.get('base') or '');buy=str(row.get('buy_venue') or '');sell=str(row.get('sell_venue') or '')
                    bm,sm=await asyncio.gather(self._metadata(c,buy,base),self._metadata(c,sell,base))
                    ba=set(bm.get('addresses') or []);sa=set(sm.get('addresses') or []);common=sorted(ba&sa)
                    hits=len(g['rows']);span=max(0.0,float(g['last_ts'])-float(g['first_ts']));vm=market.get(base) or {}
                    bq=float((vm.get(buy) or {}).get('quote_volume') or row.get('buy_quote_volume') or 0)
                    sq=float((vm.get(sell) or {}).get('quote_volume') or row.get('sell_quote_volume') or 0)
                    decision='WATCH';method=None
                    if bm.get('supported') and sm.get('supported') and bm.get('ok') and sm.get('ok'):
                        if common:decision='VERIFIED';method='CONTRACT_ADDRESS_MATCH'
                        else:decision='REJECTED';method='CONTRACT_ADDRESS_MISMATCH'
                    elif hits>=MIN_REPEAT_HITS and span>=MIN_REPEAT_SPAN_SEC and len(vm)>=MIN_CONSENSUS_VENUES and bq>=TRUSTED_QUOTE_VOLUME and sq>=TRUSTED_QUOTE_VOLUME and float(row.get('execution_net_pct') or 0)>0:
                        decision='VERIFIED';method='STRONG_MARKET_CONSENSUS_PAPER_ONLY'
                    rec={'verified_at':now,'signature':sig,'base':base,'buy_venue':buy,'sell_venue':sell,'method':method,
                         'repeat_hits':hits,'repeat_span_sec':span,'venue_consensus_count':len(vm),'buy_quote_volume':bq,'sell_quote_volume':sq,
                         'gross_depth_pct':float(row.get('gross_depth_pct') or 0),'execution_net_pct':float(row.get('execution_net_pct') or 0),
                         'conservative_net_pct':float(row.get('conservative_net_pct') or 0),'paper_edge_quote':NOTIONAL*float(row.get('execution_net_pct') or 0)/100.0,
                         'buy_addresses':sorted(ba),'sell_addresses':sorted(sa),'common_addresses':common,'live_eligible':False}
                    if decision=='VERIFIED':self.state.setdefault('verified',{})[sig]=rec;self.state.setdefault('watch',{}).pop(sig,None);new_verified+=1
                    elif decision=='REJECTED':self.state.setdefault('rejected',{})[sig]=rec;self.state.setdefault('watch',{}).pop(sig,None);new_rejected+=1
                    else:
                        rec['reason']='NEEDS_MORE_IDENTITY_OR_REPEAT_EVIDENCE';self.state.setdefault('watch',{})[sig]=rec
            self.state['scan_count']=int(self.state.get('scan_count') or 0)+1;self.state['last_scan_ts']=now
            self.state['candidate_signature_count']=len(groups);self.state['last_processed']=processed;self.state['last_new_verified']=new_verified;self.state['last_new_rejected']=new_rejected;self._save()
            self.last_error=None
        except Exception as exc:
            self.last_error=str(exc)[:500];self.state['last_error']=self.last_error;self.state['last_error_ts']=time.time()
            try:self._save()
            except Exception:pass
        self.last_refresh=now;return self.status()
    def status(self):
        verified=list((self.state.get('verified') or {}).values());rejected=list((self.state.get('rejected') or {}).values());watch=list((self.state.get('watch') or {}).values())
        edge=sum(float(x.get('paper_edge_quote') or 0) for x in verified)
        return {'ok':self.last_error is None,'strategy':'CROSSVENUE_ARB_VERIFIER_V2_ALL_REVIEW','mode':'PAPER_IDENTITY_VERIFICATION','paper_only':True,'live_enabled':False,
                'candidate_signature_count':self.state.get('candidate_signature_count',0),'verified_count':len(verified),'rejected_count':len(rejected),'watch_count':len(watch),'verified_paper_edge_quote':round(edge,6),
                'last_processed':self.state.get('last_processed',0),'last_new_verified':self.state.get('last_new_verified',0),'last_new_rejected':self.state.get('last_new_rejected',0),
                'recent_verified':verified[-20:],'recent_rejected':rejected[-20:],'last_error':self.last_error,
                'policy':{'all_review_required_positive_events_verified':True,'high_edge_not_discarded':True,'contract_match_preferred':True,'market_consensus_fallback_paper_only':True,'no_live_orders':True}}
    async def start(self):
        if self.task and not self.task.done():return
        await self.refresh();self.task=asyncio.create_task(self._loop(),name='crossvenue-arb-verifier-v2')
    async def stop(self):
        if self.task and not self.task.done():self.task.cancel();await asyncio.gather(self.task,return_exceptions=True)
        self.task=None
    async def _loop(self):
        while True:
            await asyncio.sleep(self.interval)
            if self.enabled:await self.refresh()

crossvenue_arb_verifier_v1=CrossVenueArbVerifierV1()
