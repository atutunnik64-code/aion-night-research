from __future__ import annotations
from app.http_shared import SHARED_SSL_CONTEXT
import os
from app.services.private_trading import BybitPrivate, OKXPrivate

class BybitDemo(BybitPrivate):
    name='BYBIT_DEMO'
    base_url='https://api-demo.bybit.com'
    def __init__(self):
        self.key=os.getenv('AION_DEMO_BYBIT_KEY','')
        self.secret=os.getenv('AION_DEMO_BYBIT_SECRET','')
        self.passphrase=''
        self.base_url=os.getenv('AION_DEMO_BYBIT_BASE_URL','https://api-demo.bybit.com').rstrip('/')
        self.auth_ok=False; self.auth_error=None
    def configured(self):
        return bool(self.key and self.secret)
    def ready(self):
        return bool(self.configured() and self.auth_ok)

class OKXDemo(OKXPrivate):
    name='OKX_DEMO'
    def __init__(self):
        self.key=os.getenv('AION_DEMO_OKX_KEY','')
        self.secret=os.getenv('AION_DEMO_OKX_SECRET','')
        self.passphrase=os.getenv('AION_DEMO_OKX_PASSPHRASE','')
        self.base_url=os.getenv('AION_DEMO_OKX_BASE_URL','https://openapi.okx.com').rstrip('/')
        self.auth_ok=False; self.auth_error=None
    def configured(self):
        return bool(self.key and self.secret and self.passphrase)
    def ready(self):
        return bool(self.configured() and self.auth_ok)
    def _headers(self,method,path,body):
        h=super()._headers(method,path,body)
        h['x-simulated-trading']='1'
        return h

DEMO_ADAPTERS={
    'Bybit':BybitDemo(),
    'OKX':OKXDemo(),
}

def demo_readiness():
    return {
        name:{
            'configured':adapter.configured(),
            'ready':adapter.ready(),
            'auth_ok':bool(getattr(adapter,'auth_ok',False)),
            'auth_error':getattr(adapter,'auth_error',None),
            'fill_verification':True,
            'environment':'DEMO',
        }
        for name,adapter in DEMO_ADAPTERS.items()
    }

async def bybit_apply_demo_funds(funds):
    adapter=DEMO_ADAPTERS['Bybit']
    if not adapter.ready():
        return {'ok':False,'status':'DEMO_BYBIT_CREDENTIALS_MISSING'}
    import json,httpx
    allowed={'BTC':15.0,'ETH':200.0,'USDT':100000.0,'USDC':100000.0}
    items=[]
    for coin,amount in (funds or {}).items():
        coin=str(coin).upper()
        if coin not in allowed:continue
        value=max(0.0,min(float(amount),allowed[coin]))
        if value>0:items.append({'coin':coin,'amountStr':str(value)})
    if not items:return {'ok':False,'status':'NO_VALID_FUNDS'}
    path='/v5/account/demo-apply-money'
    await adapter._ensure_time()
    body=json.dumps({'adjustType':0,'utaDemoApplyMoney':items},separators=(',',':'))
    async with httpx.AsyncClient(verify=SHARED_SSL_CONTEXT, timeout=7) as c:
        r=await c.post(adapter.base_url+path,headers=adapter._headers(body),content=body)
    d=r.json()
    return {'ok':r.is_success and d.get('retCode')==0,'status':'FUNDS_APPLIED' if d.get('retCode')==0 else 'FUNDS_FAILED','data':d}


def reload_demo_credentials():
    from pathlib import Path
    env_path=Path(__file__).parents[2]/'data'/'secrets.env'
    if env_path.exists():
        for raw in env_path.read_text(encoding='utf-8-sig').splitlines():
            line=raw.strip()
            if not line or line.startswith('#') or '=' not in line: continue
            k,v=line.split('=',1); os.environ[k.strip()]=v.strip()
    b=DEMO_ADAPTERS['Bybit']; b.key=os.getenv('AION_DEMO_BYBIT_KEY',''); b.secret=os.getenv('AION_DEMO_BYBIT_SECRET',''); b.base_url=os.getenv('AION_DEMO_BYBIT_BASE_URL','https://api-demo.bybit.com').rstrip('/'); b.auth_ok=False; b.auth_error=None
    o=DEMO_ADAPTERS['OKX']; o.key=os.getenv('AION_DEMO_OKX_KEY',''); o.secret=os.getenv('AION_DEMO_OKX_SECRET',''); o.passphrase=os.getenv('AION_DEMO_OKX_PASSPHRASE','')
    o.base_url=os.getenv('AION_DEMO_OKX_BASE_URL','https://openapi.okx.com').rstrip('/'); o.auth_ok=False; o.auth_error=None
    return demo_readiness()

async def verify_demo_credentials():
    reload_demo_credentials(); out={}
    for name,adapter in DEMO_ADAPTERS.items():
        if not adapter.configured():
            adapter.auth_ok=False; adapter.auth_error='KEYS_MISSING'
            out[name]={'configured':False,'ready':False,'ok':False,'status':'KEYS_MISSING'}; continue
        try:
            balances=await adapter.balances(); adapter.auth_ok=True; adapter.auth_error=None
            out[name]={'configured':True,'ready':True,'ok':True,'status':'AUTH_OK','assets':len(balances),'nonzero_assets':sorted(balances)[:20]}
        except Exception as exc:
            adapter.auth_ok=False; adapter.auth_error=str(exc)[:300]
            out[name]={'configured':True,'ready':False,'ok':False,'status':'AUTH_FAILED','error':adapter.auth_error}
    return out
