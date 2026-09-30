from __future__ import annotations
import json,time
from pathlib import Path
from app.services.basis_funding import basis_funding_scanner

VENUES=['Binance','Bybit','OKX','Bitget','HTX']
REQUIRED_FEATURES=[
    'perp_mark_price','index_price','basis_pct','funding_rate',
    'open_interest','perp_volume','taker_imbalance','liquidation_context',
]
EXECUTION_COSTS=[
    'maker_taker_fee','slippage','funding_accrual','basis_drift',
    'mark_index_divergence','liquidation_buffer',
]
NATIVE_TOKENS=('funding','perp','crowding','liquidation','finite_arbitrage')
ROOT=Path(__file__).parents[2]
REGISTRY=ROOT/'data'/'futures_validation_registry.json'

class FuturesExtensionPolicy:
    def __init__(self):
        self.enabled=True
        self.version='FUTURES_STANDARD_V1'
        self.live_enabled=False
        self.created_at=time.time()

    @staticmethod
    def _native(family:str)->bool:
        f=str(family or '').lower()
        if f.startswith(('crossvenue_oi_migration_','crossvenue_taker_')):
            return True
        return any(x in f for x in NATIVE_TOKENS)

    @staticmethod
    def _validation(name:str)->dict:
        try:
            x=json.loads(REGISTRY.read_text(encoding='utf-8'))
            return dict((x.get('branches') or {}).get(name) or {})
        except Exception:
            return {}

    def profile(self,name:str,family:str,strategy_status:dict|None=None)->dict:
        s=strategy_status or {}
        if str(family or '').lower().startswith('moex_'):
            return {
                'policy_version':self.version,'required':True,'stage':'MOEX_FORTS_RESEARCH',
                'native_futures_logic':True,'markets':['MOEX_FORTS'],'venues':['MOEX'],
                'required_features':['settle_price','open_interest','volume','contract_expiry','last_rub'],
                'execution_costs':['broker_fee','exchange_fee','slippage','variation_margin'],
                'paper_only':True,'live_enabled':False,'default_effective_leverage':1.0,
                'research_leverage_cap':1.0,'funding_must_be_charged':False,
                'liquidation_model_required_above_1x':False,'spot_baseline_required':False,
                'perp_parallel_test_required':False,'cross_market_test_required':False,
                'scanner_market_count':int(s.get('sample_count') or 0),'scanner_last_refresh':(s.get('latest') or {}).get('source_ts'),
                'validation':{},'validation_inherited_from':None,'technical_ready':True,'promotion_ready':False,
            }
        native=self._native(family)
        validation=self._validation(name);validation_inherited_from=None
        if not validation and ':' in name:
            root=name.split(':',1)[0];validation=self._validation(root)
            if validation:validation_inherited_from=root
        validated=bool(validation.get('validated'))
        technical_ready=bool(native or validated or s.get('futures_evaluated'))
        if validation_inherited_from:
            promotion_ready=False
        elif validation:
            promotion_ready=bool(validated and validation.get('strategy_promotion_eligible') is True)
        else:
            # Native futures means the market plumbing is ready, not that the strategy evidence is sufficient.
            # Promotion always requires an explicit validation record or an explicit evaluated+approved gate.
            promotion_ready=bool(s.get('futures_evaluated') and s.get('futures_promotion_eligible') is True)
        base_status=basis_funding_scanner.status()
        market_count=int(base_status.get('market_count') or 0)
        last_refresh=base_status.get('last_refresh')
        stage=('NATIVE_FUTURES' if native and not validation else ('PERP_DELTA_HEDGE_READY' if validated and str(family).startswith('options_') and validation.get('perp_delta_hedge_ready') else ('PERP_VALIDATED_RESEARCH' if validated else 'PERP_PARALLEL_REQUIRED')))
        return {
            'policy_version':self.version,'required':True,'stage':stage,
            'native_futures_logic':native,'markets':['USDT_PERPETUAL'],
            'venues':list(VENUES),'required_features':list(REQUIRED_FEATURES),
            'execution_costs':list(EXECUTION_COSTS),'paper_only':True,
            'live_enabled':False,'default_effective_leverage':1.0,
            'research_leverage_cap':3.0,'funding_must_be_charged':True,
            'liquidation_model_required_above_1x':True,'spot_baseline_required':True,
            'perp_parallel_test_required':not native,'cross_market_test_required':True,
            'scanner_market_count':market_count,'scanner_last_refresh':last_refresh,
            'validation':validation,'validation_inherited_from':validation_inherited_from,
            'technical_ready':technical_ready,'promotion_ready':promotion_ready,
        }

    def variant_profile(self,parent:str,family:str='')->dict:
        native=self._native(family) or any(x in str(parent).lower() for x in NATIVE_TOKENS)
        validation=self._validation(parent)
        validated=bool(validation.get('validated'))
        technical_ready=bool(native or validated)
        promotion_ready=False  # every spawned child must pass its own futures research gate
        return {
            'policy_version':self.version,'required':True,'parent':parent,
            'native_futures_logic':native,'markets':['USDT_PERPETUAL'],'venues':list(VENUES),
            'required_features':list(REQUIRED_FEATURES),'execution_costs':list(EXECUTION_COSTS),
            'spot_baseline_required':True,'perp_parallel_test_required':not native,
            'cross_market_test_required':True,'default_effective_leverage':1.0,
            'research_leverage_cap':3.0,'paper_only':True,'live_enabled':False,
            'validation':validation,'technical_ready':technical_ready,'promotion_ready':promotion_ready,
        }

    def status(self)->dict:
        b=basis_funding_scanner.status()
        try:
            reg=json.loads(REGISTRY.read_text(encoding='utf-8'))
            validated=sorted(k for k,v in (reg.get('branches') or {}).items() if v.get('validated'))
        except Exception:
            validated=[]
        return {
            'ok':True,'enabled':self.enabled,'version':self.version,
            'mode':'RESEARCH_SHADOW_ONLY','live_enabled':False,
            'venues':list(VENUES),'required_features':list(REQUIRED_FEATURES),
            'execution_costs':list(EXECUTION_COSTS),'validated_research_branches':validated,
            'basis_scanner':{
                'ok':b.get('ok'),'market_count':b.get('market_count'),
                'last_refresh':b.get('last_refresh'),'last_error':b.get('last_error'),
            },
        }

futures_extension_policy=FuturesExtensionPolicy()
