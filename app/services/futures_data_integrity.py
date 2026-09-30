from __future__ import annotations
import json,time
from pathlib import Path
import pandas as pd
from app.services.futures_forward_worker import futures_forward_worker
from app.services.idio_skew_rotation_shadow import idio_skew_rotation_shadow
from app.services.crowding_dislocation_v2_shadow import crowding_dislocation_v2_shadow

ROOT=Path(__file__).parents[2]
INVALID=ROOT/'data'/'futures_forward_invalidations_v1.jsonl'

class FuturesDataIntegrity:
    version='FUTURES_DATA_INTEGRITY_V1'
    @staticmethod
    def _ts(x):
        if x is None:return None
        try:
            if isinstance(x,(int,float)) and float(x)>1e12:return pd.Timestamp(int(x),unit='ms',tz='UTC')
            return pd.Timestamp(x)
        except Exception:return None
    @staticmethod
    def _lag(canon,x):
        t=FuturesDataIntegrity._ts(x)
        if canon is None or t is None:return None
        return float((canon-t)/pd.Timedelta(hours=8))
    def status(self):
        fw=futures_forward_worker.status();tr=fw.get('tracker') or {};v42=fw.get('v42_tracker') or {}
        canon=self._ts(tr.get('data_end'));v42bar=v42.get('data_end');v42lag=self._lag(canon,v42bar)
        idio=idio_skew_rotation_shadow.status();crowd=crowding_dislocation_v2_shadow.status()
        ib=(idio.get('latest') or {}).get('bar_ts');cb=(crowd.get('latest') or {}).get('bar_ts')
        ilag=self._lag(canon,ib);clag=self._lag(canon,cb)
        invalid=0
        if INVALID.exists():
            invalid=sum(1 for x in INVALID.read_text(encoding='utf-8').splitlines() if x.strip())
        checks={
            'canonical_full_8h_only':bool(tr.get('full_8h_only')),
            'canonical_parameters_locked':bool(tr.get('parameters_locked')),
            'canonical_not_live':not bool(tr.get('live_enabled')),
            'idio_not_ahead_of_canonical':ilag is None or ilag>=-1e-9,
            'crowding_not_ahead_of_canonical':clag is None or clag>=-1e-9,
            'v42_not_ahead_of_canonical':v42lag is None or v42lag>=-1e-9,
            'v42_full_8h_only':bool(v42.get('full_8h_only')),
            'v42_parameters_locked':bool(v42.get('parameters_locked')),
            'v42_crossvenue_funding_signal':set(v42.get('funding_signal_sources') or [])=={'binance','bybit'},
            'partial_records_tombstoned':invalid>=int(tr.get('invalidated_records') or 0),
        }
        ok=all(checks.values())
        return {'ok':ok,'version':self.version,'mode':'RESEARCH_DATA_GUARD','live_enabled':False,
                'canonical_source':'BINANCE_USDT_PERPETUAL_EXACT','canonical_closed_8h_bar':str(canon) if canon is not None else None,
                'invalidated_partial_records':invalid,'checks':checks,
                'branches':{
                    'directional_v103':{'source':'EXACT_BINANCE_PERP_FORWARD','lag_8h_bars':0.0,'promotion_source':True},
                    'idio_skew':{'source':'EXACT_BINANCE_PERP_DIRECT','bar_ts':ib,'lag_8h_bars':ilag,'promotion_source':True},
                    'crowding_dislocation':{'source':'BINANCE_PERP_PLUS_BYBIT_DERIVATIVES','bar_ts':cb,'lag_8h_bars':clag,'promotion_source':True},
                    'legacy_v103':{'source':'BITGET_SPOT_LEGACY','promotion_source':False,'quarantined':True},
                    'v42_exact_overlay':{'source':'EXACT_BINANCE_PERP_PLUS_BINANCE_BYBIT_FUNDING','bar_ts':v42bar,'lag_8h_bars':v42lag,'promotion_source':True},
                    'v42_legacy_overlay':{'source':'LEGACY_TURBO_RUNTIME','promotion_source':False,'quarantined':True}},
                'promotion_data_integrity_ready':ok,'generated_at':time.time()}

futures_data_integrity=FuturesDataIntegrity()
