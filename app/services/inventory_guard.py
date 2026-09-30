from __future__ import annotations
import json,time
from pathlib import Path

PATH=Path(__file__).parents[2]/'data'/'inventory_targets.json'

class InventoryGuard:
    def __init__(self):
        self.reserve_pct=15.0
        self.targets={}
        self.current={}
        self.current_updated_at={}
        self.last_check=None
        self.last_settlement=None
        self._load()
    def _load(self):
        try:
            raw=json.loads(PATH.read_text(encoding='utf-8'))
            self.targets=raw.get('targets',{}) or {}
            self.current=raw.get('current',{}) or {}
            self.current_updated_at=raw.get('current_updated_at',{}) or {}
        except Exception:
            self.targets={}; self.current={}; self.current_updated_at={}
    def _save(self):
        PATH.parent.mkdir(parents=True,exist_ok=True)
        PATH.write_text(json.dumps({'targets':self.targets,'current':self.current,'current_updated_at':self.current_updated_at,'updated_at':time.time()},ensure_ascii=False,indent=2),encoding='utf-8')
    @staticmethod
    def _key(venue,asset):return f'{venue}:{str(asset or "").upper()}'
    def record_balances(self,balance_map,source='exchange_snapshot'):
        now=time.time()
        for venue,assets in (balance_map or {}).items():
            for asset,free in (assets or {}).items():
                try:free=float(free or 0)
                except (TypeError,ValueError):continue
                k=self._key(venue,asset)
                self.current[k]=free; self.current_updated_at[k]=now
                if k not in self.targets and free>0:self.targets[k]=free
        self._save()
        return {'ok':True,'source':source,'updated_at':now,'count':len(self.current)}
    def check(self,balance_map,legs):
        self.record_balances(balance_map,'preflight_balance_snapshot')
        errors=[]; projected=dict(self.current)
        for leg in legs:
            k=self._key(leg['venue'],leg.get('input_asset')); need=float(leg.get('input_required') or 0)*1.002
            projected[k]=projected.get(k,0.0)-need
        for k,after in projected.items():
            target=float(self.targets.get(k) or 0)
            if target<=0:continue
            floor=target*self.reserve_pct/100.0
            if after<floor:
                errors.append({'inventory':k,'after':round(after,10),'reserve_floor':round(floor,10),'target':round(target,10)})
        self.last_check={'at':time.time(),'ok':not errors,'errors':errors,'projected':projected}
        self._save()
        return {'ok':not errors,'errors':errors,'reserve_pct':self.reserve_pct,'projected':projected}
    @staticmethod
    def _state_cash(leg,state):
        qty=max(0.0,float((state or {}).get('filled_qty') or 0))
        quote=max(0.0,float((state or {}).get('filled_quote') or 0))
        if quote<=0 and qty>0:quote=qty*max(0.0,float(leg.get('price') or 0))
        fee=max(0.0,float(leg.get('fee_rate') or 0))
        return qty,quote,fee
    def _apply_fill(self,deltas,leg,state):
        qty,quote,fee=self._state_cash(leg,state)
        if qty<=0:return
        base=self._key(leg.get('venue'),leg.get('base')); qasset=self._key(leg.get('venue'),leg.get('quote'))
        if str(leg.get('side')).lower()=='buy':
            deltas[base]=deltas.get(base,0.0)+qty
            deltas[qasset]=deltas.get(qasset,0.0)-quote*(1.0+fee)
        else:
            deltas[base]=deltas.get(base,0.0)-qty
            deltas[qasset]=deltas.get(qasset,0.0)+quote*(1.0-fee)
    def apply_execution(self,legs,states,recovery=None,balance_map_after=None):
        before=dict(self.current); deltas={}
        for leg,state in zip(legs or [],states or []):self._apply_fill(deltas,leg,state)
        if isinstance(recovery,dict) and recovery.get('leg') and recovery.get('state'):
            self._apply_fill(deltas,recovery['leg'],recovery['state'])
        now=time.time()
        for k,delta in deltas.items():
            self.current[k]=max(0.0,float(self.current.get(k,0.0))+float(delta)); self.current_updated_at[k]=now
        source='fill_ledger'
        if balance_map_after:
            self.record_balances(balance_map_after,'post_execution_exchange_snapshot'); source='exchange_snapshot'
        self.last_settlement={'at':now,'source':source,'deltas':{k:round(v,12) for k,v in deltas.items()},'before':before,'after':dict(self.current)}
        self._save(); return self.last_settlement
    def status(self):
        return {'reserve_pct':self.reserve_pct,'target_count':len(self.targets),'targets':self.targets,'current':self.current,
                'current_updated_at':self.current_updated_at,'last_check':self.last_check,'last_settlement':self.last_settlement}
    def set_targets(self,targets,source='manager'):
        now=time.time(); changed=0
        for key,value in (targets or {}).items():
            try:value=max(0.0,float(value or 0))
            except (TypeError,ValueError):continue
            self.targets[str(key)]=value; changed+=1
        self._save(); return {'ok':True,'source':source,'changed':changed,'updated_at':now}
    def reset_targets(self):
        self.targets={}; self._save(); return self.status()

inventory_guard=InventoryGuard()
