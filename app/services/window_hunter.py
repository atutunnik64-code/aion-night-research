from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path

DATA_FILE = Path(__file__).resolve().parents[2] / 'data' / 'crypto_windows.json'

class WindowHunter:
    def __init__(self):
        self.active: dict[str, dict] = {}
        self.closed: list[dict] = []
        self._load()

    def _now(self):
        return datetime.now(timezone.utc)

    def _load(self):
        try:
            if DATA_FILE.exists():
                data = json.loads(DATA_FILE.read_text(encoding='utf-8'))
                self.active = data.get('active', {})
                self.closed = data.get('closed', [])[-100:]
        except Exception:
            self.active, self.closed = {}, []

    def _save(self):
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = DATA_FILE.with_suffix('.tmp')
        tmp.write_text(json.dumps({'active':self.active,'closed':self.closed[-100:]}, ensure_ascii=False, indent=2), encoding='utf-8')
        tmp.replace(DATA_FILE)

    def _state(self, row, now):
        first = datetime.fromisoformat(row['first_seen'])
        age = max(0, (now-first).total_seconds())
        samples = int(row.get('samples', 1))
        if samples >= 20 and age >= 600:
            return 'STABLE'
        if samples >= 3:
            return 'CONFIRMED'
        return 'FLASH'
    def update(self, cycles: list[dict], prefunded: list[dict]):
        now = self._now(); now_iso = now.isoformat(); seen: dict[str, tuple[float,dict]] = {}
        for item in cycles:
            pct = float(item.get('full_net_pct', item.get('profit_pct', 0)))
            if pct <= 0.01: continue
            sig = 'cycle|' + '>'.join(item.get('nodes') or [])
            seen[sig] = (pct, {'kind':'cycle','label':' → '.join(item.get('nodes') or []),'meta':{'hops':item.get('hops'),'window_mode':item.get('window_mode')}})
        for item in prefunded:
            pct = float(item.get('net_pct', 0))
            if pct <= 0.01: continue
            sig = f"prefunded|{item.get('base')}/{item.get('quote')}|{item.get('buy_venue')}|{item.get('sell_venue')}"
            label = f"{item.get('base')}/{item.get('quote')} · {item.get('buy_venue')} → {item.get('sell_venue')}"
            seen[sig] = (pct, {'kind':'prefunded','label':label,'meta':{'gross_pct':item.get('gross_pct'),'quote_in':item.get('quote_in')}})

        for sig, (pct, info) in seen.items():
            row = self.active.get(sig)
            if row is None:
                row = {'signature':sig,'kind':info['kind'],'label':info['label'],'first_seen':now_iso,'last_seen':now_iso,'samples':1,'current_pct':pct,'max_pct':pct,'meta':info['meta']}
                self.active[sig] = row
            else:
                row['last_seen'] = now_iso; row['samples'] = int(row.get('samples',0))+1; row['current_pct'] = pct; row['max_pct'] = max(float(row.get('max_pct',pct)), pct); row['meta'] = info['meta']
            row['state'] = self._state(row, now)

        for sig in list(self.active):
            if sig in seen: continue
            row = self.active.pop(sig); row['closed_at'] = now_iso; row['state'] = 'CLOSED'; self.closed.append(row)

        self._save()
        active = sorted(self.active.values(), key=lambda x: float(x.get('current_pct',0)), reverse=True)
        counts = {'flash':0,'confirmed':0,'stable':0}
        for row in active:
            key = str(row.get('state','FLASH')).lower()
            if key in counts: counts[key] += 1
        return {'active':active,'counts':counts,'recently_closed':list(reversed(self.closed[-20:]))}

window_hunter = WindowHunter()
