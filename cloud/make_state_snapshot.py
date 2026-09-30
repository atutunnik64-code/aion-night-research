from __future__ import annotations
import json, tarfile, time
from pathlib import Path

ROOT = Path(__file__).parents[1]
DATA = ROOT / 'data'
OUT = DATA / 'cloud_snapshots'
OUT.mkdir(parents=True, exist_ok=True)

PATTERNS = [
    'moex_*', 'funding_*', 'crossvenue_*',
    '*taker*', '*oi_migration*', '*leadlag*',
]

def selected_files():
    seen = set()
    for pat in PATTERNS:
        for p in DATA.glob(pat):
            if p.is_file() and p.suffix.lower() in {'.json', '.jsonl', '.sqlite3'}:
                rp = p.resolve()
                if rp not in seen:
                    seen.add(rp)
                    yield p

ts = int(time.time())
archive = OUT / f'aion_public_research_{ts}.tar.gz'
files = list(selected_files())
manifest = {
    'created_at': ts,
    'file_count': len(files),
    'files': [str(p.relative_to(DATA)) for p in files],
    'scope': 'PUBLIC_RESEARCH_ONLY',
    'contains_secrets': False,
}
manifest_path = OUT / f'manifest_{ts}.json'
manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')

with tarfile.open(archive, 'w:gz') as tar:
    for p in files:
        tar.add(p, arcname=f'data/{p.relative_to(DATA)}')
    tar.add(manifest_path, arcname='manifest.json')

print(json.dumps({'ok': True, 'archive': str(archive), **manifest}, ensure_ascii=False))
