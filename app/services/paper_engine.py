from __future__ import annotations
import sqlite3, time
from pathlib import Path

DB_PATH=Path(__file__).parents[2]/'data'/'paper_trades.sqlite3'
DB_PATH.parent.mkdir(parents=True,exist_ok=True)

class PaperEngine:
    def __init__(self):
        self.db=sqlite3.connect(DB_PATH,check_same_thread=False)
        self.db.row_factory=sqlite3.Row
        self.enabled=True; self.min_score=35.0; self.max_notional=1000.0; self.cooldown=45.0
        self.db.execute('''CREATE TABLE IF NOT EXISTS trades(
          id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, signature TEXT, mode TEXT,
          score REAL, notional REAL, expected_net_pct REAL, expected_profit REAL,
          status TEXT, details TEXT)''')
        self.db.commit()

    @staticmethod
    def _sig(x,mode):
        return f"{mode}|{x.get('signature') or '>'.join(x.get('nodes') or x.get('assets') or []) or str(x.get('base'))+'|'+str(x.get('slow_venue'))+'|'+str(x.get('fast_venue'))}"

    def _last(self,sig):
        return self.db.execute('SELECT ts FROM trades WHERE signature=? ORDER BY id DESC LIMIT 1',(sig,)).fetchone()
    def consider(self,rows,mode):
        if not self.enabled:return []
        now=time.time(); opened=[]
        for x in rows:
            score=float(x.get('aion_score') or 0)
            if score<self.min_score:continue
            sig=self._sig(x,mode); last=self._last(sig)
            if last and now-float(last['ts'])<self.cooldown:continue
            if mode=='FULL_NOW':
                net=float(x.get('full_net_pct') or 0); base_notional=float(x.get('start_amount') or 0)
                exp=float(x.get('full_profit_amount') or 0)
            elif mode=='BATCH_READY':
                p=x.get('batch_ready_plan') or {}; net=float(p.get('net_pct_per_cycle') or 0)
                base_notional=float(x.get('start_amount') or 0); exp=float(p.get('net_per_cycle') or 0)
            else:
                net=float(x.get('net_pct') or 0); base_notional=float(x.get('quote_in') or 0); exp=float(x.get('net_profit') or 0)
            notional=min(self.max_notional,base_notional) if base_notional>0 else 0
            if notional<10 or net<=0:continue
            expected_profit=notional*net/100.0
            self.db.execute('INSERT INTO trades(ts,signature,mode,score,notional,expected_net_pct,expected_profit,status,details) VALUES(?,?,?,?,?,?,?,?,?)',
                            (now,sig,mode,score,notional,net,expected_profit,'PAPER_EXECUTED','simulated simultaneous execution'))
            opened.append({'signature':sig,'mode':mode,'score':score,'notional':round(notional,2),'expected_net_pct':round(net,5),'expected_profit':round(expected_profit,6)})
        self.db.commit(); return opened

    def summary(self,limit=50):
        rows=[dict(r) for r in self.db.execute('SELECT * FROM trades ORDER BY id DESC LIMIT ?',(limit,)).fetchall()]
        agg=self.db.execute('SELECT COUNT(*) c,COALESCE(SUM(expected_profit),0) p,COALESCE(AVG(expected_net_pct),0) n FROM trades').fetchone()
        return {'enabled':self.enabled,'min_score':self.min_score,'max_notional':self.max_notional,
                'trade_count':agg['c'],'expected_profit_total':round(agg['p'],6),'avg_net_pct':round(agg['n'],5),'recent':rows}

paper_engine=PaperEngine()
