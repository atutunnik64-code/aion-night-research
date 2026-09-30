from __future__ import annotations
import math, sqlite3, time
from pathlib import Path

DB_PATH = Path(__file__).parents[2] / 'data' / 'opportunity_history.sqlite3'
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

class OpportunityMemory:
    def __init__(self):
        self.db = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._init()

    def _init(self):
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS routes(
          signature TEXT PRIMARY KEY, mode TEXT, label TEXT,
          first_seen REAL, last_seen REAL, hits INTEGER DEFAULT 0,
          avg_net REAL DEFAULT 0, max_net REAL DEFAULT 0,
          avg_profit REAL DEFAULT 0, max_profit REAL DEFAULT 0,
          avg_liquidity REAL DEFAULT 0, max_liquidity REAL DEFAULT 0,
          avg_speed_ms REAL DEFAULT 0, closed_at REAL
        );
        CREATE TABLE IF NOT EXISTS observations(
          id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL,
          signature TEXT, mode TEXT, net_pct REAL,
          profit REAL, liquidity REAL, speed_ms REAL
        );
        CREATE INDEX IF NOT EXISTS ix_obs_sig_ts ON observations(signature, ts);
        ''')
        self.db.commit()
    @staticmethod
    def _signature(row, mode):
        sig = row.get('signature')
        if sig: return f'{mode}|{sig}'
        nodes = row.get('nodes') or row.get('assets') or []
        if nodes: return f"{mode}|{'>' .join(map(str,nodes))}"
        return '|'.join([mode,str(row.get('base','')),str(row.get('quote','')),
                         str(row.get('buy_venue') or row.get('slow_venue') or ''),
                         str(row.get('sell_venue') or row.get('fast_venue') or '')])

    @staticmethod
    def _metrics(row, mode):
        if mode == 'FULL_NOW':
            net=float(row.get('full_net_pct') or 0); profit=float(row.get('full_profit_amount') or 0)
            liquidity=float(row.get('start_amount') or 0); speed=0.0
        elif mode == 'BATCH_READY':
            p=row.get('batch_ready_plan') or {}; net=float(p.get('net_pct_per_cycle') or 0)
            profit=float(p.get('net_per_cycle') or 0); liquidity=float(row.get('start_amount') or 0); speed=0.0
        else:
            net=float(row.get('net_pct') or 0); profit=float(row.get('net_profit') or 0)
            liquidity=float(row.get('quote_in') or 0); speed=float(row.get('observed_lag_ms') or 0)
        return net,profit,liquidity,speed

    def record(self, rows, mode):
        now=time.time(); seen=set()
        for row in rows:
            sig=self._signature(row,mode); seen.add(sig)
            net,profit,liq,speed=self._metrics(row,mode)
            label=' → '.join(row.get('nodes') or row.get('assets') or []) or f"{row.get('base')}/{row.get('quote')}"
            old=self.db.execute('SELECT * FROM routes WHERE signature=?',(sig,)).fetchone()
            if old:
                n=old['hits']+1
                avg_net=(old['avg_net']*old['hits']+net)/n; avg_profit=(old['avg_profit']*old['hits']+profit)/n
                avg_liq=(old['avg_liquidity']*old['hits']+liq)/n; avg_speed=(old['avg_speed_ms']*old['hits']+speed)/n
                self.db.execute('''UPDATE routes SET last_seen=?,hits=?,avg_net=?,max_net=?,avg_profit=?,max_profit=?,
                    avg_liquidity=?,max_liquidity=?,avg_speed_ms=?,closed_at=NULL WHERE signature=?''',
                    (now,n,avg_net,max(old['max_net'],net),avg_profit,max(old['max_profit'],profit),
                     avg_liq,max(old['max_liquidity'],liq),avg_speed,sig))
            else:
                self.db.execute('''INSERT INTO routes(signature,mode,label,first_seen,last_seen,hits,avg_net,max_net,
                    avg_profit,max_profit,avg_liquidity,max_liquidity,avg_speed_ms,closed_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)''',
                    (sig,mode,label,now,now,1,net,net,profit,profit,liq,liq,speed))
            self.db.execute('INSERT INTO observations(ts,signature,mode,net_pct,profit,liquidity,speed_ms) VALUES(?,?,?,?,?,?,?)',
                            (now,sig,mode,net,profit,liq,speed))
        if seen:
            marks=','.join('?'*len(seen))
            self.db.execute(f'''UPDATE routes SET closed_at=COALESCE(closed_at,?)
                WHERE mode=? AND closed_at IS NULL AND signature NOT IN ({marks}) AND last_seen<?''',
                (now,mode,*seen,now-20))
        else:
            self.db.execute('''UPDATE routes SET closed_at=COALESCE(closed_at,?)
                WHERE mode=? AND closed_at IS NULL AND last_seen<?''',(now,mode,now-20))
        self.db.commit()
        return [self.enrich(row,mode) for row in rows]

    def observe_one(self,row,mode):
        now=time.time(); sig=self._signature(row,mode)
        net,profit,liq,speed=self._metrics(row,mode)
        label=' → '.join(row.get('nodes') or row.get('assets') or []) or f"{row.get('base')}/{row.get('quote')}"
        old=self.db.execute('SELECT * FROM routes WHERE signature=?',(sig,)).fetchone()
        if old:
            n=old['hits']+1
            sql='UPDATE routes SET last_seen=?,hits=?,avg_net=?,max_net=?,avg_profit=?,max_profit=?,avg_liquidity=?,max_liquidity=?,avg_speed_ms=?,closed_at=NULL WHERE signature=?'
            vals=(now,n,(old['avg_net']*old['hits']+net)/n,max(old['max_net'],net),(old['avg_profit']*old['hits']+profit)/n,max(old['max_profit'],profit),(old['avg_liquidity']*old['hits']+liq)/n,max(old['max_liquidity'],liq),(old['avg_speed_ms']*old['hits']+speed)/n,sig)
            self.db.execute(sql,vals)
        else:
            sql='INSERT INTO routes(signature,mode,label,first_seen,last_seen,hits,avg_net,max_net,avg_profit,max_profit,avg_liquidity,max_liquidity,avg_speed_ms,closed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)'
            self.db.execute(sql,(sig,mode,label,now,now,1,net,net,profit,profit,liq,liq,speed))
        self.db.execute('INSERT INTO observations(ts,signature,mode,net_pct,profit,liquidity,speed_ms) VALUES(?,?,?,?,?,?,?)',(now,sig,mode,net,profit,liq,speed))
        self.db.commit(); return self.enrich(row,mode)

    def enrich(self,row,mode):
        sig=self._signature(row,mode)
        stat=self.db.execute('SELECT * FROM routes WHERE signature=?',(sig,)).fetchone()
        if not stat: return row
        z=dict(row); z['history']=self._public(stat); z['aion_score']=self.score(stat,row,mode)
        return z
    @staticmethod
    def _public(r):
        age=max(0,time.time()-r['first_seen']); active=max(0,r['last_seen']-r['first_seen'])
        if r['hits']>=20 and active>=900: state='LONG_STABLE'
        elif r['hits']>=10 and active>=300: state='STABLE'
        elif r['hits']>=4 and active>=45: state='CONFIRMED'
        else: state='WATCH'
        return {'hits':r['hits'],'reliability_state':state,'first_seen':r['first_seen'],'last_seen':r['last_seen'],
                'age_seconds':round(age,1),'observed_life_seconds':round(active,1),
                'avg_net_pct':round(r['avg_net'],5),'max_net_pct':round(r['max_net'],5),
                'avg_profit':round(r['avg_profit'],8),'max_profit':round(r['max_profit'],8),
                'avg_liquidity':round(r['avg_liquidity'],2),'max_liquidity':round(r['max_liquidity'],2),
                'avg_speed_ms':round(r['avg_speed_ms'],1),'closed_at':r['closed_at']}

    @staticmethod
    def score(stat,row,mode):
        net=max(0.0,float(stat['avg_net'])); hits=max(1,int(stat['hits']))
        life=max(0.0,float(stat['last_seen']-stat['first_seen'])); liq=max(0.0,float(stat['avg_liquidity']))
        profit=max(0.0,float(stat['avg_profit'])); speed=max(0.0,float(stat['avg_speed_ms']))
        net_s=min(35.0,35.0*(1-math.exp(-net/0.35)))
        repeat_s=min(22.0,5.5*math.log2(hits+1)); life_s=min(18.0,18.0*(1-math.exp(-life/180.0)))
        liq_s=min(15.0,3.3*math.log10(max(1.0,liq))); profit_s=min(8.0,2.2*math.log10(max(1.0,profit+1)))
        speed_bonus=0.0 if mode!='LATENCY' else min(6.0,6.0*(1-math.exp(-speed/3000.0)))
        score=min(100.0,net_s+repeat_s+life_s+liq_s+profit_s+speed_bonus)
        return round(score,1)

    def leaderboard(self,limit=50,min_hits=2):
        rows=self.db.execute('''SELECT * FROM routes WHERE hits>=? ORDER BY hits DESC, avg_net DESC LIMIT ?''',(min_hits,limit)).fetchall()
        out=[]
        for r in rows:
            x=self._public(r); x.update({'signature':r['signature'],'mode':r['mode'],'label':r['label']})
            x['aion_score']=self.score(r,{},r['mode']); out.append(x)
        return sorted(out,key=lambda x:(x['aion_score'],x['hits'],x['avg_net_pct']),reverse=True)

    def long_windows(self,min_seconds=300,limit=50):
        rows=self.db.execute('''SELECT * FROM routes WHERE closed_at IS NULL AND (? - first_seen)>=?
            ORDER BY avg_net DESC, hits DESC LIMIT ?''',(time.time(),min_seconds,limit)).fetchall()
        out=[]
        for r in rows:
            x=self._public(r); x.update({'signature':r['signature'],'mode':r['mode'],'label':r['label']})
            x['aion_score']=self.score(r,{},r['mode']); out.append(x)
        return out

memory=OpportunityMemory()
