from __future__ import annotations
import asyncio,json,subprocess,sys,time
from pathlib import Path
ROOT=Path(__file__).parents[2]
STATE=ROOT/'data'/'futures_forward_tracker_v1.json'
V42STATE=ROOT/'data'/'v42_perp_forward_tracker_v1.json'
RUNLOG=ROOT/'data'/'futures_forward_worker_runs.jsonl'
SCRIPTS=[
    'update_binance_perp_future_extension_v1.py',
    'update_bybit_future_positioning_v1.py',
    'update_bybit_funding_future_extension_v1.py',
    'futures_forward_tracker_v1.py',
    'v42_perp_forward_tracker_v1.py',
]

class FuturesForwardWorker:
    def __init__(self):
        self.enabled=True;self.interval=900.0;self.task=None
        self.last_run=None;self.last_error=None;self.last_actions=[]
    def _run(self,script):
        p=subprocess.run([sys.executable,str(ROOT/'research'/script)],cwd=str(ROOT),capture_output=True,text=True,timeout=180)
        row={'ts':time.time(),'script':script,'returncode':p.returncode,'stdout_tail':p.stdout[-2500:],'stderr_tail':p.stderr[-1200:]}
        with RUNLOG.open('a',encoding='utf-8') as f:f.write(json.dumps(row,ensure_ascii=False)+'\n')
        if p.returncode!=0:raise RuntimeError(f'{script}:RC={p.returncode}:{p.stderr[-300:]}')
        return {'script':script,'returncode':p.returncode}
    def _state(self):
        try:return json.loads(STATE.read_text(encoding='utf-8'))
        except Exception:return {}
    def _v42_state(self):
        try:return json.loads(V42STATE.read_text(encoding='utf-8'))
        except Exception:return {}
    async def refresh(self):
        actions=[]
        try:
            for script in SCRIPTS:
                actions.append(await asyncio.to_thread(self._run,script))
            self.last_error=None
        except Exception as exc:
            self.last_error=str(exc)[:600]
        self.last_run=time.time();self.last_actions=actions
        return self.status()
    def status(self):
        return {'ok':self.last_error is None,'enabled':self.enabled,'mode':'FUTURES_FORWARD_RESEARCH_ONLY',
                'live_enabled':False,'interval_seconds':self.interval,'append_only':True,
                'last_run':self.last_run,'last_error':self.last_error,'last_actions':self.last_actions,
                'tracker':self._state(),'v42_tracker':self._v42_state()}
    async def start(self):
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self._loop(),name='futures-forward-worker')
    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:await self.task
            except BaseException:pass
        self.task=None
    async def _loop(self):
        await asyncio.sleep(30.0)
        while True:
            if self.enabled:await self.refresh()
            await asyncio.sleep(max(300.0,self.interval))

futures_forward_worker=FuturesForwardWorker()
