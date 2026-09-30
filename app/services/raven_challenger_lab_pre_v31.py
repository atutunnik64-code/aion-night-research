from __future__ import annotations
import asyncio, json, sys, time
from pathlib import Path

ROOT=Path(__file__).parents[2]
SCRIPT=ROOT/'research'/'raven_challenger_search.py'
STATE=ROOT/'data'/'raven_challenger_lab_state.json'

class RavenChallengerLab:
    def __init__(self):
        self.enabled=True
        self.interval=21600.0
        self.samples=180
        self.task=None
        self.child=None
        self.last_error=None
        self.state=self._load()

    def _load(self):
        base={'runs':0,'last_start':None,'last_finish':None,'last_exit_code':None,
              'last_seed':None,'last_output_tail':[],'running':False}
        try:
            old=json.loads(STATE.read_text(encoding='utf-8'))
            if isinstance(old,dict): base.update(old)
        except Exception:
            pass
        return base
    def _save(self):
        STATE.parent.mkdir(parents=True,exist_ok=True)
        STATE.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding='utf-8')

    async def run_once(self,samples:int|None=None):
        if self.child and self.child.returncode is None:
            return {**self.status(),'started':False,'reason':'RUN_ALREADY_ACTIVE'}
        n=max(20,min(500,int(samples or self.samples)))
        seed=int(time.time()//21600)
        self.state.update({'last_start':time.time(),'last_seed':seed,'running':True})
        self._save()
        try:
            self.child=await asyncio.create_subprocess_exec(
                sys.executable,str(SCRIPT),'--samples',str(n),'--seed',str(seed),
                cwd=str(ROOT),stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.STDOUT)
            out,_=await self.child.communicate()
            text=(out or b'').decode('utf-8','replace')
            code=int(self.child.returncode or 0)
            self.state['runs']=int(self.state.get('runs') or 0)+1
            self.state['last_finish']=time.time();self.state['last_exit_code']=code
            self.state['last_output_tail']=text.splitlines()[-80:]
            self.state['running']=False
            self.last_error=None if code==0 else f'CHALLENGER_EXIT_{code}'
            self._save()
            return self.status()
        except Exception as exc:
            self.state['running']=False;self.last_error=str(exc)[:300];self._save()
            return self.status()
        finally:
            self.child=None
    def status(self):
        running=bool(self.child and self.child.returncode is None) or bool(self.state.get('running'))
        return {'ok':self.last_error is None,'mode':'RESEARCH_ONLY','enabled':self.enabled,
                'interval_seconds':self.interval,'samples':self.samples,'running':running,
                'runs':int(self.state.get('runs') or 0),'last_start':self.state.get('last_start'),
                'last_finish':self.state.get('last_finish'),'last_exit_code':self.state.get('last_exit_code'),
                'last_seed':self.state.get('last_seed'),'last_error':self.last_error,
                'last_output_tail':(self.state.get('last_output_tail') or [])[-20:],
                'live_promotion_enabled':False}

    async def start(self):
        if self.task and not self.task.done(): return
        self.state['running']=False;self._save()
        self.task=asyncio.create_task(self._loop(),name='raven-challenger-lab')

    async def stop(self):
        if self.child and self.child.returncode is None:
            self.child.terminate()
            try: await asyncio.wait_for(self.child.wait(),timeout=5)
            except Exception: self.child.kill()
        if self.task and not self.task.done():
            self.task.cancel()
            try: await self.task
            except BaseException: pass
        self.task=None;self.state['running']=False;self._save()

    async def _loop(self):
        await asyncio.sleep(120.0)
        while True:
            if self.enabled: await self.run_once()
            await asyncio.sleep(max(3600.0,self.interval))

raven_challenger_lab=RavenChallengerLab()
