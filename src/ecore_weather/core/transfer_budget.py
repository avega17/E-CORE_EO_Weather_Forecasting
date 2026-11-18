"""Cross-process returned-payload budget for controlled network experiments.

Reserve the known request length before sending. Opaque failed requests are
charged at their full reservation; streaming failures settle observed bytes.
This conservative upper bound prevents retries or parallel requests overrunning
an experiment's cumulative transfer limit. It excludes LIST/HTTP headers.
"""
import contextlib
import fcntl
import json
import os
from pathlib import Path
import threading

class BudgetExceeded(RuntimeError):pass

_LOCK=threading.Lock()

class Budget:
    def __init__(self,path,limit=None):
        self.path=Path(path)
        self.path.parent.mkdir(parents=True,exist_ok=True)
        if limit is not None and not self.path.exists():
            with self.path.open('x') as f:json.dump({'limit':int(limit),'charged':0,'observed':0,'requests':0,'inflight':0},f)
    @contextlib.contextmanager
    def state(self):
        with _LOCK,self.path.open('r+') as f:
            fcntl.flock(f,fcntl.LOCK_EX)
            data=json.load(f)
            try:yield data
            finally:
                f.seek(0);json.dump(data,f);f.truncate();f.flush();fcntl.flock(f,fcntl.LOCK_UN)
    def reserve(self,size):
        with self.state() as data:
            if data['charged']+size>data['limit']:raise BudgetExceeded('Controlled network byte budget exhausted')
            data['charged']+=size;data['inflight']+=size;data['requests']+=1
        return size
    def settle(self,reservation,observed=None):
        with self.state() as data:
            data['inflight']-=reservation
            if observed is not None:
                data['charged']+=observed-reservation;data['observed']+=observed
    def summary(self):
        with self.state() as data:return dict(data)

def active():
    path=os.getenv('ECORE_TRANSFER_BUDGET')
    return Budget(path) if path else None
