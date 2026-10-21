"""SDK callback isolation contract, not a native transport/inference test."""
import asyncio
import json
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'sdk/python'))
from atelier_sdk import AtelierDomain

async def python_case():
    domain=AtelierDomain(domain='slow_probe')
    release=asyncio.Event()
    counts={'slow':0,'fast':0}
    async def slow(event):
        counts['slow']+=1
        await release.wait()
    def fast(event):counts['fast']+=1
    domain._log_listeners=[(slow,None,None),(fast,None,None)]
    for i in range(10_000):domain._dispatch_system_log({'data':{'event_id':i}})
    await asyncio.sleep(0)
    assert counts=={'slow':1,'fast':10_000},counts
    assert len(domain._callbacks)==1
    release.set()
    for _ in range(3):await asyncio.sleep(0)
    assert not domain._callbacks and not domain._log_busy
    domain._dispatch_system_log({'data':{'event_id':10_001}})
    for _ in range(3):await asyncio.sleep(0)
    assert counts=={'slow':2,'fast':10_001}
    return counts

py=asyncio.run(python_case())
js=r'''
const {AtelierDomain}=require(process.argv[1]);
(async()=>{
 const d=new AtelierDomain({domain:'slow_probe'});
 let release;const gate=new Promise(resolve=>release=resolve);
 const counts={slow:0,fast:0};
 d._logListeners=[{callback:async()=>{counts.slow++;await gate;}},{callback:()=>{counts.fast++;}}];
 for(let i=0;i<10000;i++)d._dispatchSystemLog({data:{event_id:i}});
 if(counts.slow!==1||counts.fast!==10000||d._callbacks.size!==1)throw Error(JSON.stringify(counts));
 release();await new Promise(setImmediate);
 if(d._callbacks.size||d._logBusy.size)throw Error('retained callback');
 d._dispatchSystemLog({data:{event_id:10001}});await new Promise(setImmediate);
 if(counts.slow!==2||counts.fast!==10001)throw Error('did not recover');
 console.log(JSON.stringify(counts));
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
node=json.loads(subprocess.check_output(['node','-e',js,str(ROOT/'sdk/nodejs/index.js')],text=True))
assert py==node
folder=ROOT/'tmp/adversarial-audit';folder.mkdir(parents=True,exist_ok=True)
report={'success':True,'python':py,'node':node,'scope':__doc__}
(folder/'sdk-slow-log.json').write_text(json.dumps(report,indent=2))
print(json.dumps(report))
