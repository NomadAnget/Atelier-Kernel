"""Real Python/Node SDKs against a real kernel; no simulated receipts or transport."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

ROOT=Path(__file__).resolve().parents[1]
WORK=ROOT/'tmp/sdk-observer-memory'/uuid.uuid4().hex
WORK.mkdir(parents=True)
BINARY=(Path(sys.argv[1]) if len(sys.argv)>1 else ROOT/'target/debug'/('atelier-kernel.exe' if os.name=='nt' else 'atelier-kernel')).resolve()
PY='''import asyncio,json,sys
from pathlib import Path
from atelier_sdk import AtelierDomain
d=AtelierDomain(domain="observer_probe_py")
@d.handler("ping")
def ping(msg):return {"ready":True}
async def start():
    sizes=[]
    for _ in range(128):
        events=await d.observer.query(limit=32)
        assert isinstance(events,list) and len(events)<=32
        assert all(e.get("target")!="kernel.observer" or "events" not in (e.get("details") or {}) for e in events)
        sizes.append(len(json.dumps(events).encode()))
        runtime=await d.runtime.query()
        assert isinstance(runtime,dict)
        assert isinstance(runtime["ledger"]["submissions"],int)
    assert max(sizes)<32768
    payload={"text":"x"*8192}
    await d.storage_insert_async("large","memory_probe",payload)
    for _ in range(128):
        stored=await d.storage_query_async(record_id="large")
        assert stored["items"][0]["data"]==payload
    events=await d.observer.query(limit=32)
    assert all(e.get("target")!="kernel.storage" or "items" not in (e.get("details") or {}) for e in events)
    Path(sys.argv[1]).write_text(json.dumps({"success":True,"queries":128,"storage_queries":128,"maximum_response_bytes":max(sizes),"sizes":sizes}))
asyncio.run(d.run_async(on_start=start))
'''
JS='''const fs=require('node:fs');
const {AtelierDomain}=require(process.argv[2]);
const d=new AtelierDomain({domain:'observer_probe_js'});
d.handler('ping',()=>({ready:true}));
d.runAsync({onStart:async()=>{
 const sizes=[];
 for(let i=0;i<128;i++){
  const events=await d.observer.query({limit:32});
  if(!Array.isArray(events)||events.length>32)throw Error('invalid observer response');
  if(events.some(e=>e.target==='kernel.observer'&&Array.isArray(e.details?.events)))throw Error('recursive snapshot');
  sizes.push(Buffer.byteLength(JSON.stringify(events)));
  const runtime=await d.runtime.query();
  if(!runtime||typeof runtime!=='object')throw Error('invalid runtime response');
  if(!Number.isInteger(runtime.ledger?.submissions))throw Error('missing native ledger diagnostics');
 }
 if(Math.max(...sizes)>=32768)throw Error('unbounded snapshot response');
 const payload={text:'x'.repeat(8192)};
 await d.storageInsertAsync('large','memory_probe',payload);
 for(let i=0;i<128;i++){
  const stored=await d.storageQueryAsync({recordId:'large'});
  if(stored.items[0].data.text!==payload.text)throw Error('storage payload changed');
 }
 const observed=await d.observer.query({limit:32});
 if(observed.some(e=>e.target==='kernel.storage'&&Array.isArray(e.details?.items)))throw Error('storage results retained in observations');
 fs.writeFileSync(process.argv[3],JSON.stringify({success:true,queries:128,storage_queries:128,maximum_response_bytes:Math.max(...sizes),sizes}));
}});
'''
py=WORK/'probe.py';py.write_text(PY)
js=WORK/'probe.cjs';js.write_text(JS)
node=shutil.which('node')
assert node,'Node runtime required'
config={'db_path':str(WORK/'kernel.db'),'web_ui_enabled':False,'domains':[
    {'domain_id':'observer_probe_py','command':sys.executable,'args':[str(py),str(WORK/'python.json')],
     'working_dir':str(ROOT)},
    {'domain_id':'observer_probe_js','command':node,'args':[str(js),str(ROOT/'sdk/nodejs/index.js'),str(WORK/'node.json')],
     'working_dir':str(ROOT)}]}
boot=WORK/'kernel_boot.json';boot.write_text(json.dumps(config))
env=os.environ.copy();env['PYTHONPATH']=str(ROOT/'sdk/python')
with (WORK/'kernel.log').open('w') as log:
    process=subprocess.Popen([str(BINARY),'--headless','--config',str(boot)],cwd=ROOT,env=env,stdout=log,stderr=log)
    try:
        for _ in range(600):
            if all((WORK/name).is_file() for name in ['python.json','node.json']):break
            assert process.poll() is None,'Kernel exited before real SDK probes finished'
            time.sleep(.1)
        else:raise TimeoutError(f'Real SDK probes not complete; inspect {WORK}/kernel.log')
        results={name:json.loads((WORK/file).read_text()) for name,file in [('python','python.json'),('node','node.json')]}
        assert all(result['success'] for result in results.values())
        (WORK/'report.json').write_text(json.dumps({'success':True,'real_kernel':str(BINARY),'results':results},indent=2))
        print(json.dumps({'success':True,'work':str(WORK),'responses':{name:item['maximum_response_bytes'] for name,item in results.items()}}))
    except Exception:
        print((WORK/'kernel.log').read_text(errors='replace')[-20000:], file=sys.stderr)
        raise
    finally:
        process.terminate()
        try:process.wait(timeout=15)
        except subprocess.TimeoutExpired:process.kill();process.wait()
