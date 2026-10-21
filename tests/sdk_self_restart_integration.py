"""Real managed Python/Node domains restart themselves using unchanged SDK APIs."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

ROOT=Path(__file__).resolve().parents[1]
WORK=ROOT/'tmp/sdk-self-restart'/uuid.uuid4().hex
WORK.mkdir(parents=True)
BINARY=(Path(sys.argv[1]) if len(sys.argv)>1 else ROOT/'target/debug'/('atelier-kernel.exe' if os.name=='nt' else 'atelier-kernel')).resolve()
PY='''import asyncio,json,os,sys
from pathlib import Path
from atelier_sdk import AtelierDomain
d=AtelierDomain(domain="self_py")
@d.handler("ping")
def ping(msg):return {"ready":True}
async def start():
    root=Path(sys.argv[1])
    first=root/"python-first.json"
    if not first.exists():
        first.write_text(json.dumps({"pid":os.getpid()}))
        await d.supervisor.restart("self_py")
    else:
        try:
            await d.supervisor.start("unconfigured_probe")
        except Exception as error:
            assert getattr(error,"status",None)=="execution_failed",repr(error)
        else:raise AssertionError("old generation success substituted for new request failure")
        runtime=await d.runtime.query()
        assert any(row["domain_id"]=="self_py" for row in runtime["domains"])
        (root/"python-second.json").write_text(json.dumps({"success":True,"pid":os.getpid(),"old_receipt_isolated":True}))
asyncio.run(d.run_async(on_start=start))
'''
JS='''const fs=require('fs'),path=require('path');
const {AtelierDomain}=require(process.argv[2]);
const d=new AtelierDomain({domain:'self_js'}),root=process.argv[3];
d.handler('ping',()=>({ready:true}));
d.runAsync({onStart:async()=>{
 const first=path.join(root,'node-first.json');
 if(!fs.existsSync(first)){
  fs.writeFileSync(first,JSON.stringify({pid:process.pid}));
  await d.supervisor.restart('self_js');
 }else{
  let failed=false;
  try{await d.supervisor.start('unconfigured_probe');}catch(error){if(error.status!=='execution_failed')throw error;failed=true;}
  if(!failed)throw Error('old generation success substituted for new request failure');
  const runtime=await d.runtime.query();
  if(!runtime.domains.some(row=>row.domain_id==='self_js'))throw Error('runtime missing self');
  fs.writeFileSync(path.join(root,'node-second.json'),JSON.stringify({success:true,pid:process.pid,old_receipt_isolated:true}));
 }
}});
'''
py=WORK/'self.py';py.write_text(PY)
js=WORK/'self.cjs';js.write_text(JS)
node=shutil.which('node');assert node
boot=WORK/'kernel_boot.json'
boot.write_text(json.dumps({'db_path':str(WORK/'kernel.db'),'web_ui_enabled':False,'domains':[
    {'domain_id':'self_py','command':sys.executable,'args':[str(py),str(WORK)],'working_dir':str(ROOT),'auto_restart':False},
    {'domain_id':'self_js','command':node,'args':[str(js),str(ROOT/'sdk/nodejs/index.js'),str(WORK)],'working_dir':str(ROOT),'auto_restart':False}]}))
env=os.environ.copy();env['PYTHONPATH']=str(ROOT/'sdk/python')
with (WORK/'kernel.log').open('w') as log:
    process=subprocess.Popen([str(BINARY),'--headless','--config',str(boot)],cwd=ROOT,env=env,stdout=log,stderr=log)
    try:
        deadline=time.monotonic()+20
        while not all((WORK/file).exists() for file in ['python-second.json','node-second.json']):
            assert process.poll() is None,'kernel exited'
            if time.monotonic()>deadline:raise TimeoutError(str(WORK/'kernel.log'))
            time.sleep(.05)
        results={}
        for language in ['python','node']:
            first=json.loads((WORK/f'{language}-first.json').read_text())
            second=json.loads((WORK/f'{language}-second.json').read_text())
            assert second['success'] and first['pid']!=second['pid']
            results[language]={'before':first,'after':second}
        (WORK/'report.json').write_text(json.dumps({'success':True,'results':results},indent=2))
        print(json.dumps({'success':True,'report':str(WORK/'report.json'),'results':results}))
    except Exception:
        print((WORK/'kernel.log').read_text(errors='replace')[-16000:],file=sys.stderr)
        raise
    finally:
        process.terminate()
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:process.kill();process.wait()
