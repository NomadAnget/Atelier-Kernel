"""Exercise official Python/Node paging against the actual Rust kernel; no Web domain."""
import json,os,shutil,subprocess,sys,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
BINARY=(Path(sys.argv[1]) if len(sys.argv)>1 else ROOT/'target/debug'/('atelier-kernel.exe' if os.name=='nt' else 'atelier-kernel')).resolve()
WORK=ROOT/'tmp/sdk-log-pagination'/str(uuid.uuid4());WORK.mkdir(parents=True)
PY=r"""import asyncio,json,sys
from pathlib import Path
from atelier_sdk import AtelierDomain,DomainRequestError
sdk=AtelierDomain(domain='py_page',shutdown_grace=.2)
result=None
@sdk.handler('ping')
def ping(msg):return {'ready':True}
async def start():
 global result
 for i in range(3):sdk.logger.info('page-'+str(i),component='Paging')
 for _ in range(100):
  all_rows=await sdk.logger.query_logs_async(domain='py_page',limit=10)
  if all_rows['total']==3:break
  await asyncio.sleep(.02)
 assert all_rows['total']==3,all_rows
 first=await sdk.query_logs_async(domain='py_page',limit=1)
 assert first['has_more'] and first['next_before_id']==first['logs'][-1]['id']
 mid=await sdk.query_logs(domain='py_page',limit=1,before_id=first['next_before_id'])
 second=(await sdk.wait_result(mid,timeout=3))['data']
 third=await sdk.logger.query_logs_async(domain='py_page',limit=1,before_id=second['next_before_id'])
 assert not third['has_more'] and third['next_before_id'] is None
 messages=[p['logs'][0]['message'] for p in [first,second,third]]
 assert messages==['page-2','page-1','page-0'],messages
 try:await sdk.query_logs_async(before_id=-1)
 except DomainRequestError as error:assert error.status=='contract_invalid'
 else:raise AssertionError('invalid cursor accepted')
 result={'language':'python','messages':messages,'legacy_send_wait':True,'invalid_cursor_rejected':True}
 asyncio.create_task(sdk.shutdown(grace=.2))
asyncio.run(sdk.run_async(on_start=start))
Path(sys.argv[1]).write_text(json.dumps(result))
"""
JS=r"""const fs=require('fs');
const assert=require('node:assert/strict');
const {AtelierDomain}=require(process.argv[2]);
const sdk=new AtelierDomain({domain:'js_page',shutdownGrace:.2});let result;
sdk.handler('ping',()=>({ready:true}));
sdk.runAsync({onStart:async()=>{
 for(let i=0;i<3;i++)sdk.logger.info('page-'+i,{component:'Paging'});
 let all;for(let i=0;i<100;i++){all=await sdk.logger.queryLogsAsync({domain:'js_page',limit:10});if(all.total===3)break;await new Promise(r=>setTimeout(r,20));}assert.equal(all.total,3);
 const first=await sdk.queryLogsAsync({domain:'js_page',limit:1});assert.ok(first.has_more);assert.equal(first.next_before_id,first.logs[0].id);
 const mid=await sdk.queryLogs({domain:'js_page',limit:1,before_id:first.next_before_id});const second=(await sdk.waitResult(mid,{timeout:3})).data;
 const third=await sdk.logger.queryLogsAsync({domain:'js_page',limit:1,beforeId:second.next_before_id});assert.equal(third.has_more,false);assert.equal(third.next_before_id,null);
 const messages=[first,second,third].map(p=>p.logs[0].message);assert.deepEqual(messages,['page-2','page-1','page-0']);
 await assert.rejects(sdk.queryLogsAsync({beforeId:-1}),error=>error.status==='contract_invalid');
 result={language:'node',messages,legacy_send_wait:true,invalid_cursor_rejected:true};setImmediate(()=>sdk.shutdown({grace:.2}));
}}).then(()=>fs.writeFileSync(process.argv[3],JSON.stringify(result))).catch(error=>{console.error(error);process.exitCode=1;});
"""
(WORK/'probe.py').write_text(PY);(WORK/'probe.cjs').write_text(JS)
config={'db_path':str(WORK/'history.db'),'web_ui_enabled':False,'domains':[
 {'domain_id':'py_page','command':sys.executable,'args':[str(WORK/'probe.py'),str(WORK/'python.json')],'working_dir':str(ROOT),'auto_restart':False},
 {'domain_id':'js_page','command':shutil.which('node'),'args':[str(WORK/'probe.cjs'),str(ROOT/'sdk/nodejs'),str(WORK/'node.json')],'working_dir':str(ROOT),'auto_restart':False}]}
(WORK/'kernel_boot.json').write_text(json.dumps(config));env={**os.environ,'PYTHONPATH':str(ROOT/'sdk/python'),'PYTHON':sys.executable,'PYTHON_BIN':sys.executable}
with (WORK/'kernel.log').open('w') as log:
 child=subprocess.Popen([str(BINARY),'--headless','--config',str(WORK/'kernel_boot.json')],cwd=ROOT,env=env,stdout=log,stderr=log)
 try:
  end=time.monotonic()+20
  while time.monotonic()<end:
   if all((WORK/name).exists() for name in ['python.json','node.json']):break
   if child.poll() is not None:raise RuntimeError((WORK/'kernel.log').read_text())
   time.sleep(.05)
  else:raise AssertionError((WORK/'kernel.log').read_text())
  result=[json.loads((WORK/name).read_text()) for name in ['python.json','node.json']]
  assert all(item and item['invalid_cursor_rejected'] for item in result),result
  (WORK/'result.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
 finally:
  if child.poll() is None:child.terminate()
  try:child.wait(timeout=5)
  except subprocess.TimeoutExpired:child.kill();child.wait()
