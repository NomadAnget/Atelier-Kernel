"""All nonpersistent SDK observation streams isolate slow async callbacks."""
import asyncio,json,subprocess,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'sdk/python'))
from atelier_sdk import AtelierDomain
async def python_case(kind):
    domain=AtelierDomain(domain='slow_probe');release=asyncio.Event();counts={'slow':0,'fast':0}
    async def slow(event):counts['slow']+=1;await release.wait()
    def fast(event):counts['fast']+=1
    field={'observer':'_observation_listeners','telemetry':'_telemetry_listeners','notify':'_notify_listeners'}[kind]
    setattr(domain,field,[slow,fast]);dispatch=getattr(domain,'_dispatch_system_'+kind)
    for n in range(10000):dispatch({'data':{'event_id':n}})
    await asyncio.sleep(0);assert counts=={'slow':1,'fast':10000},(kind,counts);assert len(domain._callbacks)==1
    release.set()
    for _ in range(3):await asyncio.sleep(0)
    assert not domain._callbacks
    dispatch({'data':{'event_id':10001}})
    for _ in range(3):await asyncio.sleep(0)
    assert counts=={'slow':2,'fast':10001};return counts
async def all_python():return {kind:await python_case(kind) for kind in ['observer','telemetry','notify']}
py=asyncio.run(all_python())
js=r'''
const {AtelierDomain}=require(process.argv[1]);
(async()=>{
 const results={};
 for(const kind of ['observer','telemetry','notify']){
  const d=new AtelierDomain({domain:'slow_probe'});let release;const gate=new Promise(resolve=>release=resolve),counts={slow:0,fast:0};
  const field={observer:'_observationListeners',telemetry:'_telemetryListeners',notify:'_notifyListeners'}[kind];
  d[field]=[async()=>{counts.slow++;await gate;},()=>{counts.fast++;}];
  const dispatch=d[{observer:'_dispatchSystemObserver',telemetry:'_dispatchSystemTelemetry',notify:'_dispatchSystemNotify'}[kind]].bind(d);
  for(let n=0;n<10000;n++)dispatch({data:{event_id:n}});
  if(counts.slow!==1||counts.fast!==10000||d._callbacks.size!==1)throw Error(kind+JSON.stringify(counts));
  release();await new Promise(setImmediate);if(d._callbacks.size)throw Error('retained callback');
  dispatch({data:{event_id:10001}});await new Promise(setImmediate);
  if(counts.slow!==2||counts.fast!==10001)throw Error('did not recover');results[kind]=counts;
 }
 console.log(JSON.stringify(results));
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
node=json.loads(subprocess.check_output(['node','-e',js,str(ROOT/'sdk/nodejs/index.js')],text=True));assert py==node
folder=ROOT/'tmp/adversarial-audit';folder.mkdir(parents=True,exist_ok=True)
report={'success':True,'python':py,'node':node,'scope':__doc__};(folder/'sdk-slow-observer.json').write_text(json.dumps(report,indent=2));print(json.dumps(report))
