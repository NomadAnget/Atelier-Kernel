"""Paired protocol-client scenarios; business payloads and policies belong to domains."""
import json,os,queue,subprocess,sys,threading,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
WORK=ROOT/'tmp/sdk-portable-tests'/str(uuid.uuid4());WORK.mkdir(parents=True)
PY='''import asyncio,json,sys
from atelier_sdk import AtelierDomain,DomainRequestError,DomainExecutionFailed
mode=sys.argv[1]
limits={}
if mode=='pending': limits['max_pending']=1
d=AtelierDomain(domain='test',shutdown_grace=.1,**limits)
@d.handler('work')
async def work(msg):
    snapshot=d.context;snapshot['message_id']='mutated'
    assert d.context['message_id']==msg.message_id
    d.logger.info('context log')
    if mode in ('limits','shutdown'): await asyncio.sleep(1 if mode=='shutdown' else .12)
    if mode=='context': await d.request('worker.echo','Nested',data={'token':msg.message_id})
    return {'amount':msg.data['amount'],'context':d.context}
@d.handler('rpc')
async def rpc(msg):
    try: return {'status':(await d.request('worker.echo','Pending',data={'token':msg.message_id},timeout=1))['status']}
    except DomainRequestError as e: return {'status':e.status}
@d.handler('stop')
async def stop(msg):
    asyncio.create_task(d.shutdown(grace=.03));return {'stopping':True}
@d.handler('error')
def error(msg): raise DomainExecutionFailed({'domain_defined': ['free', 7]})
@d.handler('echo')
def echo(msg): return msg.data
@d.handler('many')
async def many(msg): return await d.wait_results(await d.send(['worker.ok','worker.bad','worker.silent'],'Fanout'),timeout=.08)
@d.handler('store')
async def store(msg):
    return [await d.storage_insert_async('id','category',{'n':1}),await d.storage_update_async('id',{'n':2}),await d.storage_query_async(domain='shared',record_id='id',category='category',filter={'n':2},limit=7),await d.storage_delete_async(record_id='id')]
async def start():
    if mode=='startup': await asyncio.sleep(.15)
async def end(): d.logger.info(json.dumps({'cleanup':True}))
asyncio.run(d.run_async(on_start=start,on_close=end))
'''
JS='''const {AtelierDomain,DomainExecutionFailed}=require(process.argv[2]);
const mode=process.argv[3];
const limits={};if(mode==='pending')limits.maxPending=1;
const d=new AtelierDomain({domain:'test',shutdownGrace:.1,...limits});
d.handler('work',async msg=>{
 const snapshot=d.context;snapshot.message_id='mutated';if(d.context.message_id!==msg.message_id)throw Error('mutable context');
 d.logger.info('context log');
 if(['limits','shutdown'].includes(mode))await new Promise((resolve,reject)=>{const timer=setTimeout(resolve,mode==='shutdown'?1000:120);msg.signal.addEventListener('abort',()=>{clearTimeout(timer);reject(new Error('aborted'));},{once:true});});
 if(mode==='context')await d.request('worker.echo','Nested',{}, {token:msg.message_id});
 return {amount:msg.data.amount,context:d.context};
});
d.handler('rpc',async msg=>{try{return {status:(await d.request('worker.echo','Pending',{}, {token:msg.message_id},{timeout:1})).status};}catch(e){return {status:e.status};}});
d.handler('stop',()=>{d.shutdown({grace:.03});return {stopping:true};});
d.handler('error',()=>{throw new DomainExecutionFailed({domain_defined:['free',7]});});
d.handler('echo',msg=>msg.data);
d.handler('many',async()=>d.waitResults(await d.send(['worker.ok','worker.bad','worker.silent'],'Fanout'),{timeout:.08}));
d.handler('store',async()=>[await d.storageInsertAsync('id','category',{n:1}),await d.storageUpdateAsync('id',{n:2}),await d.storageQueryAsync({domain:'shared',recordId:'id',category:'category',filter:{n:2},limit:7}),await d.storageDeleteAsync({recordId:'id'})]);
d.runAsync({onStart:async()=>{if(mode==='startup')await new Promise(r=>setTimeout(r,150));},onClose:()=>d.logger.info(JSON.stringify({cleanup:true}))}).catch(e=>{process.stderr.write(String(e));process.exitCode=1;});
'''
(WORK/'fixture.py').write_text(PY);(WORK/'fixture.cjs').write_text(JS)

class Driver:
    def __init__(self,lang,mode):
        self.lang,self.mode=lang,mode;self.events=[];self.ids=0;self.dep=0;self.requests=[];self.pending=[];self.logs=[]
        self.stderr=open(WORK/(lang+'-'+mode+'.stderr'),'w')
        cmd=[sys.executable,str(WORK/'fixture.py'),mode] if lang=='python' else ['node',str(WORK/'fixture.cjs'),str(ROOT/'sdk/nodejs'),mode]
        self.proc=subprocess.Popen(cmd,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=self.stderr,text=True,env={**os.environ,'PYTHONPATH':str(ROOT/'sdk/python')},bufsize=1)
        self.q=queue.Queue()
        def read():
            for line in self.proc.stdout:self.q.put(json.loads(line))
            self.q.put(None)
        threading.Thread(target=read,daemon=True).start()
    def write(self,frame):self.proc.stdin.write(json.dumps(frame,ensure_ascii=False)+'\n');self.proc.stdin.flush()
    def call(self,handler='work',data='__default__',source='allowed',trace=None):
        self.ids+=1;mid='call-'+str(self.ids)
        params={'caller_value':trace} if trace is not None else {}
        self.write(dict(frame_type='deliver',logical_id='logical-'+mid,message_id=mid,target='test.'+handler,source_domain=source,message_type='Call',params=params,data={'amount':1} if data=='__default__' else data))
        return mid
    def reply(self,frame,status='completed',data=None,target=None):
        self.write(dict(frame_type='receipt',seq=frame['seq'],target=target or frame['targets'][0],message_id='rpc-'+str(frame['seq'])+'-'+(target or frame['targets'][0]),status=status,data={} if data is None else data))
    def step(self):
        frame=self.q.get(timeout=4)
        if frame is None:return None
        self.events.append(frame)
        if frame['frame_type']=='log':self.logs.append(frame['record'])
        if frame['frame_type']=='send':
            self.requests.append(frame);kind=frame['message_type']
            if kind=='ReadinessProbe':
                self.dep+=1;self.reply(frame,'target_invalid' if self.dep==1 else 'completed',{'ready':self.dep>=3})
            elif kind=='Pending':self.pending.append(frame)
            elif kind=='Nested':
                self.pending.append(frame)
                if len(self.pending)==2:
                    for item in reversed(self.pending):self.reply(item,data={'token':item['data']['token']})
            elif kind.startswith('Storage'):self.reply(frame,data={'kind':kind})
            elif kind=='Fanout':
                for target in reversed(frame['targets']):self.reply(frame,'sent' if target=='worker.silent' else ('target_invalid' if target=='worker.bad' else 'completed'),{'ok':True},target=target)
        return frame
    def until(self,predicate):
        for _ in range(500):
            result=predicate()
            if result:return result
            if self.step() is None:raise AssertionError((self.lang,self.mode,'unexpected EOF',self.events))
        raise AssertionError('event limit')
    def done(self,mid):return self.until(lambda:next((f for f in self.events if f.get('message_id')==mid and f.get('status') in ('completed','execution_failed')),None))
    def registered(self):self.until(lambda:any(f.get('message_type')=='RegisterHandlers' for f in self.events));time.sleep(.02)
    def finish(self,already=False):
        if not already:self.proc.stdin.close()
        for _ in range(500):
            frame=self.step()
            if frame is None:break
        assert self.proc.wait(timeout=3)==0,(self.lang,self.mode)
        self.stderr.close();assert not Path(self.stderr.name).read_text(),Path(self.stderr.name).read_text()
        clean=[json.loads(r['message']) for r in self.logs if r['message'].startswith('{') and json.loads(r['message']).get('cleanup')]
        assert len(clean)==1,(self.lang,self.mode,clean)
        return clean[0]

def semantic(driver):
    finals=[{k:f[k] for k in ('message_id','target','status','data')} for f in driver.events if f['frame_type']=='receipt' and f.get('status') in ('completed','execution_failed')]
    return {'mode':driver.mode,'results':sorted(finals,key=lambda f:f['message_id'])}

results=[]
for lang in ['python','node']:
    checks=[]
    for mode in ['pending','startup','context','passthrough','store','many','shutdown']:
        d=Driver(lang,mode)
        try:
            d.registered()
            if mode=='pending':
                first=d.call('rpc');d.until(lambda:d.pending)
                second=d.call('rpc');assert d.done(second)['data']['status']=='busy'
                d.reply(d.pending[0]);assert d.done(first)['data']['status']=='completed'
            elif mode=='startup':
                assert d.done(d.call())['status']=='completed'
            elif mode=='context':
                first=d.call(trace='trace-A');second=d.call(trace='trace-B')
                for mid,trace in [(first,'trace-A'),(second,'trace-B')]:
                    ctx=d.done(mid)['data']['context'];assert ctx=={'logical_id':'logical-'+mid,'message_id':mid,'source_domain':'allowed','target':'test.work','message_type':'Call'}
                nested=[f for f in d.requests if f['message_type']=='Nested']
                assert len(nested)==2 and all(f['params']=={} for f in nested)
                assert all(not any(k in r for k in ('trace_id','parent_message_id','message_id','logical_id')) for r in d.logs if r['message']=='context log')
            elif mode=='passthrough':
                # No SDK policy intercepts source names, arbitrary data or domain error payloads.
                assert d.done(d.call(source='another-domain'))['status']=='completed'
                assert d.done(d.call('error'))['data']=={'domain_defined':['free',7]}
                for value in [False,0,'',[],None,{'arbitrary':{'shape':True}}]:assert d.done(d.call('echo',data=value))['data']==value
            elif mode=='store':
                answer=d.done(d.call('store'))['data'];assert [x['kind'] for x in answer]==['StorageInsert','StorageUpdate','StorageQuery','StorageDelete']
                request=next(f for f in d.requests if f['message_type']=='StorageQuery');assert request['params']['filter']=={'n':2} and request['params']['domain']=='shared' and request['params']['limit']==7
            elif mode=='many':
                answer=d.done(d.call('many'))['data'];assert [x['target'] for x in answer]==['worker.ok','worker.bad','worker.silent']
                assert [x['status'] for x in answer]==['completed','target_invalid','timeout']
            elif mode=='shutdown':
                first=d.call();d.until(lambda:any(f.get('message_id')==first and f.get('status')=='started' for f in d.events))
                stop=d.call('stop');assert d.done(stop)['status']=='completed'
                reject=d.done(d.call());assert reject['data']['code']=='shutting_down'
                assert d.done(first)['data']['code']=='cancelled'
                d.finish(already=True);checks.append(semantic(d));continue
            d.finish();checks.append(semantic(d))
        finally:
            if d.proc.poll() is None:d.proc.kill();d.proc.wait()
    results.append(checks)
assert results[0]==results[1]
(WORK/'report.json').write_text(json.dumps({'passed':True,'python':results[0],'node':results[1]},indent=2))
print('Portable feature scenarios passed for both SDKs:',len(results[0]));print('Evidence:',WORK/'report.json')
