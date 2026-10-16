"""Same ID-returning API and early/late result scenarios for Python and Node."""
from pathlib import Path
source=(Path(__file__).with_name('sdk_portable_features_test.py')).read_text()
exec(compile(source[:source.index('\nresults=[]')],__file__,'exec'))

PY=PY.replace("if mode=='pending':", "if mode in ('pending','cache'):")
PY=PY.replace('async def start():','''@d.handler('ids')
async def ids(msg):
    ids=await d.send(['worker.ok','worker.bad','worker.silent','worker.ok'],'Fanout')
    await asyncio.sleep(.03)
    result=await d.request_many(list(reversed(ids)),timeout=.03)
    for mid in ids:d.forget_result(mid)
    return {'ids':ids,'results':result}
@d.handler('cache')
async def cache(msg):
    mid=await d.send('worker.echo','Cache')
    try:await d.send('worker.echo','Cache');raise AssertionError('unread result was evicted')
    except DomainRequestError as e:assert e.status=='busy'
    result=await d.wait_result(mid)
    mid2=await d.send('worker.echo','Cache');assert d.forget_result(mid2)
    try:await d.wait_result(mid2)
    except DomainRequestError as e:return {'id':mid,'receipt':result,'forgotten':e.status}
@d.handler('late')
async def late(msg):
    mid=await d.send('worker.echo','Late')
    try:await d.wait_result(mid,timeout=.02)
    except DomainRequestError as e:return {'id':mid,'status':e.status}
@d.handler('cancel')
async def cancel(msg):
    mid=await d.send('worker.echo','Late')
    task=asyncio.create_task(d.wait_result(mid))
    await asyncio.sleep(.01);task.cancel()
    try:await task
    except asyncio.CancelledError:return {'id':mid,'status':'cancelled'}
@d.handler('parallel')
async def parallel(msg):
    mid=await d.send('worker.echo','Cache')
    values=await asyncio.gather(d.wait_result(mid),d.wait_result(mid))
    return {'ids':[x['message_id'] for x in values],'id':mid}
@d.handler('get')
async def get(msg):
    try:return await d.wait_result(msg.data['id'],timeout=.2)
    except DomainRequestError as e:return {'status':e.status}
async def start():''')
JS=JS.replace("if(mode==='pending')", "if(['pending','cache'].includes(mode))")
JS=JS.replace('d.runAsync({', '''d.handler('ids',async()=>{
 const ids=await d.send(['worker.ok','worker.bad','worker.silent','worker.ok'],'Fanout');
 await new Promise(r=>setTimeout(r,30));
 const result=await d.requestMany([...ids].reverse(),{timeout:.03});
 for(const id of ids)d.forgetResult(id);return {ids,results:result};
});
d.handler('cache',async()=>{
 const mid=await d.send('worker.echo','Cache');
 try{await d.send('worker.echo','Cache');throw Error('unread result was evicted');}catch(e){if(e.status!=='busy')throw e;}
 const result=await d.waitResult(mid),mid2=await d.send('worker.echo','Cache');if(!d.forgetResult(mid2))throw Error('missing result');
 try{await d.waitResult(mid2);}catch(e){return {id:mid,receipt:result,forgotten:e.status};}
});
d.handler('late',async()=>{const mid=await d.send('worker.echo','Late');try{await d.waitResult(mid,{timeout:.02});}catch(e){return {id:mid,status:e.status};}});
d.handler('cancel',async()=>{const mid=await d.send('worker.echo','Late'),controller=new AbortController();setTimeout(()=>controller.abort(),10);try{await d.waitResult(mid,{signal:controller.signal});}catch(e){return {id:mid,status:e.status};}});
d.handler('parallel',async()=>{const mid=await d.send('worker.echo','Cache');const values=await Promise.all([d.waitResult(mid),d.waitResult(mid)]);return {ids:values.map(x=>x.message_id),id:mid};});
d.handler('get',async msg=>{try{return await d.waitResult(msg.data.id,{timeout:.2});}catch(e){return {status:e.status};}});
d.runAsync({''')
(WORK/'fixture.py').write_text(PY);(WORK/'fixture.cjs').write_text(JS)
original_step=Driver.step
def step(self):
    frame=original_step(self)
    if frame and frame.get('frame_type')=='send':
        if frame['message_type']=='Cache':self.reply(frame,data={'early':True})
        if frame['message_type']=='Late':self.reply(frame,status='sent');self.pending.append(frame)
    return frame
Driver.step=step
results=[]
for lang in ['python','node']:
    checks=[]
    for mode in ['ids','cache','late','cancel','parallel']:
        d=Driver(lang,mode)
        try:
            d.registered();frame=d.done(d.call(mode));assert frame['status']=='completed',frame
            data=frame['data']
            if mode=='ids':
                assert len(data['ids'])==len(set(data['ids']))==3
                assert [x['message_id'] for x in data['results']]==list(reversed(data['ids']))
                assert [x['status'] for x in data['results']]==['timeout','target_invalid','completed']
                assert len([r for r in d.requests if r['message_type']=='Fanout'])==1
            elif mode=='cache':assert isinstance(data['id'],str) and data['forgotten']=='unknown_message' and data['receipt']['data']=={'early':True}
            elif mode=='parallel':assert data['ids']==[data['id'],data['id']]
            else:
                assert data['status']==('cancelled' if mode=='cancel' else 'timeout')
                # An RPC timeout does not discard the receipt: retry by the same ID, no re-send.
                retry=d.call('get',data={'id':data['id']});d.reply(d.pending[0],data={'late':True})
                assert d.done(retry)['data']['data']=={'late':True}
                assert d.done(d.call('get',data={'id':data['id']}))['data']['status']=='unknown_message'
                assert len([r for r in d.requests if r['message_type']=='Late'])==1
            d.finish();checks.append(semantic(d))
        finally:
            if d.proc.poll() is None:d.proc.kill();d.proc.wait()
    results.append(checks)
assert results[0]==results[1]
(WORK/'message-id-report.json').write_text(json.dumps({'passed':True,'python':results[0],'node':results[1]},indent=2))
print('ID-returning and delayed-wait scenarios passed for both SDKs:',WORK/'message-id-report.json')
