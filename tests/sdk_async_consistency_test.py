"""Drive both packaged SDK implementations through identical bidirectional IPC cases."""
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / 'tmp/sdk-async-tests' / str(uuid.uuid4())
WORK.mkdir(parents=True)
PY = '''import asyncio,json,sys
from atelier_sdk import AtelierDomain,DomainRequestError
d=AtelierDomain(domain='test')
@d.handler('relay')
async def relay(msg):
    action=msg.data['action']
    if action=='stats': return {'pending':len(d._pending)}
    try:
        if action=='writefail':
            original=d._submit
            def fail(*args,**kwargs): raise BrokenPipeError('injected write failure')
            d._submit=fail
            try: await d.request('worker.echo','AsyncProbe')
            finally: d._submit=original
        if action=='cancel':
            task=asyncio.create_task(d.request('worker.echo','AsyncProbe',data=msg.data,timeout=2))
            await asyncio.sleep(.02);task.cancel()
            try: await task
            except asyncio.CancelledError: return {'status':'cancelled'}
        frame=await d.request('worker.echo','AsyncProbe',data=msg.data,timeout=.12 if action=='timeout' else 2)
        return {'status':'completed','token':frame['data']['token'],'bytes':len(frame['data'].get('blob',''))}
    except DomainRequestError as e: return {'status':e.status,'data':e.data,'message_id':e.message_id}
async def end():
    try: await d.request('worker.echo','AfterClose')
    except DomainRequestError as e: d.logger.info(json.dumps({'closed':e.status,'pending':len(d._pending)}))
async def begin(): d._submit(['worker.manual'],'Manual',seq=10000)
asyncio.run(d.run_async(on_start=begin,on_close=end))
'''
JS = '''const {AtelierDomain}=require(process.argv[2]);
const d=new AtelierDomain({domain:'test'});
d.handler('relay',async msg=>{
  const action=msg.data.action;
  if(action==='stats')return {pending:d._pending.size};
  try {
    if(action==='writefail'){
      const original=d._submit;d._submit=()=>{throw new Error('injected write failure');};
      try{await d.request('worker.echo','AsyncProbe');}finally{d._submit=original;}
    }
    let controller;
    if(action==='cancel'){controller=new AbortController();setTimeout(()=>controller.abort(),20);}
    const frame=await d.request('worker.echo','AsyncProbe',{},msg.data,{timeout:action==='timeout'?.12:2,signal:controller?.signal});
    return {status:'completed',token:frame.data.token,bytes:(frame.data.blob||'').length};
  } catch(e){return {status:e.status,data:e.data,message_id:e.message_id};}
});
d.runAsync({onStart:()=>d._submit(['worker.manual'],'Manual',{}, {},10000),onClose:async()=>{
 try{await d.request('worker.echo','AfterClose');}catch(e){d.logger.info(JSON.stringify({closed:e.status,pending:d._pending.size}));}
}}).catch(e=>{process.stderr.write(String(e));process.exitCode=1;});
'''
(WORK/'fixture.py').write_text(PY)
(WORK/'fixture.cjs').write_text(JS)

def exercise(language):
    stderr=open(WORK/(language+'.stderr'),'w')
    k2d_r,k2d_w=os.pipe()
    d2k_r,d2k_w=os.pipe()
    env={**os.environ,'PYTHONPATH':str(ROOT/'sdk/python'),'PYTHON':sys.executable,'PYTHON_BIN':sys.executable}
    kwargs={'stdin':subprocess.DEVNULL,'stdout':subprocess.DEVNULL,'stderr':stderr,'text':True,'bufsize':1}
    if os.name=='nt':
        import msvcrt
        os.set_inheritable(k2d_r,True);os.set_inheritable(d2k_w,True)
        env['ATELIER_IPC_READ_FD']=str(msvcrt.get_osfhandle(k2d_r))
        env['ATELIER_IPC_WRITE_FD']=str(msvcrt.get_osfhandle(d2k_w))
        env['ATELIER_IPC_HANDLE_MODE']='win32'
        kwargs['close_fds']=False
    else:
        env['ATELIER_IPC_READ_FD']=str(k2d_r)
        env['ATELIER_IPC_WRITE_FD']=str(d2k_w)
        kwargs['pass_fds']=(k2d_r,d2k_w)
    command=[sys.executable,str(WORK/'fixture.py')] if language=='python' else ['node',str(WORK/'fixture.cjs'),str(ROOT/'sdk/nodejs')]
    process=subprocess.Popen(command,env=env,**kwargs)
    os.close(k2d_r);os.close(d2k_w)
    ipc_tx=os.fdopen(k2d_w,'w',encoding='utf-8',buffering=1)
    ipc_rx=os.fdopen(d2k_r,'r',encoding='utf-8',buffering=1)
    inbox=queue.Queue()
    def read():
        for line in ipc_rx:inbox.put(json.loads(line))
        ipc_rx.close()
        inbox.put(None)
    threading.Thread(target=read,daemon=True).start()
    sent=[];logs=[];batch=[];sequence=0
    def write(frame):ipc_tx.write(json.dumps(frame,ensure_ascii=False)+'\n');ipc_tx.flush()
    def deliver(action,token='',blob=''):
        nonlocal sequence
        sequence+=1;mid='incoming-'+str(sequence)
        write(dict(frame_type='deliver',message_id=mid,target='test.relay',source_domain='caller',targets=['test.relay'],message_type='Call',params={},data=dict(action=action,token=token,blob=blob)))
        return mid
    def respond(frame,status='completed',target='worker.echo'):
        data={'token':frame['data']['token'],'blob':frame['data'].get('blob','')} if status=='completed' else {'reason':'expected failure'}
        write(dict(frame_type='receipt',seq=frame['seq'],target=target,message_id='rpc-'+str(frame['seq']),status=status,data=data))
    def next_frame():
        frame=inbox.get(timeout=5)
        if frame is None:raise AssertionError(language+' exited unexpectedly')
        if frame['frame_type']=='log':logs.append(frame['record'])
        if frame['frame_type']=='send':
            sent.append(frame)
            if frame['message_type']=='AsyncProbe':
                action=frame['data']['action']
                if action=='concurrent':
                    batch.append(frame)
                    if len(batch)==8:
                        for item in reversed(batch):
                            respond(item,target='wrong.target')
                            respond(item,'started');respond(item);respond(item)
                elif action not in ('timeout','cancel','disconnect'):
                    respond(frame,'received');respond(frame,'started')
                    respond(frame,action if action in ('execution_failed','target_invalid','contract_invalid') else 'completed')
        return frame
    def completed(mid):
        while True:
            frame=next_frame()
            if frame.get('message_id')==mid and frame.get('status') in ('completed','execution_failed'):
                assert frame['status']=='completed',frame
                return frame['data']
    results=[]
    try:
        while not sent:next_frame()
        ipc_tx.write('invalid json\n[]\n');ipc_tx.flush()
        mids=[deliver('concurrent',str(i)) for i in range(8)]
        got={}
        while len(got)<8:
            frame=next_frame()
            if frame.get('message_id') in mids and frame.get('status')=='completed':got[frame['message_id']]=frame['data']
        assert [got[mid]['token'] for mid in mids]==list(map(str,range(8)))
        assert len({f['seq'] for f in sent})==len(sent),sent
        assert all(f['seq']>10000 for f in sent if f['message_type']=='AsyncProbe')
        results.append('concurrent / out of order / intermediate / duplicate / wrong target')
        for status in ('execution_failed','target_invalid','contract_invalid'):
            result=completed(deliver(status))
            assert result['status']==status and result['data']=={'reason':'expected failure'}
            assert result['message_id'].startswith('rpc-')
            results.append(status)
        assert completed(deliver('timeout'))['status']=='timeout'
        late=next(f for f in reversed(sent) if f.get('data',{}).get('action')=='timeout')
        respond(late)
        assert completed(deliver('cancel'))['status']=='cancelled'
        assert completed(deliver('writefail'))['status']=='connection_closed'
        results.append('timeout / late receipt / cancellation')
        blob='x'*(512*1024)
        large=completed(deliver('large','large',blob))
        assert large=={'status':'completed','token':'large','bytes':len(blob)}
        assert completed(deliver('stats'))=={'pending':0}
        results.append('512KiB delivery and receipt / pending cleanup')
        mid=deliver('disconnect')
        while not any(f.get('data',{}).get('action')=='disconnect' for f in sent):next_frame()
        ipc_tx.close()
        while True:
            frame=inbox.get(timeout=5)
            if frame is None:break
            if frame['frame_type']=='log':logs.append(frame['record'])
        assert process.wait(timeout=5)==0
        assert any(json.loads(r['message'])=={'closed':'connection_closed','pending':0} for r in logs if r['message'].startswith('{'))
        results.append('EOF / closed transport / pending cleanup')
    finally:
        if process.poll() is None:process.kill();process.wait()
        stderr.close()
    assert not (WORK/(language+'.stderr')).read_text(),(WORK/(language+'.stderr')).read_text()
    return results

py=exercise('python');node=exercise('node')
assert py==node
(WORK/'report.json').write_text(json.dumps({'python':py,'node':node,'passed':True},indent=2))
print('Both SDKs passed identical async IPC cases:',len(py))
print('Evidence:',WORK/'report.json')
