"""Both SDKs reject execution overflow without spawning an unbounded task backlog."""
import json,os,subprocess,sys,threading,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
WORK=ROOT/'tmp/core-sdk-execution'/str(uuid.uuid4());WORK.mkdir(parents=True)
PY='''import asyncio
from atelier_sdk import AtelierDomain
d=AtelierDomain(domain='test',max_pending=1,max_outbox=4096,shutdown_grace=.2)
@d.handler('ping')
async def ping(msg):
    await asyncio.sleep(2)
    assert len(d._active)<=1024
    return {'ok':True}
async def ready():d.logger.info('execution-probe-ready')
asyncio.run(d.run_async(on_start=ready))
'''
JS='''const {AtelierDomain}=require(process.argv[2]);
const d=new AtelierDomain({domain:'test',maxPending:1,maxOutbox:4096,shutdownGrace:.2});
d.handler('ping',async()=>{await new Promise(r=>setTimeout(r,2000));if(d._active.size>1024)throw new Error('unbounded execution');return {ok:true};});
d.runAsync({onStart:()=>d.logger.info('execution-probe-ready')}).catch(e=>{process.stderr.write(String(e));process.exitCode=1;});
'''
(WORK/'fixture.py').write_text(PY);(WORK/'fixture.cjs').write_text(JS)
results=[]
for lang in ['python','node']:
    a,b=os.pipe();c,d=os.pipe()
    env={**os.environ,'PYTHONPATH':str(ROOT/'sdk/python'),'ATELIER_IPC_READ_FD':str(a),'ATELIER_IPC_WRITE_FD':str(d),'PYTHON':sys.executable,'PYTHON_BIN':sys.executable}
    cmd=[sys.executable,str(WORK/'fixture.py')] if lang=='python' else ['node',str(WORK/'fixture.cjs'),str(ROOT/'sdk/nodejs')]
    proc=subprocess.Popen(cmd,env=env,pass_fds=(a,d),stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True)
    os.close(a);os.close(d);tx=os.fdopen(b,'w',buffering=1);rx=os.fdopen(c,'r');frames=[]
    watch=threading.Timer(8,lambda:proc.kill() if proc.poll() is None else None);watch.start()
    try:
        while True:
            line=rx.readline();assert line,(lang,'early EOF',proc.stderr.read());frame=json.loads(line);frames.append(frame)
            if frame.get('record',{}).get('message')=='execution-probe-ready':
                def submit():
                    for n in range(1,1026):tx.write(json.dumps({'frame_type':'deliver','logical_id':'l'+str(n),'message_id':'m'+str(n),'source_domain':'caller','target':'test.ping','targets':['test.ping'],'message_type':'Durable','params':{},'data':{}})+'\n')
                    tx.flush()
                writer=threading.Thread(target=submit);writer.start()
            terminal={f.get('message_id'):f for f in frames if f.get('frame_type')=='receipt' and f.get('status') in ['completed','execution_failed']}
            if len(terminal)==1025:
                assert terminal['m1']['status']=='completed',(lang,terminal)
                assert terminal['m1025']['data']['error']=='sdk_execution_capacity_exhausted',(lang,terminal)
                assert not any(f.get('message_id')=='m1025' and f.get('status')=='started' for f in frames)
                break
        writer.join(timeout=2);assert not writer.is_alive()
        tx.close();proc.wait(timeout=4);assert proc.returncode==0,(lang,proc.stderr.read());results.append({'language':lang,'passed':True})
    finally:
        watch.cancel()
        if proc.poll() is None:proc.kill();proc.wait()
        rx.close();(WORK/(lang+'.json')).write_text(json.dumps(frames,indent=2))
print(json.dumps(results));print('Evidence:',WORK)
