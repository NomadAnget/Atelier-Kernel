"""Deterministic slow-writer and oversized-frame tests in both SDKs."""
import json,os,subprocess,sys,uuid,threading
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];WORK=ROOT/'tmp/sdk-transport-tests'/str(uuid.uuid4());WORK.mkdir(parents=True)
PY='''import asyncio,json,sys,threading
from atelier_sdk import AtelierDomain,DomainRequestError
d=AtelierDomain(domain='test',max_outbox=1 if sys.argv[1]=='outbox' else 8,max_frame_bytes=1024,shutdown_grace=.1)
@d.handler('ping')
def ping(msg):return {}
async def start():
    while d._outbox.unfinished_tasks:await asyncio.sleep(.001)
    if sys.argv[1]=='startup':raise RuntimeError('start failed')
    if sys.argv[1]=='input':d.logger.info(json.dumps({'ready':True}));return
    if sys.argv[1]=='outbox':
        original=d._write_ipc_bytes;entered=threading.Event();gate=threading.Event()
        def blocked(w_fd,data_bytes):entered.set();gate.wait();return original(w_fd,data_bytes)
        d._write_ipc_bytes=blocked
        d._submit(['worker.test'],'Manual')
        while not entered.is_set():await asyncio.sleep(.001)
        d._submit(['worker.test'],'Manual')
        try:d._submit(['worker.test'],'Manual');raise AssertionError('queue did not reject')
        except DomainRequestError as e:assert e.status=='busy'
        gate.set()
        while d._outbox.unfinished_tasks:await asyncio.sleep(.001)
        d._write_ipc_bytes=original
    else:
        try:await d.request('worker.test','Large',data={'text':'x'*4096});raise AssertionError('frame did not reject')
        except DomainRequestError as e:assert e.status=='frame_too_large'
    d.logger.info(json.dumps({'passed':True,'pending':len(d._pending)}))
async def end():d.logger.info(json.dumps({'cleanup':True}))
try:asyncio.run(d.run_async(on_start=start,on_close=end))
except Exception as error:sys.stderr.write(str(error));sys.exit(1)
'''
JS='''const {AtelierDomain}=require(process.argv[2]);
const d=new AtelierDomain({domain:'test',maxOutbox:process.argv[3]==='outbox'?1:8,maxFrameBytes:1024,shutdownGrace:.1});d.handler('ping',()=>({}));
d.runAsync({onStart:async()=>{
 if(process.argv[3]==='startup')throw new Error('start failed');
 if(process.argv[3]==='input'){d.logger.info(JSON.stringify({ready:true}));return;}
 if(process.argv[3]==='outbox'){
   const out=d._ensureIpcOutput();
   const original=out.write;out.write=function(...args){original.apply(this,args);return false;};
   d._submit(['worker.test'],'Manual');d._submit(['worker.test'],'Manual');
   try{d._submit(['worker.test'],'Manual');throw new Error('queue did not reject');}catch(e){if(e.status!=='busy')throw e;}
   out.write=original;out.emit('drain');
 }else{
   try{await d.request('worker.test','Large',{}, {text:'x'.repeat(4096)});throw new Error('frame did not reject');}catch(e){if(e.status!=='frame_too_large')throw e;}
 }
 d.logger.info(JSON.stringify({passed:true,pending:d._pending.size}));
},onClose:()=>d.logger.info(JSON.stringify({cleanup:true}))}).catch(e=>{process.stderr.write(String(e));process.exitCode=1;});
'''
(WORK/'fixture.py').write_text(PY);(WORK/'fixture.cjs').write_text(JS)
report=[]
for lang in ['python','node']:
    for mode in ['outbox','frame','input','startup']:
        k2d_r,k2d_w=os.pipe()
        d2k_r,d2k_w=os.pipe()
        cmd=[sys.executable,str(WORK/'fixture.py'),mode] if lang=='python' else ['node',str(WORK/'fixture.cjs'),str(ROOT/'sdk/nodejs'),mode]
        env={**os.environ,'PYTHONPATH':str(ROOT/'sdk/python')}
        kwargs={'stdin':subprocess.DEVNULL,'stdout':subprocess.DEVNULL,'stderr':subprocess.PIPE,'text':True}
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
        proc=subprocess.Popen(cmd,env=env,**kwargs)
        os.close(k2d_r);os.close(d2k_w)
        ipc_tx=os.fdopen(k2d_w,'w',encoding='utf-8',buffering=1)
        ipc_rx=os.fdopen(d2k_r,'r',encoding='utf-8',buffering=1)
        watchdog=threading.Timer(4,lambda:proc.kill() if proc.poll() is None else None);watchdog.start()
        frames=[]
        try:
            while True:
                line=ipc_rx.readline()
                if not line and mode in ('startup','input'):break
                assert line,(lang,mode,'early EOF',proc.stderr.read())
                frame=json.loads(line);frames.append(frame)
                if frame['frame_type']=='log' and frame['record']['message'].startswith('{'):
                    value=json.loads(frame['record']['message'])
                    if mode=='input' and value.get('ready'):
                        ipc_tx.write(json.dumps({'frame_type':'deliver','data':'x'*4096})+'\n');ipc_tx.flush()
                    elif mode in ('outbox','frame') and value.get('passed'):
                        assert value=={'passed':True,'pending':0};break
            ipc_tx.close();proc.wait(timeout=3);ipc_rx.close()
            error=proc.stderr.read()
            if mode=='startup':assert proc.returncode!=0 and 'start failed' in error
            else:assert proc.returncode==0 and not error,(lang,mode,error)
            if mode=='input':assert any(f.get('record',{}).get('message')=='SDK frame limit exceeded' for f in frames)
            if mode=='outbox':assert len([f for f in frames if f.get('message_type')=='Manual'])==2
            report.append({'language':lang,'case':mode,'passed':True})
        finally:
            watchdog.cancel()
            (WORK/(lang+'-'+mode+'.json')).write_text(json.dumps(frames,indent=2))
            if proc.poll() is None:proc.kill();proc.wait()
(WORK/'report.json').write_text(json.dumps(report,indent=2));print('Transport bounds passed for both SDKs');print('Evidence:',WORK/'report.json')
