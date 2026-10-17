const fs=require('fs');
const net=require('net');
const readline=require('readline');
const {spawn}=require('child_process');
const {AsyncLocalStorage}=require('async_hooks');
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const context=new AsyncLocalStorage();
let _claimedIpcFds=null;

const _WIN32_BRIDGE_PY=[
  'import ctypes,msvcrt,os,sys,threading',
  'k32=ctypes.windll.kernel32',
  'r_h,w_h,ppid=int(sys.argv[1]),int(sys.argv[2]),int(sys.argv[3])',
  'k32.SetHandleInformation(ctypes.c_void_p(r_h),1,0)',
  'k32.SetHandleInformation(ctypes.c_void_p(w_h),1,0)',
  'h_p=k32.OpenProcess(0x0040,False,ppid)',
  'if h_p:',
  ' d=ctypes.c_void_p()',
  ' k32.DuplicateHandle(h_p,ctypes.c_void_p(r_h),0,ctypes.byref(d),0,False,1)',
  ' k32.DuplicateHandle(h_p,ctypes.c_void_p(w_h),0,ctypes.byref(d),0,False,1)',
  ' k32.CloseHandle(h_p)',
  'msvcrt.setmode(0,os.O_BINARY);msvcrt.setmode(1,os.O_BINARY)',
  'r_fd=msvcrt.open_osfhandle(r_h,os.O_RDONLY|os.O_BINARY)',
  'w_fd=msvcrt.open_osfhandle(w_h,os.O_BINARY)',
  'def p_in():',
  ' try:',
  '  while True:',
  '   b=os.read(r_fd,65536)',
  '   if not b:break',
  '   mv=memoryview(b)',
  '   while mv:mv=mv[os.write(1,mv):]',
  ' except Exception:pass',
  ' finally:',
  '  try:os.close(1)',
  '  except Exception:pass',
  'def p_out():',
  ' try:',
  '  while True:',
  '   b=os.read(0,65536)',
  '   if not b:break',
  '   mv=memoryview(b)',
  '   while mv:mv=mv[os.write(w_fd,mv):]',
  ' except Exception:pass',
  ' finally:',
  '  for fd in (w_fd,r_fd):',
  '   try:os.close(fd)',
  '   except Exception:pass',
  '  os._exit(0)',
  't1=threading.Thread(target=p_in,daemon=True);t2=threading.Thread(target=p_out,daemon=True)',
  't1.start();t2.start();t2.join()'
].join('\n');

function _claimIpcFds(){
  if(_claimedIpcFds)return _claimedIpcFds;
  const rRaw=process.env.ATELIER_IPC_READ_FD;
  const wRaw=process.env.ATELIER_IPC_WRITE_FD;
  const handleMode=process.env.ATELIER_IPC_HANDLE_MODE||'';
  delete process.env.ATELIER_IPC_READ_FD;
  delete process.env.ATELIER_IPC_WRITE_FD;
  delete process.env.ATELIER_IPC_HANDLE_MODE;
  if(rRaw===undefined||wRaw===undefined)return null;
  const readFd=Number.parseInt(rRaw,10);
  const writeFd=Number.parseInt(wRaw,10);
  if(!Number.isInteger(readFd)||readFd<0||!Number.isInteger(writeFd)||writeFd<0){
    throw new Error('invalid ATELIER_IPC_READ_FD / ATELIER_IPC_WRITE_FD');
  }
  _claimedIpcFds={readFd,writeFd,handleMode,streams:null};
  return _claimedIpcFds;
}
_claimIpcFds();

function enhance(Base,RequestError,ExecutionFailed) {
  class RuntimeDomain extends Base {
    _requireIpcFds(){
      const fds=_claimIpcFds();
      if(!fds)throw new Error('missing ATELIER_IPC_READ_FD / ATELIER_IPC_WRITE_FD: dedicated dual anonymous pipes are required');
      return fds;
    }
    _ensureIpcStreams(){
      const fds=this._requireIpcFds();
      if(!fds.streams){
        if(process.platform==='win32'&&(fds.handleMode==='win32'||fds.handleMode==='win32_handle')){
          const pyCmd=process.env.PYTHON||'python';
          const bridge=spawn(pyCmd,['-u','-c',_WIN32_BRIDGE_PY,String(fds.readFd),String(fds.writeFd),String(process.pid)],{stdio:['pipe','pipe','inherit'],windowsHide:true});
          bridge.stdout.pause();
          const exitPromise=new Promise(resolve=>{bridge.once('exit',resolve);bridge.once('error',resolve);});
          fds.streams={input:bridge.stdout,output:bridge.stdin,bridge,exitPromise};
        }else{
          const input=new net.Socket({fd:fds.readFd,readable:true,writable:false});
          input.pause();
          const output=new net.Socket({fd:fds.writeFd,readable:false,writable:true});
          fds.streams={input,output,bridge:null,exitPromise:null};
        }
      }
      this._ipcInput=fds.streams.input;
      this._ipcOutput=fds.streams.output;
      this._ipcBridge=fds.streams.bridge;
      this._ipcBridgeExit=fds.streams.exitPromise;
      return fds.streams;
    }
    _ensureIpcOutput(){
      if(!this._ipcOutput)this._ensureIpcStreams();
      return this._ipcOutput;
    }
    _diagnostic(message){try{this.logger.error(message);}catch(_){process.stderr.write(message+'\n');}}
    constructor(options={}) {
      super(options);
      const defaults={maxPending:1024,maxOutbox:256,maxFrameBytes:16777216,shutdownGrace:5};
      for(const key of Object.keys(options))if(!['domain','stageDelayMs',...Object.keys(defaults)].includes(key))throw new Error('unknown SDK option: '+key);
      this._limits={...defaults};
      for(const key of Object.keys(defaults))if(options[key]!==undefined)this._limits[key]=options[key];
      for(const [key,value] of Object.entries(this._limits))if(typeof value!=='number'||!Number.isFinite(value)||value<(key==='shutdownGrace'?0:1)||(key!=='shutdownGrace'&&!Number.isInteger(value)))throw new Error('invalid SDK option: '+key);
      this._state='new';this._closed=false;this._active=new Map();this._results=new Map();this._pending=new Map();
      this._outbox=[];this._blocked=false;this._rl=null;this._ipcInput=null;this._ipcOutput=null;this._ipcBridge=null;this._ipcBridgeExit=null;this._finishing=null;this._writerError=null;this._drainUntil=null;this._callbacks=new Set();
    }
    get context(){const value=context.getStore();if(!value)return {};const {signal,...portable}=value;return {...portable};}
    _currentSignal(){return context.getStore()?.signal;}
    async queryHandlersAsync({domain,target,handler,includeInactive,include_inactive}={},options={}){const params={};for(const [k,v] of Object.entries({domain,target,handler}))if(v!==undefined)params[k]=v;if(includeInactive||include_inactive)params.include_inactive=true;return (await this.request('kernel.registry','QueryHandlers',params,{},options)).data;}
    async getHandlerDescriptionAsync(target,options={}){const res=await this.queryHandlersAsync({target},options);return res?.description??null;}
    async storageInsertAsync(recordId,category,data,upsert=true,options={}){return (await this.request('kernel.storage','StorageInsert',{record_id:recordId,category,upsert},data,options)).data;}
    async storageUpdateAsync(recordId,data,patch=true,options={}){return (await this.request('kernel.storage','StorageUpdate',{record_id:recordId,patch},data,options)).data;}
    async storageDeleteAsync({recordId,record_id,category}={},options={}){const params={};if(recordId!==undefined||record_id!==undefined)params.record_id=recordId??record_id;if(category!==undefined)params.category=category;return (await this.request('kernel.storage','StorageDelete',params,{},options)).data;}
    async storageQueryAsync({domain,recordId,record_id,category,filter,filter_map,limit=500}={},options={}){const params={limit};for(const [key,value] of Object.entries({domain,record_id:recordId??record_id,category,filter:filter??filter_map}))if(value!==undefined)params[key]=value;return (await this.request('kernel.storage','StorageQuery',params,{},options)).data;}
    _writeFrame(frame) {
      if(this._state==='closed')throw new RequestError('connection_closed');
      let line;
      try{line=JSON.stringify(frame,(_,v)=>{if(typeof v==='number'&&!Number.isFinite(v))throw new Error();return v;})+'\n';}
      catch(_){throw new RequestError('invalid_argument',{reason:'message is not JSON serializable'});}
      if(Buffer.byteLength(line)>this._limits.maxFrameBytes)throw new RequestError('frame_too_large');
      if(this._writerError)throw new RequestError('connection_closed');
      const output=this._ensureIpcOutput();
      if(this._blocked){if(this._outbox.length>=this._limits.maxOutbox)throw new RequestError('busy');this._outbox.push(line);}
      else this._blocked=!output.write(line);
    }
    _flush(){const output=this._ensureIpcOutput();while(!this._blocked&&this._outbox.length)this._blocked=!output.write(this._outbox.shift());}
    _failPending(){this._failMessageWaits();}
    close(){
      if(this._closed)return;this._closed=true;this._state=this._state==='new'?'closed':'draining';this._failPending();this._rl?.close();
    }
    async shutdown({grace=this._limits.shutdownGrace}={}) {
      if(!Number.isFinite(grace)||grace<0)throw new Error('invalid shutdown grace');
      if(this._state==='closed')return;
      this._state='draining';const deadline=Date.now()+grace*1000;
      while((this._active.size||this._pending.size)&&Date.now()<deadline)await sleep(5);
      this._drainUntil=Date.now();
      this.close();await this._runPromise;
    }
    run(){this.runAsync().catch(()=>{process.exitCode=1;});}
    runAsync({onStart,onClose}={}) {
      if(this._state!=='new')throw new Error('domain receive session already started or closed');
      this._ensureIpcStreams();
      const output=this._ipcOutput;
      this._state='open';this._onClose=onClose;
      this._runPromise=new Promise((resolve,reject)=>{this._resolveRun=resolve;this._rejectRun=reject;});this._runPromise.catch(()=>{});
      this._drain=()=>{this._blocked=false;this._flush();};this._outputError=error=>{this._writerError=error;this.close();};
      output.on('drain',this._drain);output.on('error',this._outputError);
      this._rl=readline.createInterface({input:this._ipcInput,terminal:false});
      this._rl.on('close',()=>this._finish());
      this._rl.on('line',line=>{
        if(this._closed)return;
        if(Buffer.byteLength(line+'\n')>this._limits.maxFrameBytes){this._diagnostic('SDK frame limit exceeded');this.close();return;}
        let frame;try{frame=JSON.parse(line);if(!frame||typeof frame!=='object'||Array.isArray(frame))throw new Error();}catch(_){this._diagnostic('SDK received malformed JSON frame');return;}
        try{this._receive(frame);}catch(error){this._diagnostic(`SDK dispatch failed: ${error}`);this.close();}
      });
      if(this._handlers.size)this._submit(['kernel.registry'],'RegisterHandlers',{handlers:[...this._handlers.keys()],descriptions:Object.fromEntries(this._handlerDocs||[])},{});
      this._startup=Promise.resolve().then(async()=>{if(onStart)await onStart();}).catch(error=>{this._startupError=error;this._diagnostic(`SDK startup failed: ${error}`);this.close();});
      return this._runPromise;
    }
    _replyError(frame,code,reason){this.reportReceipt(frame.message_id,frame.target,'execution_failed',{code,error:code,reason});}
    _receive(frame) {
      if(frame.frame_type==='receipt'){
        this._receiveResult(frame);
        for(const callback of this._receiptCallbacks){try{const result=callback(frame);if(result?.then){if(this._callbacks.size>=this._limits.maxPending){result.catch(()=>{});this.close();throw new Error('receipt callback capacity exceeded');}const task=Promise.resolve(result);this._callbacks.add(task);task.catch(error=>this._diagnostic(`SDK receipt callback failed: ${error}`)).finally(()=>this._callbacks.delete(task));}}catch(error){this._diagnostic(`SDK receipt callback failed: ${error}`);}}
        return;
      }
      if(frame.frame_type!=='deliver')return;
      if(['message_id','target','source_domain'].some(k=>typeof frame[k]!=='string'||!frame[k]))throw new Error('malformed delivery');
      if(this._state==='draining'){this._replyError(frame,'shutting_down','domain is shutting down');return;}
      this.reportReceipt(frame.message_id,frame.target,'received');this._dispatch(frame);
    }
    _dispatch(frame){
      const controller=new AbortController();
      const task=Promise.resolve().then(()=>this._execute(frame,controller.signal));
      this._active.set(task,{frame,controller});
      task.catch(()=>this.close()).finally(()=>this._active.delete(task));
    }
    async _execute(frame,signal){
      const ctx={logical_id:frame.logical_id||'',message_id:frame.message_id,source_domain:frame.source_domain,target:frame.target,message_type:frame.message_type||'',signal};
      return context.run(ctx,async()=>{
        try {
          this.reportReceipt(frame.message_id,frame.target,'started');
          let abort;
          const abortError=()=>new ExecutionFailed({code:'cancelled',error:'cancelled',reason:'domain is shutting down'});
          const cancelled=new Promise((_,reject)=>{abort=()=>reject(abortError());signal.addEventListener('abort',abort,{once:true});if(signal.aborted)abort();});
          const invoke=async()=>{if(this._stageDelayMs)await sleep(this._stageDelayMs);if(signal.aborted)throw abortError();const fn=this._handlers.get(frame.target.split('.').slice(1).join('.'));if(!fn)throw new ExecutionFailed({error:`Unknown handler: ${frame.target.split('.').slice(1).join('.')}`});return fn({...frame,params:frame.params||{},data:Object.hasOwn(frame,'data')?frame.data:{},targets:frame.targets||[frame.target],signal},this);};
          let result;
          try{result=await Promise.race([invoke(),cancelled]);}finally{signal.removeEventListener('abort',abort);}
          this.reportReceipt(frame.message_id,frame.target,'completed',result??null);
        }catch(error){this.reportReceipt(frame.message_id,frame.target,'execution_failed',error instanceof ExecutionFailed?error.payload:{error:'handler_exception',reason:error?.message||String(error)});}
      });
    }
    async _finish(){
      if(this._finishing)return this._finishing;
      this.close();
      this._finishing=(async()=>{
        const deadline=this._drainUntil??(Date.now()+this._limits.shutdownGrace*1000);
        while((this._active.size)&&Date.now()<deadline)await sleep(5);
        for(const entry of this._active.values())entry.controller.abort();
        await Promise.allSettled([...this._active.keys()]);
        if(this._onClose){let timer;try{await Promise.race([Promise.resolve().then(this._onClose),new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('cleanup timeout')),Math.max(1,this._limits.shutdownGrace*1000));})]);}catch(error){this._diagnostic(`SDK cleanup failed: ${error}`);}finally{clearTimeout(timer);}}
        const flushDeadline=Date.now()+Math.max(50,this._limits.shutdownGrace*1000);
        while((this._blocked||this._outbox.length||this._ipcOutput?.writableLength>0)&&!this._writerError&&Date.now()<flushDeadline)await sleep(5);
        this._ipcOutput?.removeListener('drain',this._drain);this._ipcOutput?.removeListener('error',this._outputError);
        if(this._ipcBridge){
          try{this._ipcOutput?.end();}catch(_){}
          await Promise.race([this._ipcBridgeExit,sleep(Math.max(50,this._limits.shutdownGrace*1000))]);
        }
        this._ipcInput?.pause();this._ipcInput?.destroy();this._ipcOutput?.destroy();this._state='closed';this._startupError?this._rejectRun(this._startupError):this._resolveRun();
      })().catch(error=>{this._state='closed';this._rejectRun(error);});return this._finishing;
    }
  }
  Object.assign(RuntimeDomain.prototype,require('./messages')(RequestError));
  return {AtelierDomain:RuntimeDomain};
}
module.exports={enhance};
