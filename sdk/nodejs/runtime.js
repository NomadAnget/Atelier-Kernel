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
  'w_fd=msvcrt.open_osfhandle(w_h,os.O_WRONLY|os.O_BINARY)',
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
  // Do not close r_fd while p_in owns a blocking CRT read on Windows: close
  // waits for that descriptor lock. Process exit reclaims it without joining p_in.
  '  try:os.close(w_fd)',
  '  except Exception:pass',
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

const _LEVEL_WEIGHTS = { DEBUG: 10, INFO: 20, WARN: 30, WARNING: 30, ERROR: 40, CRITICAL: 50, FATAL: 50 };
function _nodeLevelMatches(recLevel, filterLevel) {
  if (!filterLevel || String(filterLevel).toUpperCase() === 'ALL') return true;
  const wMin = _LEVEL_WEIGHTS[String(filterLevel).toUpperCase()] || 0;
  const wRec = _LEVEL_WEIGHTS[String(recLevel).toUpperCase()] || 0;
  if (wMin > 0 && wRec > 0) return wRec >= wMin;
  return String(recLevel).toUpperCase() === String(filterLevel).toUpperCase();
}
function _nodeDomainMatches(record, expectedDomain) {
  if (!expectedDomain) return true;
  const exp = String(expectedDomain).toLowerCase();
  const src = String(record?.source_domain || '').toLowerCase();
  const comp = String(record?.component || '').toLowerCase();
  return src === exp || comp === exp;
}

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
          const pyCmd=process.env.PYTHON||process.env.PYTHON_BIN||'python';
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
    async queryLogsAsync({domain,domainId,domain_id,source_domain,level,layer,component,phase,sinceTs,since_ts,limit=200}={},options={}){const params={limit};const d=domain??domainId??domain_id??source_domain;if(d!==undefined)params.domain_id=d;if(level!==undefined)params.level=level;if(layer!==undefined)params.layer=layer;if(component!==undefined)params.component=component;if(phase!==undefined)params.phase=phase;const ts=sinceTs??since_ts;if(ts!==undefined)params.since_ts=ts;return (await this.request('kernel.logger','QueryLogs',params,{},options)).data;}
    async queryProgressAsync(options={}){return (await this.request('kernel.logger','QueryProgress',{},{},options)).data;}
    async subscribeLogsAsync({level,sourceDomain,source_domain}={},options={}){const params={};if(level!==undefined)params.level=level;const d=sourceDomain??source_domain;if(d!==undefined)params.source_domain=d;return (await this.request('kernel.logger','SubscribeLogs',params,{},options)).data;}
    async unsubscribeLogsAsync(options={}){return (await this.request('kernel.logger','UnsubscribeLogs',{},{},options)).data;}
    async subscribeTelemetryAsync(options={}){return (await this.request('kernel.telemetry','SubscribeTelemetry',{},{},options)).data;}
    async unsubscribeTelemetryAsync(options={}){return (await this.request('kernel.telemetry','UnsubscribeTelemetry',{},{},options)).data;}
    async subscribeNotifyAsync(options={}){return (await this.request('kernel.notify','SubscribeNotify',{},{},options)).data;}
    async unsubscribeNotifyAsync(options={}){return (await this.request('kernel.notify','UnsubscribeNotify',{},{},options)).data;}
    async queryRuntimeAsync({domain,domainId,domain_id}={},options={}){const params={};const d=domain??domainId??domain_id;if(d!==undefined)params.domain=d;return (await this.request('kernel.runtime','QueryRuntime',params,{},options)).data;}
    async queryObserverAsync({limit=100,messageId,message_id,target,sourceDomain,source_domain}={},options={}){const params={limit};const mid=messageId??message_id;if(mid!==undefined)params.message_id=mid;if(target!==undefined)params.target=target;const sd=sourceDomain??source_domain;if(sd!==undefined)params.source_domain=sd;return (await this.request('kernel.observer','QueryObserver',params,{},options)).data;}
    async subscribeObserverAsync(options={}){return (await this.request('kernel.observer','SubscribeObserver',{},{},options)).data;}
    async unsubscribeObserverAsync(options={}){return (await this.request('kernel.observer','UnsubscribeObserver',{},{},options)).data;}
    async supervisorStartAsync({domain,spec}={},options={}){const params={domain};if(spec!==undefined)params.spec=spec;return (await this.request('kernel.supervisor','StartDomain',params,{},options)).data;}
    async supervisorStopAsync({domain}={},options={}){const params={domain};return (await this.request('kernel.supervisor','StopDomain',params,{},options)).data;}
    async supervisorRestartAsync({domain}={},options={}){const params={domain};return (await this.request('kernel.supervisor','RestartDomain',params,{},options)).data;}
    async supervisorUpdateAsync({domain,spec,probeHandler,probe_handler,timeoutMs,timeout_ms=5000}={},options={}){const params={domain,timeout_ms:timeoutMs??timeout_ms};if(spec!==undefined&&spec!==null)params.spec=spec;const ph=probeHandler??probe_handler;if(ph!==undefined)params.probe_handler=ph;return (await this.request('kernel.supervisor','UpdateDomain',params,{},options)).data;}
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
      if(this._logListeners&&this._logListeners.length){for(const {level,sourceDomain} of this._logListeners){const p={};if(level!==undefined)p.level=level;if(sourceDomain!==undefined)p.source_domain=sourceDomain;this._submit(['kernel.logger'],'SubscribeLogs',p,{});}}
      if(this._telemetryListeners&&this._telemetryListeners.length){this._submit(['kernel.telemetry'],'SubscribeTelemetry',{},{});}
      if(this._notifyListeners&&this._notifyListeners.length){this._submit(['kernel.notify'],'SubscribeNotify',{},{});}
      if(this._observationListeners&&this._observationListeners.length){this._submit(['kernel.observer'],'SubscribeObserver',{},{});}
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
      const target=frame.target||'';
      if(typeof target==='string'&&target.startsWith('kernel.')&&!frame.source_domain){
        frame.source_domain='kernel';
      }
      if(['message_id','target','source_domain'].some(k=>typeof frame[k]!=='string'||!frame[k]))throw new Error('malformed delivery');
      if(this._state==='draining'){this._replyError(frame,'shutting_down','domain is shutting down');return;}

      const msgType=frame.message_type||'';
      if(target==='kernel.logger'||msgType==='LogEvent'||msgType==='LogRecord'){
        this._dispatchSystemLog(frame);
        return;
      }
      if(target==='kernel.telemetry'||msgType==='TelemetryEvent'){
        this._dispatchSystemTelemetry(frame);
        return;
      }
      if(target==='kernel.notify'||msgType==='NotifyEvent'){
        this._dispatchSystemNotify(frame);
        return;
      }
      if(target==='kernel.observer'||msgType==='ObservationEvent'){
        this._dispatchSystemObserver(frame);
        return;
      }

      this.reportReceipt(frame.message_id,frame.target,'received');this._dispatch(frame);
    }
    _dispatchSystemLog(frame){
      const record=frame.data||{};
      for(const {callback,level,sourceDomain} of (this._logListeners||[])){
        if(level&&!_nodeLevelMatches(record.level,level))continue;
        if(sourceDomain&&!_nodeDomainMatches(record,sourceDomain))continue;
        this._invokeEventListener(callback,record);
      }
    }
    _dispatchSystemTelemetry(frame){
      const metric=frame.data||{};
      for(const callback of (this._telemetryListeners||[])){
        this._invokeEventListener(callback,metric);
      }
    }
    _dispatchSystemNotify(frame){
      const alert=frame.data||{};
      for(const callback of (this._notifyListeners||[])){
        this._invokeEventListener(callback,alert);
      }
    }
    _dispatchSystemObserver(frame){
      const event=frame.data||{};
      for(const callback of (this._observationListeners||[])){
        this._invokeEventListener(callback,event);
      }
    }
    _invokeEventListener(callback,payload){
      try{
        const res=callback(payload);
        if(res&&typeof res.then==='function'){
          res.catch(error=>this._diagnostic(`SDK system event listener error: ${error}`));
        }
      }catch(error){
        this._diagnostic(`SDK system event listener error: ${error}`);
      }
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
          let bridgeTimer;
          let bridgeExited=false;
          try{
            await Promise.race([this._ipcBridgeExit.then(()=>{bridgeExited=true;}),new Promise(resolve=>{
              bridgeTimer=setTimeout(resolve,Math.max(50,this._limits.shutdownGrace*1000));
            })]);
          }finally{clearTimeout(bridgeTimer);}
          if(!bridgeExited){
            this._ipcBridge.kill();
            await this._ipcBridgeExit;
          }
        }
        this._ipcInput?.pause();this._ipcInput?.destroy();this._ipcOutput?.destroy();this._state='closed';this._startupError?this._rejectRun(this._startupError):this._resolveRun();
      })().catch(error=>{this._state='closed';this._rejectRun(error);});return this._finishing;
    }
  }
  Object.assign(RuntimeDomain.prototype,require('./messages')(RequestError));
  return {AtelierDomain:RuntimeDomain};
}
module.exports={enhance};
