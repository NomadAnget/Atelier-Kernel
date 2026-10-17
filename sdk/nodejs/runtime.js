const readline=require('readline');
const {AsyncLocalStorage}=require('async_hooks');
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const context=new AsyncLocalStorage();

function enhance(Base,RequestError,ExecutionFailed) {
  class RuntimeDomain extends Base {
    _diagnostic(message){try{this.logger.error(message);}catch(_){process.stderr.write(message+'\n');}}
    constructor(options={}) {
      super(options);
      const defaults={maxPending:1024,maxOutbox:256,maxFrameBytes:16777216,shutdownGrace:5};
      for(const key of Object.keys(options))if(!['domain','stageDelayMs',...Object.keys(defaults)].includes(key))throw new Error('unknown SDK option: '+key);
      this._limits={...defaults};
      for(const key of Object.keys(defaults))if(options[key]!==undefined)this._limits[key]=options[key];
      for(const [key,value] of Object.entries(this._limits))if(typeof value!=='number'||!Number.isFinite(value)||value<(key==='shutdownGrace'?0:1)||(key!=='shutdownGrace'&&!Number.isInteger(value)))throw new Error('invalid SDK option: '+key);
      this._state='new';this._closed=false;this._active=new Map();this._results=new Map();this._pending=new Map();
      this._outbox=[];this._blocked=false;this._rl=null;this._finishing=null;this._writerError=null;this._drainUntil=null;this._callbacks=new Set();
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
      if(this._blocked){if(this._outbox.length>=this._limits.maxOutbox)throw new RequestError('busy');this._outbox.push(line);}
      else this._blocked=!process.stdout.write(line);
    }
    _flush(){while(!this._blocked&&this._outbox.length)this._blocked=!process.stdout.write(this._outbox.shift());}
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
      this._state='open';this._onClose=onClose;
      this._runPromise=new Promise((resolve,reject)=>{this._resolveRun=resolve;this._rejectRun=reject;});this._runPromise.catch(()=>{});
      this._drain=()=>{this._blocked=false;this._flush();};this._outputError=error=>{this._writerError=error;this.close();};
      process.stdout.on('drain',this._drain);process.stdout.on('error',this._outputError);
      this._rl=readline.createInterface({input:process.stdin,terminal:false});
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
        while((this._blocked||this._outbox.length)&&!this._writerError&&Date.now()<flushDeadline)await sleep(5);
        process.stdout.removeListener('drain',this._drain);process.stdout.removeListener('error',this._outputError);
        process.stdin.pause();process.stdin.unref?.();this._state='closed';this._startupError?this._rejectRun(this._startupError):this._resolveRun();
      })().catch(error=>{this._state='closed';this._rejectRun(error);});return this._finishing;
    }
  }
  Object.assign(RuntimeDomain.prototype,require('./messages')(RequestError));
  return {AtelierDomain:RuntimeDomain};
}
module.exports={enhance};
