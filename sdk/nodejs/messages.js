// Client-side receipt waits only. IDs and execution state are authoritative in the kernel.
module.exports=function messageMethods(RequestError){
 const terminal=['completed','execution_failed','target_invalid','contract_invalid'];
 return {
  async send(targets,type,params={},data={},options={}){
   const single=typeof targets==='string',names=single?[targets]:targets;
   if(!Array.isArray(names)||!names.length||names.some(t=>typeof t!=='string'||!t))throw Error('targets must be a name or non-empty name list');
   const unique=[...new Set(names)],{timeout=30,signal=this._currentSignal()}=options;
   if(this._closed||this._state==='new')throw new RequestError('connection_closed');
   if(!Number.isFinite(timeout)||timeout<=0)throw Error('invalid timeout');
   if(signal?.aborted)throw new RequestError('cancelled');
   const reserved=[...this._pending.values()].reduce((n,e)=>n+e.targets.length-e.ids.size,0);
   if(this._results.size+reserved+unique.length>this._limits.maxPending)throw new RequestError('busy');
   const seq=this._seqCounter++;let resolve,reject;
   const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});promise.catch(()=>{});
   const entry={targets:unique,ids:new Map(),resolve,reject};this._pending.set(seq,entry);let accepted=false;
   try{
    try{this._submit(unique,type,params,data,seq);}catch(e){throw e instanceof RequestError?e:new RequestError('connection_closed',{reason:String(e)});}
    const ids=await this._awaitMessage(promise,timeout,signal);accepted=true;return single?ids[0]:ids;
   }finally{this._pending.delete(seq);if(!accepted)for(const id of entry.ids.values())this._results.delete(id);}
  },
  _awaitMessage(promise,timeout,signal,messageId=null){
   if(!Number.isFinite(timeout)||timeout<=0)return Promise.reject(Error('invalid timeout'));
   return new Promise((resolve,reject)=>{
    let timer;const abort=()=>finish(new RequestError('cancelled',null,messageId));
    const finish=(error,result)=>{clearTimeout(timer);signal?.removeEventListener('abort',abort);error?reject(error):resolve(result);};
    timer=setTimeout(()=>finish(new RequestError('timeout',null,messageId)),timeout*1000);
    signal?.addEventListener('abort',abort,{once:true});if(signal?.aborted)abort();
    promise.then(value=>finish(null,value),error=>finish(error));
   });
  },
  _receiveResult(frame){
   const {message_id:mid,target,status}=frame;
   if(typeof mid!=='string'||!mid||!['sent','received','started',...terminal].includes(status))return;
   const entry=this._pending.get(frame.seq);
   if(entry&&!entry.targets.includes(target)){if(target==='kernel.contract'&&status==='contract_invalid')entry.reject(new RequestError(status,frame.data,mid));return;}
   if(entry&&!entry.ids.has(target)){
    if(this._results.has(mid))throw Error('kernel reused a child message ID');
    let resolve;const promise=new Promise(yes=>{resolve=yes;});
    entry.ids.set(target,mid);this._results.set(mid,{target,promise,resolve,done:false,waiters:0});
    if(entry.ids.size===entry.targets.length)entry.resolve(entry.targets.map(t=>entry.ids.get(t)));
   }
   const result=this._results.get(mid);
   if(result&&result.target===target&&!result.done&&terminal.includes(status)){result.done=true;result.resolve(frame);}
  },
  async waitResult(messageId,{timeout=30,signal=this._currentSignal()}={}){
   if(!Number.isFinite(timeout)||timeout<=0)throw Error('invalid timeout');
   const entry=this._results.get(messageId);if(!entry)throw new RequestError('unknown_message',null,messageId);
   entry.waiters++;let observed=false;
   try{const frame=await this._awaitMessage(entry.promise,timeout,signal,messageId);observed=true;if(frame.status!=='completed')throw new RequestError(frame.status,frame.data,messageId);return frame;}
   finally{entry.waiters--;if(observed&&!entry.waiters)this._results.delete(messageId);}
  },
  async waitResults(messageIds,options={}){
   if(!Array.isArray(messageIds)||!messageIds.length||messageIds.some(id=>typeof id!=='string'||!id)||new Set(messageIds).size!==messageIds.length)throw Error('expected distinct message IDs');
   return Promise.all(messageIds.map(async mid=>{const target=this._results.get(mid)?.target??null;try{return {message_id:mid,target,status:'completed',receipt:await this.waitResult(mid,options),error:null};}catch(e){if(!(e instanceof RequestError))throw e;return {message_id:mid,target,status:e.status,receipt:null,error:{status:e.status,data:e.data,message_id:mid}};}}));
  },
  forgetResult(mid){const entry=this._results.get(mid);if(entry?.waiters)throw new RequestError('busy',null,mid);return this._results.delete(mid);},
  requestMany(messageIds,options={}){return this.waitResults(messageIds,options);},
  async request(target,type,params={},data={},options={}){
   if(typeof target!=='string')throw Error('request requires one target');const start=Date.now(),timeout=options.timeout??30;let mid;
   try{mid=await this.send(target,type,params,data,{...options,timeout});const remaining=timeout-(Date.now()-start)/1000;if(remaining<=0)throw new RequestError('timeout',null,mid);return await this.waitResult(mid,{...options,timeout:remaining});}
   finally{if(mid!==undefined)this.forgetResult(mid);}
  },
  _failMessageWaits(){for(const e of this._pending.values())e.reject(new RequestError('connection_closed'));this._pending.clear();for(const [mid,e] of this._results)if(!e.done){e.done=true;e.resolve({status:'connection_closed',message_id:mid,data:null});}}
 };
};
