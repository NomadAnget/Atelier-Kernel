const test=require('node:test'),assert=require('node:assert/strict');
const methods=require('../sdk/nodejs/messages.js');
class RequestError extends Error{constructor(status,data=null,messageId=null){super(status);this.status=status;this.data=data;this.message_id=messageId;}}
class Client{
 constructor(){this._closed=false;this._state='open';this._seqCounter=1;this._pending=new Map();this._results=new Map();this._limits={maxPending:10};this.sent=[];}
 _currentSignal(){return undefined;}
 _submit(targets,type,params,data,seq){this.sent.push({seq,target:targets[0]});this.receipt('sent');}
 receipt(status,data=null){this._receiveResult({...this.sent.at(-1),message_id:'test-message',status,data});}
}
Object.assign(Client.prototype,methods(RequestError));
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
test('unbounded request receives stages then terminal without resending',async()=>{const c=new Client();let done=false;const p=c.request('worker.run','Ephemeral',{}, {},{timeout:null}).finally(()=>{done=true;});c.receipt('received');c.receipt('started');await pause(40);assert.equal(done,false);c.receipt('completed',{ok:true});assert.deepEqual((await p).data,{ok:true});assert.equal(c.sent.length,1);assert.equal(c._results.size,0);});
test('explicit finite deadline remains supported',async()=>{const c=new Client();await assert.rejects(c.request('worker.run','Ephemeral',{}, {},{timeout:.01}),e=>e.status==='timeout');assert.equal(c._results.size,0);});
test('AbortSignal settles and releases unbounded waiter',async()=>{const c=new Client(),abort=new AbortController();const p=c.request('worker.run','Ephemeral',{}, {},{timeout:null,signal:abort.signal});await pause(5);abort.abort();await assert.rejects(p,e=>e.status==='cancelled');assert.equal(c._results.size,0);});
test('disconnect settles unbounded waiter',async()=>{const c=new Client();const p=c.request('worker.run','Ephemeral',{}, {},{timeout:null});await pause(5);c._failMessageWaits();await assert.rejects(p,e=>e.status==='connection_closed');assert.equal(c._results.size,0);});
test('worker failure settles unbounded waiter',async()=>{const c=new Client();const p=c.request('worker.run','Ephemeral',{}, {},{timeout:null});await pause(5);c.receipt('execution_failed',{reason:'exit'});await assert.rejects(p,e=>e.status==='execution_failed');assert.equal(c._results.size,0);});
