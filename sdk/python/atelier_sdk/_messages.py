"""Consumable receipt waits; timeout=None waits until terminal/cancel/disconnect. No execution ledger."""
import asyncio
import math
import time
from ._errors import DomainRequestError

TERMINAL=('completed','execution_failed','target_invalid','contract_invalid')

class MessageClient:
    async def _await_receipt(self, future, timeout):
        # Python wait_for can return a ready result while external cancellation
        # races admission. Do not lose that cancellation at the send/wait boundary.
        task=asyncio.current_task()
        cancelling=getattr(task,'cancelling',lambda:0)
        before=cancelling()
        result=await asyncio.wait_for(asyncio.shield(future),timeout)
        if cancelling()>before:raise asyncio.CancelledError()
        return result

    async def send(self,targets,message_type,params=None,data=None,*,timeout=30):
        single=isinstance(targets,str)
        names=[targets] if single else targets
        if not isinstance(names,list) or not names or not all(isinstance(t,str) and t for t in names):raise ValueError('targets must be a name or non-empty name list')
        names=list(dict.fromkeys(names))
        loop=asyncio.get_running_loop()
        if self._closed or self._async_loop is not loop:raise DomainRequestError('connection_closed')
        if not isinstance(timeout,(int,float)) or not math.isfinite(timeout) or timeout<=0:raise ValueError('invalid timeout')
        reserved=sum(len(e['targets'])-len(e['ids']) for e in self._pending.values())
        if len(self._results)+reserved+len(names)>self._limits['max_pending']:raise DomainRequestError('busy')
        with self._write_lock:seq=self._seq_counter;self._seq_counter+=1
        future=loop.create_future();entry={'targets':names,'ids':{},'future':future};self._pending[seq]=entry
        accepted=False
        try:
            try:self._submit(names,message_type,params,data,seq=seq)
            except DomainRequestError:raise
            except Exception as error:raise DomainRequestError('connection_closed',{'reason':str(error)}) from error
            try:ids=await self._await_receipt(future,timeout)
            except asyncio.TimeoutError as error:raise DomainRequestError('timeout') from error
            accepted=True
            return ids[0] if single else ids
        finally:
            self._pending.pop(seq,None)
            if not future.done():future.cancel()
            if not accepted:
                for mid in entry['ids'].values():self._results.pop(mid,None)

    def _receive_result(self,frame):
        mid=frame.get('message_id');target=frame.get('target');status=frame.get('status')
        if not isinstance(mid,str) or not mid or status not in ('sent','received','started',*TERMINAL):return
        entry=self._pending.get(frame.get('seq'))
        if entry and target not in entry['targets']:
            if target=='kernel.contract' and status=='contract_invalid' and not entry['future'].done():entry['future'].set_exception(DomainRequestError(status,frame.get('data'),mid))
            return
        if entry and target not in entry['ids']:
            if mid in self._results:raise ValueError('kernel reused a child message ID')
            entry['ids'][target]=mid
            self._results[mid]={'target':target,'future':asyncio.get_running_loop().create_future(),'waiters':0}
            if len(entry['ids'])==len(entry['targets']) and not entry['future'].done():entry['future'].set_result([entry['ids'][t] for t in entry['targets']])
        result=self._results.get(mid)
        if result and result['target']==target and not result['future'].done() and status in TERMINAL:result['future'].set_result(frame)

    async def wait_result(self,message_id,timeout=30):
        if timeout is not None and (not isinstance(timeout,(int,float)) or not math.isfinite(timeout) or timeout<=0):raise ValueError('invalid timeout')
        entry=self._results.get(message_id)
        if entry is None:raise DomainRequestError('unknown_message',message_id=message_id)
        entry['waiters']+=1
        observed=False
        try:
            await asyncio.sleep(0)
            try:frame=await self._await_receipt(entry['future'],timeout)
            except asyncio.TimeoutError as error:raise DomainRequestError('timeout',message_id=message_id) from error
            observed=True
            if frame['status']!='completed':raise DomainRequestError(frame['status'],frame.get('data'),message_id)
            return frame
        finally:
            entry['waiters']-=1
            if observed and not entry['waiters']:self._results.pop(message_id,None)

    async def wait_results(self,message_ids,timeout=30):
        if not isinstance(message_ids,list) or not message_ids or not all(isinstance(i,str) and i for i in message_ids) or len(set(message_ids))!=len(message_ids):raise ValueError('expected distinct message IDs')
        async def one(mid):
            target=self._results.get(mid,{}).get('target')
            try:return dict(message_id=mid,target=target,status='completed',receipt=await self.wait_result(mid,timeout),error=None)
            except DomainRequestError as error:return dict(message_id=mid,target=target,status=error.status,receipt=None,error=dict(status=error.status,data=error.data,message_id=mid))
        return await asyncio.gather(*(one(mid) for mid in message_ids))

    def forget_result(self,message_id):
        entry=self._results.get(message_id)
        if entry and entry['waiters']:raise DomainRequestError('busy',message_id=message_id)
        return self._results.pop(message_id,None) is not None

    async def request(self,target,message_type,params=None,data=None,timeout=30):
        if not isinstance(target,str):raise ValueError('request requires one target')
        start=time.monotonic();mid=None
        try:
            mid=await self.send(target,message_type,params,data,timeout=30 if timeout is None else timeout)
            remaining=None if timeout is None else timeout-(time.monotonic()-start)
            if remaining is not None and remaining<=0:raise DomainRequestError('timeout',message_id=mid)
            return await self.wait_result(mid,remaining)
        finally:
            if mid is not None:self.forget_result(mid)

    async def request_many(self,message_ids,timeout=30):
        return await self.wait_results(message_ids,timeout)

    def _fail_message_waits(self):
        for entry in self._pending.values():
            if not entry['future'].done():entry['future'].set_exception(DomainRequestError('connection_closed'))
        self._pending.clear()
        for mid,entry in self._results.items():
            if not entry['future'].done():entry['future'].set_result(dict(status='connection_closed',message_id=mid,data=None))
