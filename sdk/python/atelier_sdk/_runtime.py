import asyncio
from concurrent.futures import TimeoutError as ThreadTimeout
from contextvars import ContextVar
import inspect
import json
import math
import os
import queue
import sys
import threading
import time
from ._messages import MessageClient
from ._errors import DomainExecutionFailed,DomainRequestError

_context=ContextVar('atelier_request',default=None)

class AsyncRuntime(MessageClient):
    def _diagnostic(self,message,*args):
        try:self.logger.error(message,*args)
        except Exception:sys.stderr.write((message % args if args else message)+'\n')
    def _configure(self,**options):
        defaults=dict(max_pending=1024,max_outbox=256,max_frame_bytes=16777216,shutdown_grace=5)
        for key,value in options.items():
            if key not in defaults:raise ValueError('unknown SDK option: '+key)
            defaults[key]=value
        for key,value in defaults.items():
            if type(value) not in (int,float) or not math.isfinite(value) or value<(0 if key=='shutdown_grace' else 1) or (key!='shutdown_grace' and int(value)!=value):
                raise ValueError('invalid SDK option: '+key)
        self._limits={k:(v if k=='shutdown_grace' else int(v)) for k,v in defaults.items()};self._state='new';self._active={}
        self._results={};self._pending={};self._closed=False;self._async_loop=None;self._inbox=None;self._startup=None;self._session_done=None
        self._outbox=None;self._writer_stop=threading.Event();self._writer_error=False;self._shutdown_task=None;self._drain_deadline=None;self._callbacks=set();self._startup_error=None

    @property
    def context(self):return dict(_context.get() or {})

    def _register_handler(self,name,fn):
        if not isinstance(name,str) or not name or '.' in name:raise ValueError('invalid handler name')
        self._handlers[name]=fn
        return fn

    async def storage_insert_async(self,record_id,category,data,upsert=True,timeout=30):
        return (await self.request('kernel.storage','StorageInsert',dict(record_id=record_id,category=category,upsert=upsert),data,timeout))['data']
    async def storage_update_async(self,record_id,data,patch=True,timeout=30):
        return (await self.request('kernel.storage','StorageUpdate',dict(record_id=record_id,patch=patch),data,timeout))['data']
    async def storage_delete_async(self,record_id=None,category=None,timeout=30):
        params={k:v for k,v in dict(record_id=record_id,category=category).items() if v is not None}
        return (await self.request('kernel.storage','StorageDelete',params,{},timeout))['data']
    async def storage_query_async(self,domain=None,record_id=None,category=None,filter=None,filter_map=None,limit=500,timeout=30):
        params={k:v for k,v in dict(domain=domain,record_id=record_id,category=category,filter=filter if filter is not None else filter_map,limit=limit).items() if v is not None}
        return (await self.request('kernel.storage','StorageQuery',params,{},timeout))['data']

    def _write_line(self,line):
        if self._state=='closed':raise DomainRequestError('connection_closed')
        if len(line.encode('utf-8'))>self._limits['max_frame_bytes']:raise DomainRequestError('frame_too_large')
        if self._outbox is None:
            sys.stdout.write(line);sys.stdout.flush();return
        try:self._outbox.put_nowait(line)
        except queue.Full as error:raise DomainRequestError('busy') from error

    def _fail_pending(self):self._fail_message_waits()

    def close(self):
        if self._closed:return
        self._closed=True;self._state='draining'
        if self._async_loop is None:self._state='closed';return
        def finish():
            self._fail_pending()
            if self._inbox is not None:
                try:self._inbox.put_nowait(None)
                except asyncio.QueueFull:pass
        if self._async_loop is not None and not self._async_loop.is_closed():self._async_loop.call_soon_threadsafe(finish)

    async def shutdown(self,grace=None):
        grace=self._limits['shutdown_grace'] if grace is None else grace
        if not math.isfinite(grace) or grace<0:raise ValueError('invalid shutdown grace')
        if self._state=='closed':return
        self._state='draining'
        deadline=time.monotonic()+grace
        current=asyncio.current_task()
        while self._pending or any(task is not current for task in self._active):
            if time.monotonic()>=deadline:break
            await asyncio.sleep(.005)
        self._drain_deadline=time.monotonic()
        self.close()
        if self._session_done is not None:await asyncio.shield(self._session_done)

    def run(self):return asyncio.run(self.run_async())

    def _reply_error(self,frame,code,reason):
        self.report_receipt(frame['message_id'],frame['target'],'execution_failed',dict(code=code,error=code,reason=reason))

    def _dispatch(self,frame):
        task=asyncio.create_task(self._execute(frame))
        self._active[task]=frame
        def done(item):
            self._active.pop(item,None)
            if not item.cancelled() and item.exception() is not None:self.close()
        task.add_done_callback(done)

    async def _execute(self,frame):
        from . import DeliveryMessage
        msg=DeliveryMessage(frame)
        token=_context.set(dict(logical_id=msg.logical_id,message_id=msg.message_id,source_domain=msg.source_domain,target=msg.target,message_type=msg.message_type))
        try:
            self.report_receipt(msg.message_id,msg.target,'started')
            if self._stage_delay_sec:await asyncio.sleep(self._stage_delay_sec)
            fn=self._handlers.get(msg.target.split('.',1)[-1])
            if fn is None:raise DomainExecutionFailed({'error':'Unknown handler: '+msg.target.split('.',1)[-1]})
            try:count=len(inspect.signature(fn).parameters)
            except (ValueError,TypeError):count=1
            result=fn(msg,self) if count>=2 else fn(msg)
            if inspect.isawaitable(result):result=await result
            self.report_receipt(msg.message_id,msg.target,'completed',result)
        except asyncio.CancelledError:
            self._reply_error(frame,'cancelled','domain is shutting down')
            raise
        except DomainExecutionFailed as error:self.report_receipt(msg.message_id,msg.target,'execution_failed',error.payload)
        except Exception as error:self.report_receipt(msg.message_id,msg.target,'execution_failed',dict(error='handler_exception',reason=str(error)))
        finally:_context.reset(token)

    async def _receive(self,frame):
        if frame.get('frame_type')=='receipt':
            self._receive_result(frame)
            for callback in self._receipt_callbacks:
                try:
                    result=callback(frame)
                    if inspect.isawaitable(result):
                        if len(self._callbacks)>=self._limits['max_pending']:
                            if inspect.iscoroutine(result):result.close()
                            self.close();raise RuntimeError('receipt callback capacity exceeded')
                        task=asyncio.create_task(result);self._callbacks.add(task)
                        def finished(item):
                            self._callbacks.discard(item)
                            if not item.cancelled() and item.exception():self._diagnostic('SDK receipt callback failed: %s',item.exception())
                        task.add_done_callback(finished)
                except Exception as error:self._diagnostic('SDK receipt callback failed: %s',error)
            return
        if frame.get('frame_type')!='deliver':return
        if not all(isinstance(frame.get(k),str) and frame[k] for k in ('message_id','target','source_domain')):raise ValueError('malformed delivery')
        if self._state=='draining':self._reply_error(frame,'shutting_down','domain is shutting down');return
        self.report_receipt(frame['message_id'],frame['target'],'received')
        self._dispatch(frame)

    async def run_async(self,on_start=None,on_close=None):
        if self._state!='new':raise RuntimeError('domain receive session already started or closed')
        loop=self._async_loop=asyncio.get_running_loop();self._state='open';self._session_done=loop.create_future()
        inbox=self._inbox=asyncio.Queue(maxsize=self._limits['max_pending'])
        self._outbox=queue.Queue(maxsize=self._limits['max_outbox'])
        def writer():
            while not self._writer_stop.is_set() or not self._outbox.empty():
                try:line=self._outbox.get(timeout=.05)
                except queue.Empty:continue
                try:
                    # Write raw UTF-8 without holding Python's buffered stdout lock
                    # while a slow pipe blocks the dedicated writer thread.
                    try:fd=sys.stdout.fileno()
                    except (AttributeError,OSError,ValueError):fd=None
                    if fd is None:sys.stdout.write(line);sys.stdout.flush()
                    else:
                        pending=memoryview(line.encode('utf-8'))
                        while pending:
                            count=os.write(fd,pending)
                            if count<=0:raise BrokenPipeError('stdout pipe closed')
                            pending=pending[count:]
                except Exception:
                    self._writer_error=True;self.close();return
                finally:self._outbox.task_done()
        def reader():
            try:
                while not self._closed:
                    line=sys.stdin.readline(self._limits['max_frame_bytes']+1)
                    if not line:break
                    if len(line.encode('utf-8'))>self._limits['max_frame_bytes']:
                        self._diagnostic('SDK frame limit exceeded');self.close();break
                    waiting=asyncio.run_coroutine_threadsafe(inbox.put(line),loop)
                    while not self._closed:
                        try:waiting.result(timeout=.1);break
                        except ThreadTimeout:continue
                    if self._closed:waiting.cancel()
            except (RuntimeError,OSError):self.close()
            finally:
                if not self._closed and not loop.is_closed():
                    try:asyncio.run_coroutine_threadsafe(inbox.put(None),loop).result()
                    except Exception:pass
        async def start():
            try:
                if on_start:
                    result=on_start()
                    if inspect.isawaitable(result):await result
            except Exception as error:
                self._startup_error=error;self._diagnostic('SDK startup failed: %s',error);self.close()
        threading.Thread(target=writer,name='atelier-sdk-stdout',daemon=True).start()
        if self._handlers:self._submit(['kernel.registry'],'RegisterHandlers',{'handlers':list(self._handlers)},{})
        self._startup=asyncio.create_task(start())
        threading.Thread(target=reader,name='atelier-sdk-stdin',daemon=True).start()
        try:
            while not self._closed:
                line=await inbox.get()
                if line is None:break
                try:
                    frame=json.loads(line,parse_constant=lambda v:(_ for _ in ()).throw(ValueError('invalid JSON number')))
                    if not isinstance(frame,dict):raise ValueError('non-object frame')
                except (ValueError,TypeError):self._diagnostic('SDK received malformed JSON frame');continue
                await self._receive(frame)
        finally:
            self.close();self._fail_pending()
            self._startup.cancel()
            await asyncio.gather(self._startup,return_exceptions=True)
            # Input has ended: accepted non-RPC work can still finish within the grace period.
            deadline=self._drain_deadline if self._drain_deadline is not None else time.monotonic()+self._limits['shutdown_grace']
            while self._active and time.monotonic()<deadline:await asyncio.sleep(.005)
            remaining=list(self._active)
            for task in remaining:task.cancel()
            if remaining:await asyncio.wait(remaining,timeout=max(.05,self._limits['shutdown_grace']))
            for task in list(self._callbacks):task.cancel()
            if self._callbacks:await asyncio.gather(*list(self._callbacks),return_exceptions=True)
            if on_close:
                try:
                    result=on_close()
                    if inspect.isawaitable(result):await asyncio.wait_for(result,max(.001,self._limits['shutdown_grace']))
                except Exception as error:self._diagnostic('SDK cleanup failed: %s',error)
            deadline=time.monotonic()+max(.05,self._limits['shutdown_grace'])
            while self._outbox.unfinished_tasks and not self._writer_error and time.monotonic()<deadline:await asyncio.sleep(.005)
            self._writer_stop.set();self._state='closed'
            if not self._session_done.done():self._session_done.set_result(None)
        if self._startup_error is not None:raise self._startup_error
