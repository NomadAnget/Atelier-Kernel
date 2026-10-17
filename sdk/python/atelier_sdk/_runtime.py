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
_CLAIMED_IPC_FDS=None
_CLAIM_LOCK=threading.Lock()

def _claim_ipc_fds():
    global _CLAIMED_IPC_FDS
    with _CLAIM_LOCK:
        if _CLAIMED_IPC_FDS is not None:return _CLAIMED_IPC_FDS
        r_raw=os.environ.pop('ATELIER_IPC_READ_FD',None)
        w_raw=os.environ.pop('ATELIER_IPC_WRITE_FD',None)
        mode=os.environ.pop('ATELIER_IPC_HANDLE_MODE',None)
        if r_raw is None or w_raw is None:return None
        try:r_val,w_val=int(r_raw),int(w_raw)
        except ValueError as exc:raise RuntimeError('invalid ATELIER_IPC_READ_FD / ATELIER_IPC_WRITE_FD') from exc
        if os.name=='nt' and mode in ('win32','win32_handle'):
            import msvcrt
            r_fd=msvcrt.open_osfhandle(r_val,os.O_RDONLY|getattr(os,'O_BINARY',0))
            w_fd=msvcrt.open_osfhandle(w_val,os.O_WRONLY|getattr(os,'O_BINARY',0))
        else:r_fd,w_fd=r_val,w_val
        try:
            os.set_inheritable(r_fd,False)
            os.set_inheritable(w_fd,False)
        except OSError:pass
        _CLAIMED_IPC_FDS=(r_fd,w_fd)
        return _CLAIMED_IPC_FDS

_LEVEL_WEIGHTS = {
    "DEBUG": 10,
    "INFO": 20,
    "WARN": 30,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50,
    "FATAL": 50,
}

def _level_matches(rec_level: str, filter_level: str) -> bool:
    if not filter_level or filter_level.upper() == "ALL":
        return True
    w_min = _LEVEL_WEIGHTS.get(filter_level.upper(), 0)
    w_rec = _LEVEL_WEIGHTS.get(rec_level.upper(), 0)
    if w_min > 0 and w_rec > 0:
        return w_rec >= w_min
    return rec_level.upper() == filter_level.upper()

def _domain_matches(record, expected_domain: str) -> bool:
    if not expected_domain:
        return True
    exp = expected_domain.lower()
    src = str(record.get("source_domain") or "").lower()
    comp = str(record.get("component") or "").lower()
    return src == exp or comp == exp


class AsyncRuntime(MessageClient):
    def _require_ipc_fds(self):
        fds=_claim_ipc_fds()
        if fds is None:
            raise RuntimeError('missing ATELIER_IPC_READ_FD / ATELIER_IPC_WRITE_FD: dedicated dual anonymous pipes are required')
        return fds

    def _write_ipc_bytes(self,w_fd,data_bytes):
        pending=memoryview(data_bytes)
        while pending:
            count=os.write(w_fd,pending)
            if count<=0:raise BrokenPipeError('IPC write pipe closed')
            pending=pending[count:]

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
        doc=inspect.getdoc(fn) or ''
        if not doc:
            try:comments=inspect.getcomments(fn) or ''
            except Exception:comments=''
            if comments:
                lines=[line.lstrip().lstrip('#').strip() for line in comments.splitlines() if line.lstrip().startswith('#')]
                doc='\n'.join(line for line in lines if line).strip()
        if not hasattr(self,'_handler_docs'):self._handler_docs={}
        self._handler_docs[name]=doc
        return fn

    async def query_handlers_async(self,domain=None,target=None,handler=None,include_inactive=False,timeout=30):
        params={k:v for k,v in dict(domain=domain,target=target,handler=handler).items() if v is not None}
        if include_inactive:params['include_inactive']=True
        return (await self.request('kernel.registry','QueryHandlers',params,{},timeout))['data']
    async def get_handler_description_async(self,target,timeout=30):
        res=await self.query_handlers_async(target=target,timeout=timeout)
        return res.get('description')

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

    async def query_logs_async(self,domain=None,domain_id=None,source_domain=None,level=None,layer=None,component=None,phase=None,since_ts=None,limit=200,timeout=30):
        params={'limit':limit}
        d=domain if domain is not None else (domain_id if domain_id is not None else source_domain)
        if d is not None:params['domain_id']=d
        if level is not None:params['level']=level
        if layer is not None:params['layer']=layer
        if component is not None:params['component']=component
        if phase is not None:params['phase']=phase
        if since_ts is not None:params['since_ts']=since_ts
        return (await self.request('kernel.logger','QueryLogs',params,{},timeout))['data']

    async def query_progress_async(self,timeout=30):
        return (await self.request('kernel.logger','QueryProgress',{},{},timeout))['data']

    async def subscribe_logs_async(self, level=None, source_domain=None, timeout=30):
        params = {}
        if level is not None: params['level'] = level
        if source_domain is not None: params['source_domain'] = source_domain
        return (await self.request('kernel.logger', 'SubscribeLogs', params, {}, timeout))['data']

    async def unsubscribe_logs_async(self, timeout=30):
        return (await self.request('kernel.logger', 'UnsubscribeLogs', {}, {}, timeout))['data']

    async def subscribe_telemetry_async(self, timeout=30):
        return (await self.request('kernel.telemetry', 'SubscribeTelemetry', {}, {}, timeout))['data']

    async def unsubscribe_telemetry_async(self, timeout=30):
        return (await self.request('kernel.telemetry', 'UnsubscribeTelemetry', {}, {}, timeout))['data']

    async def subscribe_notify_async(self, timeout=30):
        return (await self.request('kernel.notify', 'SubscribeNotify', {}, {}, timeout))['data']

    async def unsubscribe_notify_async(self, timeout=30):
        return (await self.request('kernel.notify', 'UnsubscribeNotify', {}, {}, timeout))['data']

    async def query_runtime_async(self, domain=None, timeout=30):
        params = {}
        if domain is not None:
            params['domain'] = domain
        return (await self.request('kernel.runtime', 'QueryRuntime', params, {}, timeout))['data']

    async def query_observer_async(self, limit=100, message_id=None, target=None, source_domain=None, timeout=30):
        params = {'limit': limit}
        if message_id is not None: params['message_id'] = message_id
        if target is not None: params['target'] = target
        if source_domain is not None: params['source_domain'] = source_domain
        return (await self.request('kernel.observer', 'QueryObserver', params, {}, timeout))['data']

    async def subscribe_observer_async(self, timeout=30):
        return (await self.request('kernel.observer', 'SubscribeObserver', {}, {}, timeout))['data']

    async def unsubscribe_observer_async(self, timeout=30):
        return (await self.request('kernel.observer', 'UnsubscribeObserver', {}, {}, timeout))['data']

    async def supervisor_start_async(self, domain, spec=None, timeout=30):
        params = {'domain': domain}
        if spec is not None: params['spec'] = spec
        return (await self.request('kernel.supervisor', 'StartDomain', params, {}, timeout))['data']

    async def supervisor_stop_async(self, domain, timeout=30):
        params = {'domain': domain}
        return (await self.request('kernel.supervisor', 'StopDomain', params, {}, timeout))['data']

    async def supervisor_restart_async(self, domain, timeout=30):
        params = {'domain': domain}
        return (await self.request('kernel.supervisor', 'RestartDomain', params, {}, timeout))['data']

    async def supervisor_update_async(self, domain, spec=None, probe_handler=None, timeout_ms=5000, timeout=30):
        params = {'domain': domain, 'timeout_ms': timeout_ms}
        if spec is not None: params['spec'] = spec
        if probe_handler is not None: params['probe_handler'] = probe_handler
        return (await self.request('kernel.supervisor', 'UpdateDomain', params, {}, timeout))['data']

    def _write_line(self,line):
        if self._state=='closed':raise DomainRequestError('connection_closed')
        encoded=line.encode('utf-8')
        if len(encoded)>self._limits['max_frame_bytes']:raise DomainRequestError('frame_too_large')
        _,w_fd=self._require_ipc_fds()
        if self._outbox is None:
            self._write_ipc_bytes(w_fd,encoded);return
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
            self.report_receipt(msg.message_id,msg.target,'received' if False else 'started')
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
        target = frame.get('target', '')
        if isinstance(target, str) and target.startswith('kernel.') and not frame.get('source_domain'):
            frame['source_domain'] = 'kernel'
        if not all(isinstance(frame.get(k),str) and frame[k] for k in ('message_id','target','source_domain')):raise ValueError('malformed delivery')
        if self._state=='draining':self._reply_error(frame,'shutting_down','domain is shutting down');return

        target = frame.get('target', '')
        msg_type = frame.get('message_type', '')
        if target == 'kernel.logger' or msg_type in ('LogEvent', 'LogRecord'):
            self._dispatch_system_log(frame)
            return
        if target == 'kernel.telemetry' or msg_type in ('TelemetryEvent',):
            self._dispatch_system_telemetry(frame)
            return
        if target == 'kernel.notify' or msg_type in ('NotifyEvent',):
            self._dispatch_system_notify(frame)
            return
        if target == 'kernel.observer' or msg_type in ('ObservationEvent',):
            self._dispatch_system_observer(frame)
            return

        self.report_receipt(frame['message_id'],frame['target'],'received')
        self._dispatch(frame)

    def _dispatch_system_log(self, frame):
        record = frame.get('data') or {}
        for listener, lvl, src_dom in getattr(self, '_log_listeners', []):
            if lvl and not _level_matches(record.get('level', ''), lvl):
                continue
            if src_dom and not _domain_matches(record, src_dom):
                continue
            self._invoke_event_listener(listener, record)

    def _dispatch_system_telemetry(self, frame):
        metric = frame.get('data') or {}
        for listener in getattr(self, '_telemetry_listeners', []):
            self._invoke_event_listener(listener, metric)

    def _dispatch_system_notify(self, frame):
        alert = frame.get('data') or {}
        for listener in getattr(self, '_notify_listeners', []):
            self._invoke_event_listener(listener, alert)

    def _dispatch_system_observer(self, frame):
        event = frame.get('data') or {}
        for listener in getattr(self, '_observation_listeners', []):
            self._invoke_event_listener(listener, event)

    def _invoke_event_listener(self, listener, payload):
        try:
            res = listener(payload)
            if inspect.isawaitable(res):
                task = asyncio.create_task(res)
                self._callbacks.add(task)
                task.add_done_callback(self._callbacks.discard)
        except Exception as e:
            self._diagnostic("SDK system event listener error: %s", e)

    async def run_async(self,on_start=None,on_close=None):
        if self._state!='new':raise RuntimeError('domain receive session already started or closed')
        r_fd,w_fd=self._require_ipc_fds()
        ipc_reader=self._ipc_reader=os.fdopen(r_fd,'r',encoding='utf-8',buffering=1,closefd=False)
        loop=self._async_loop=asyncio.get_running_loop();self._state='open';self._session_done=loop.create_future()
        inbox=self._inbox=asyncio.Queue(maxsize=self._limits['max_pending'])
        self._outbox=queue.Queue(maxsize=self._limits['max_outbox'])
        def writer():
            while not self._writer_stop.is_set() or not self._outbox.empty():
                try:line=self._outbox.get(timeout=.05)
                except queue.Empty:continue
                try:
                    self._write_ipc_bytes(w_fd,line.encode('utf-8'))
                except Exception:
                    self._writer_error=True;self.close();return
                finally:self._outbox.task_done()
        def reader():
            try:
                while not self._closed:
                    line=ipc_reader.readline(self._limits['max_frame_bytes']+1)
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
        threading.Thread(target=writer,name='atelier-sdk-ipc-tx',daemon=True).start()
        if self._handlers:self._submit(['kernel.registry'],'RegisterHandlers',{'handlers':list(self._handlers),'descriptions':dict(getattr(self,'_handler_docs',{}))},{})
        if getattr(self, '_log_listeners', None):
            for _, lvl, src_dom in self._log_listeners:
                p = {}
                if lvl: p['level'] = lvl
                if src_dom: p['source_domain'] = src_dom
                self._submit(['kernel.logger'], 'SubscribeLogs', params=p, data={})
        if getattr(self, '_telemetry_listeners', None):
            self._submit(['kernel.telemetry'], 'SubscribeTelemetry', params={}, data={})
        if getattr(self, '_notify_listeners', None):
            self._submit(['kernel.notify'], 'SubscribeNotify', params={}, data={})
        if getattr(self, '_observation_listeners', None):
            self._submit(['kernel.observer'], 'SubscribeObserver', params={}, data={})
        self._startup=asyncio.create_task(start())
        threading.Thread(target=reader,name='atelier-sdk-ipc-rx',daemon=True).start()
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
