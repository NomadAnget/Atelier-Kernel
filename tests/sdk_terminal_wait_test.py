"""SDK terminal waits do not time out execution; cancellation/disconnect still settle."""
import asyncio
from pathlib import Path
import sys
import threading
from types import SimpleNamespace
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'sdk/python'))
from atelier_sdk._messages import MessageClient
from atelier_sdk._errors import DomainRequestError

class Client(MessageClient):
    def __init__(self):
        self._closed=False;self._async_loop=asyncio.get_running_loop();self._pending={};self._results={}
        self._limits={'max_pending':10};self._write_lock=threading.Lock();self._seq_counter=1
        self.sent=[]
    def _submit(self,targets,kind,params,data,seq):
        self.sent.append((seq,targets[0]))
        self._receive_result(dict(message_id='test-message',seq=seq,target=targets[0],status='sent'))
    def receipt(self,status,data=None):
        seq,target=self.sent[-1]
        self._receive_result(dict(message_id='test-message',seq=seq,target=target,status=status,data=data))

class TerminalWaitTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_without_execution_deadline_keeps_lifecycle_receipts(self):
        client=Client();task=asyncio.create_task(client.request('worker.run','Ephemeral',timeout=None))
        await asyncio.sleep(.01);client.receipt('received');client.receipt('started')
        await asyncio.sleep(.04);self.assertFalse(task.done())
        client.receipt('completed',{'ok':True})
        self.assertEqual((await task)['data'],{'ok':True});self.assertFalse(client._results)
        self.assertEqual(len(client.sent),1)
    async def test_explicit_finite_timeout_still_works(self):
        client=Client()
        with self.assertRaises(DomainRequestError) as error:
            await client.request('worker.run','Ephemeral',timeout=.01)
        self.assertEqual(error.exception.status,'timeout');self.assertFalse(client._results)
    async def test_cancellation_forgets_local_wait_only(self):
        client=Client();task=asyncio.create_task(client.request('worker.run','Ephemeral',timeout=None))
        await asyncio.sleep(.01);task.cancel()
        with self.assertRaises(asyncio.CancelledError):await task
        self.assertFalse(client._results);self.assertFalse(client._pending)
    async def test_disconnect_settles_unbounded_wait(self):
        client=Client();task=asyncio.create_task(client.request('worker.run','Ephemeral',timeout=None))
        await asyncio.sleep(.01);client._fail_message_waits()
        with self.assertRaises(DomainRequestError) as error:await task
        self.assertEqual(error.exception.status,'connection_closed')
    async def test_terminal_failure_settles_unbounded_wait(self):
        client=Client();task=asyncio.create_task(client.request('worker.run','Ephemeral',timeout=None))
        await asyncio.sleep(.01);client.receipt('execution_failed',{'reason':'worker exited'})
        with self.assertRaises(DomainRequestError) as error:await task
        self.assertEqual(error.exception.status,'execution_failed')

if __name__=='__main__':unittest.main()
