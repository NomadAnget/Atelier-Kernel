use std::collections::VecDeque;
use std::sync::mpsc::{self,Receiver,SyncSender,TrySendError};
use parking_lot::Mutex;
use serde_json::Value;
use crate::protocol::{ContractStatus,ObservationEvent,now_millis};
const MAX_EVENT:usize=262_144;
const MAX_HISTORY_BYTES:usize=16*1024*1024;
struct History {
    next_id:u64,events:VecDeque<(ObservationEvent,usize)>,bytes:usize,
    subscribers:Vec<SyncSender<ObservationEvent>>,
}
/// Best-effort observations own no delivery state. One bounded history, one nonblocking fanout.
pub struct KernelObserver {state:Mutex<History>,max_history:usize}
impl Default for KernelObserver{fn default()->Self{Self::new(10_000)}}
impl KernelObserver {
    pub fn new(max_history:usize)->Self{Self{state:Mutex::new(History{next_id:1,events:VecDeque::new(),bytes:0,subscribers:Vec::new()}),max_history:max_history.max(64)}}
    pub fn subscribe(&self)->Receiver<ObservationEvent>{let (tx,rx)=mpsc::sync_channel(self.max_history.min(64));self.state.lock().subscribers.push(tx);rx}
    #[allow(clippy::too_many_arguments)]
    pub fn emit(&self,source:&str,target:&str,message_id:&str,logical_id:&str,seq:u64,kind:&str,status:ContractStatus,details:Option<Value>)->ObservationEvent {
        let mut state=self.state.lock();
        let event=ObservationEvent{event_id:state.next_id,timestamp:now_millis(),source_domain:source.into(),target:target.into(),message_id:message_id.into(),logical_id:logical_id.into(),seq,message_type:kind.into(),status,details};
        state.next_id=state.next_id.saturating_add(1);
        // Count without materializing another potentially huge serialized business result.
        struct Counter(usize);
        impl std::io::Write for Counter {
            fn write(&mut self,bytes:&[u8])->std::io::Result<usize>{if bytes.len()>MAX_EVENT.saturating_sub(self.0){return Err(std::io::Error::other("oversized observation"));}self.0+=bytes.len();Ok(bytes.len())}
            fn flush(&mut self)->std::io::Result<()>{Ok(())}
        }
        let mut size=Counter(0);
        if serde_json::to_writer(&mut size,&event).is_err(){return event;}
        while state.events.len()>=self.max_history || state.bytes+size.0>MAX_HISTORY_BYTES {
            if let Some((_,bytes))=state.events.pop_front(){state.bytes-=bytes;}else{break;}
        }
        state.bytes+=size.0;state.events.push_back((event.clone(),size.0));
        state.subscribers.retain(|tx|match tx.try_send(event.clone()){Ok(())|Err(TrySendError::Full(_))=>true,Err(TrySendError::Disconnected(_))=>false});
        event
    }
    pub fn snapshot(&self)->Vec<ObservationEvent>{self.state.lock().events.iter().map(|(event,_)|event.clone()).collect()}
    pub fn events_for_message(&self,id:&str)->Vec<ObservationEvent>{self.state.lock().events.iter().filter(|(event,_)|event.message_id==id).map(|(event,_)|event.clone()).collect()}
}