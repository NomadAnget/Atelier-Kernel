//! Real IPC regression: storage results exceeding the former 2 MiB output budget.
#[cfg(target_os = "linux")]
#[test]
fn large_checkpoint_query_returns_over_real_pipe_without_replacing_domain() {
    use atelier_kernel::{AtelierKernel, DomainProcessSpec};
    use std::time::{Duration, Instant};
    let result = std::env::temp_dir().join(format!(
        "atelier-large-storage-{}.json",
        uuid::Uuid::new_v4()
    ));
    let script = r#"
import os,json,sys
r=os.fdopen(int(os.environ['ATELIER_IPC_READ_FD']),'r')
w=os.fdopen(int(os.environ['ATELIER_IPC_WRITE_FD']),'w')
def call(seq,kind,params,data):
 w.write(json.dumps(dict(frame_type='send',seq=seq,targets=['kernel.storage'],message_type=kind,params=params,data=data))+'\n');w.flush()
 while True:
  f=json.loads(r.readline())
  if f.get('seq')==seq and f.get('status')=='completed':return f
payload='x'*(12*1024*1024)
call(1,'StorageInsert',{'record_id':'checkpoint','category':'checkpoints'},{'payload':payload})
for seq in (2,3):
 f=call(seq,'StorageQuery',{'category':'checkpoints'}, {})
 assert f['data']['items'][0]['data']['payload']==payload
with open(sys.argv[1],'w') as out:json.dump({'queries':2,'bytes':len(payload)},out)
# Remain connected so the host can assert this generation is still alive.
while r.readline():pass
"#;
    let k = AtelierKernel::new_in_memory().unwrap();
    let spec: DomainProcessSpec = serde_json::from_value(serde_json::json!({
        "domain_id":"large_storage", "command":"python3",
        "args":["-c",script,result.to_str().unwrap()], "auto_restart":false
    }))
    .unwrap();
    k.supervisor().spawn_process_domain(spec).unwrap();
    let pid = k.supervisor().get_pid("large_storage").unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !result.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    let passed = result.exists();
    let same_pid = k.supervisor().get_pid("large_storage") == Some(pid);
    k.supervisor().stop_process_domain("large_storage");
    let _ = std::fs::remove_file(result);
    assert!(passed, "large storage results did not arrive over IPC");
    assert!(same_pid, "large query replaced the domain process");
}
