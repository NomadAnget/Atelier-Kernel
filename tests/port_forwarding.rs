//! Real sockets and real SDK processes: stable public ingress across generations.
use atelier_kernel::{AtelierKernel, DomainProcessSpec};
use serde_json::json;
use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    time::{Duration, Instant},
};
fn free_port() -> String {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().to_string()
}
fn line(stream: &mut TcpStream) -> String {
    stream.write_all(b"ping\n").unwrap();
    let mut s = String::new();
    BufReader::new(stream.try_clone().unwrap())
        .read_line(&mut s)
        .unwrap();
    s.trim().into()
}
fn connect(address: &str, expected: &str) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(mut s) = TcpStream::connect(address) {
            s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            let mut got = String::new();
            let _ = s.write_all(b"ping\n");
            if BufReader::new(s.try_clone().unwrap())
                .read_line(&mut got)
                .is_ok()
                && got.trim() == expected
            {
                return s;
            }
        }
        assert!(
            Instant::now() < deadline,
            "public endpoint never returned {expected}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}
fn exercise(node: bool) {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let public = free_port();
    let second = free_port();
    let py = r#"import asyncio,socketserver,threading,sys
from atelier_sdk import AtelierDomain
d=AtelierDomain(domain='service')
@d.handler('ping')
def ping(m):return {'ok':True}
class H(socketserver.StreamRequestHandler):
 def handle(self):
  while self.rfile.readline():self.wfile.write((sys.argv[1]+'\n').encode());self.wfile.flush()
servers=[]
async def start():
 if sys.argv[1]=='bad':raise RuntimeError('intentional startup failure')
 await asyncio.sleep(float(sys.argv[2]))
 for ep in d.bindings.values():
  srv=socketserver.ThreadingTCPServer((ep['host'],ep['port']),H);srv.daemon_threads=True;servers.append(srv);threading.Thread(target=srv.serve_forever,daemon=True).start()
d.run(on_start=start)
"#;
    // run() has no on_start parameter in older facades; the async API is authoritative.
    let py = py.replace(
        "d.run(on_start=start)",
        "asyncio.run(d.run_async(on_start=start))",
    );
    let js = r#"const {AtelierDomain}=require(process.argv[1]);const net=require('node:net');const d=new AtelierDomain({domain:'service'});d.handler('ping',()=>({ok:true}));d.runAsync({onStart:async()=>{if(process.argv[2]==='bad')throw Error('intentional startup failure');await new Promise(r=>setTimeout(r,Number(process.argv[3])*1000));for(const ep of Object.values(d.bindings)){await new Promise((resolve,reject)=>{const server=net.createServer(c=>{c.on('data',()=>c.write(process.argv[2]+'\n'));});server.once('error',reject);server.listen(ep.port,ep.host,resolve);});}}}).catch(()=>{process.exitCode=1;});"#;
    let make = |version: &str, delay: &str| -> DomainProcessSpec {
        let args = if node {
            vec![
                "-e".into(),
                js.into(),
                root.join("sdk/nodejs").to_string_lossy().into(),
                version.into(),
                delay.into(),
            ]
        } else {
            vec!["-c".into(), py.clone(), version.into(), delay.into()]
        };
        serde_json::from_value(json!({"domain_id":"service","command":if node{"node"}else if cfg!(windows){"python"}else{"python3"},"args":args,"env":{"PYTHONPATH":root.join("sdk/python").to_string_lossy()},"auto_restart":false,"ports":[{"name":"http","listen":public},{"name":"other","listen":second}]})).unwrap()
    };
    let k = AtelierKernel::new_in_memory().unwrap();
    k.supervisor()
        .spawn_process_domain(make("old", "0"))
        .unwrap();
    let old = k.supervisor().get_pid("service").unwrap();
    let mut held = connect(&public, "old");
    let _ = connect(&second, "old");
    std::thread::scope(|scope| {
        let spec = make("new", "0.5");
        let update = scope.spawn(|| {
            k.supervisor()
                .update_process_domain("service", Some(spec), None, Duration::from_secs(5))
                .unwrap()
        });
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(line(&mut held), "old");
        let _ = connect(&public, "old");
        let result = update.join().unwrap();
        assert_ne!(result.new_pid, old);
    });
    let mut fresh = connect(&public, "new");
    assert_eq!(
        line(&mut held),
        "old",
        "existing connection must remain on old generation"
    );
    let _ = connect(&second, "new");
    let current = k.supervisor().get_pid("service");
    let err = k.supervisor().update_process_domain(
        "service",
        Some(make("bad", "0")),
        None,
        Duration::from_secs(2),
    );
    assert!(
        err.as_ref()
            .unwrap_err()
            .to_string()
            .contains("intentional startup failure"),
        "startup error was not propagated: {err:?}"
    );
    assert_eq!(k.supervisor().get_pid("service"), current);
    assert_eq!(line(&mut fresh), "new");
    let _ = connect(&public, "new");
    #[cfg(target_os = "linux")]
    {
        let needle = format!("--port-relay {}", public);
        let mut relay_pid = None;
        for entry in std::fs::read_dir("/proc").unwrap().flatten() {
            if let Ok(cmd) = std::fs::read(entry.path().join("cmdline")) {
                let cmd = String::from_utf8_lossy(&cmd).replace('\0', " ");
                if cmd.ends_with(&(needle.clone() + " ")) {
                    relay_pid = entry
                        .file_name()
                        .to_str()
                        .and_then(|n| n.parse::<u32>().ok());
                    break;
                }
            }
        }
        let relay_pid = relay_pid.expect("relay process not found");
        assert!(
            std::process::Command::new("kill")
                .args(["-9", &relay_pid.to_string()])
                .status()
                .unwrap()
                .success()
        );
        let _ = connect(&public, "new");
        assert_eq!(k.supervisor().get_pid("service"), current);
    }
    drop(held);
    std::thread::sleep(Duration::from_millis(150));
    #[cfg(target_os = "linux")]
    assert!(
        !std::path::Path::new(&format!("/proc/{old}")).exists(),
        "old generation was not drained/reaped"
    );
    k.supervisor().stop_process_domain("service");
    drop(k);
    assert!(
        TcpListener::bind(&public).is_ok(),
        "public port not released with host"
    );
}
#[test]
fn python_ports_survive_updates_and_failed_startup() {
    exercise(false)
}
#[test]
fn node_ports_survive_updates_and_failed_startup() {
    exercise(true)
}
