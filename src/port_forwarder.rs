//! Kernel-owned byte forwarding. Public listeners outlive domain generations.
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{self, BufRead, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    process::{Child, ChildStdin, Command, Stdio},
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PortBinding {
    pub name: String,
    pub listen: SocketAddr,
}
struct Control {
    input: ChildStdin,
    replies: mpsc::Receiver<serde_json::Value>,
}
pub(crate) struct Forwarder {
    child: Mutex<Child>,
    control: Mutex<Control>,
    listen: SocketAddr,
    backend: Mutex<Option<SocketAddr>>,
    recovery: Mutex<()>,
}
impl Forwarder {
    pub fn spawn(listen: SocketAddr) -> io::Result<Arc<Self>> {
        let mut exe = std::env::current_exe()?;
        if exe.file_stem().and_then(|s| s.to_str()) != Some("atelier-kernel") {
            exe = exe
                .parent()
                .and_then(|p| p.parent())
                .unwrap()
                .join(if cfg!(windows) {
                    "atelier-kernel.exe"
                } else {
                    "atelier-kernel"
                });
        }
        let mut child = Command::new(exe)
            .args(["--port-relay", &listen.to_string()])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let input = child.stdin.take().unwrap();
        let output = child.stdout.take().unwrap();
        let (tx, replies) = mpsc::channel();
        thread::spawn(move || {
            for line in io::BufReader::new(output).lines() {
                match line.ok().and_then(|s| serde_json::from_str(&s).ok()) {
                    Some(v) => {
                        if tx.send(v).is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
        });
        let relay = Arc::new(Self {
            child: Mutex::new(child),
            control: Mutex::new(Control { input, replies }),
            listen,
            backend: Mutex::new(None),
            recovery: Mutex::new(()),
        });
        let first = relay
            .control
            .lock()
            .replies
            .recv_timeout(Duration::from_secs(5))
            .map_err(|e| io::Error::other(e.to_string()))?;
        if first.get("ready") != Some(&serde_json::Value::Bool(true)) {
            return Err(io::Error::other(first.to_string()));
        }
        Ok(relay)
    }
    fn request(&self, value: serde_json::Value) -> io::Result<serde_json::Value> {
        let mut c = self.control.lock();
        writeln!(c.input, "{value}")?;
        c.input.flush()?;
        c.replies
            .recv_timeout(Duration::from_secs(2))
            .map_err(|e| io::Error::other(e.to_string()))
    }
    pub fn switch(&self, backend: Option<SocketAddr>) -> io::Result<()> {
        self.request(serde_json::json!({"switch":backend.map(|a|a.to_string())}))?;
        *self.backend.lock() = backend;
        Ok(())
    }
    pub fn ensure_alive(&self) -> io::Result<()> {
        let _guard = self.recovery.lock();
        if self.child.lock().try_wait()?.is_none() {
            return Ok(());
        }
        let replacement = Self::spawn(self.listen)?;
        replacement.switch(*self.backend.lock())?;
        std::mem::swap(&mut *self.child.lock(), &mut *replacement.child.lock());
        std::mem::swap(&mut *self.control.lock(), &mut *replacement.control.lock());
        Ok(())
    }
    pub fn connections(&self, backend: SocketAddr) -> io::Result<u64> {
        let v = self.request(serde_json::json!({"count":backend.to_string()}))?;
        Ok(v["count"].as_u64().unwrap_or(0))
    }
}
impl Drop for Forwarder {
    fn drop(&mut self) {
        let c = self.child.get_mut();
        let _ = c.kill();
        let _ = c.wait();
    }
}
#[derive(Default)]
struct Routes {
    backend: Option<SocketAddr>,
    connections: HashMap<SocketAddr, u64>,
}
fn forward(mut client: TcpStream, backend: SocketAddr) {
    if let Ok(mut upstream) = TcpStream::connect_timeout(&backend, Duration::from_secs(5)) {
        if let (Ok(mut client_read), Ok(mut upstream_write)) =
            (client.try_clone(), upstream.try_clone())
        {
            thread::scope(|s| {
                s.spawn(move || {
                    let _ = io::copy(&mut client_read, &mut upstream_write);
                    let _ = upstream_write.shutdown(Shutdown::Write);
                });
                let _ = io::copy(&mut upstream, &mut client);
                let _ = client.shutdown(Shutdown::Write);
            });
        }
    }
}
/// Executed only as a thin child of the kernel. EOF on its private control pipe ends it.
pub fn run(listen: SocketAddr) -> io::Result<()> {
    let listener = TcpListener::bind(listen)?;
    let routes = Arc::new(Mutex::new(Routes::default()));
    let shared = routes.clone();
    thread::spawn(move || {
        for client in listener.incoming() {
            let Ok(client) = client else { break };
            let target = {
                let mut s = shared.lock();
                s.backend
                    .inspect(|b| *s.connections.entry(*b).or_default() += 1)
            };
            if let Some(backend) = target {
                let r = shared.clone();
                thread::spawn(move || {
                    forward(client, backend);
                    let mut s = r.lock();
                    if let Some(n) = s.connections.get_mut(&backend) {
                        *n -= 1;
                        if *n == 0 {
                            s.connections.remove(&backend);
                        }
                    }
                });
            }
        }
    });
    println!("{{\"ready\":true}}");
    io::stdout().flush()?;
    for line in io::stdin().lock().lines() {
        let value: serde_json::Value = serde_json::from_str(&line?).map_err(io::Error::other)?;
        let response = {
            let mut s = routes.lock();
            if let Some(v) = value.get("switch") {
                s.backend = if v.is_null() {
                    None
                } else {
                    Some(
                        v.as_str()
                            .ok_or_else(|| io::Error::other("invalid backend"))?
                            .parse::<SocketAddr>()
                            .map_err(io::Error::other)?,
                    )
                };
                serde_json::json!({"ok":true})
            } else {
                let key = value["count"]
                    .as_str()
                    .unwrap_or("")
                    .parse::<SocketAddr>()
                    .map_err(io::Error::other)?;
                serde_json::json!({"count":s.connections.get(&key).copied().unwrap_or(0)})
            }
        };
        println!("{response}");
        io::stdout().flush()?;
    }
    // Exit the relay process, including forwarding threads, when its kernel dies.
    std::process::exit(0)
}
