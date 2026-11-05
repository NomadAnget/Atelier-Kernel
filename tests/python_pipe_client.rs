//! 诊断：Python 能否用 open() 直接连接内核的命名管道端点。

use atelier_kernel::endpoint;
use std::io::Read;
use std::time::{Duration, Instant};

#[test]
fn python_opens_named_pipe() {
    let path = endpoint::endpoint_path("pytest", &uuid::Uuid::new_v4().simple().to_string());
    let mut listener = endpoint::bind(&path).unwrap();

    let script = format!(
        "import sys\n\
         f = open(r'{}', 'r+b', buffering=0)\n\
         f.write(b'hello\\n')\n\
         f.flush()\n",
        path
    );
    let mut child = std::process::Command::new("python")
        .args(["-c", &script])
        .spawn()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if child.try_wait().unwrap().is_some() {
            panic!("python client exited before the kernel accepted it");
        }
        if Instant::now() > deadline {
            panic!("python client did not connect within 5s");
        }
        std::thread::sleep(Duration::from_millis(50));
        break;
    }

    let mut stream = listener.accept().unwrap();
    let mut buf = [0u8; 6];
    stream.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"hello\n");
    let _ = child.wait();
}
