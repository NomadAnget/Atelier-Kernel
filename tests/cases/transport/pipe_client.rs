//! 诊断：Python 能否用 `open()` 直接连接内核的命名管道端点。
//!
//! 这是 Windows 独有语义——命名管道可以用文件 API 打开；Unix domain socket
//! 只能 `socket.connect()`，`open()` 必然失败，所以在非 Windows 上不编译此测试。

#[cfg(windows)]
use atelier_kernel::edge::endpoint;

#[cfg(windows)]
#[test]
fn python_opens_named_pipe() {
    use std::io::Read;
    use std::time::Duration;

    let path = endpoint::endpoint_path("pytest", &uuid::Uuid::new_v4().simple().to_string());
    let mut listener = endpoint::bind(&path).unwrap();

    // 先在独立线程里进入 accept（ConnectNamedPipe 会阻塞等待客户端），
    // 再拉起 python。这样不存在「客户端先连上又断开」的窗口。
    let (accepted_tx, accepted_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = accepted_tx.send(listener.accept());
    });

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

    let mut stream = match accepted_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(Ok(stream)) => stream,
        Ok(Err(error)) => {
            let _ = child.kill();
            panic!("kernel accept failed: {error}");
        }
        Err(_) => {
            let _ = child.kill();
            panic!("python client did not connect within 10s");
        }
    };

    let mut buf = [0u8; 6];
    stream.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"hello\n");
    let _ = child.wait();
}
