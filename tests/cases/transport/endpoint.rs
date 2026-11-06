//! 端点传输往返：内核侧 `bind → accept → into_split` 与客户端能否双向通信。
//!
//! `EndpointReader` / `EndpointWriter` 的契约是**轮询式**的：`read` 无数据时返回
//! `WouldBlock`（由 supervisor 的 `pump_lines` 轮询 `stop`），并非阻塞语义，所以
//! 这里用带截止时间的轮询读，而不是直接 `read_exact`。
//!
//! 客户端打开方式按平台分流：Windows 命名管道支持文件 API；Unix domain socket
//! 是套接字文件，必须 `connect()`。

use atelier_kernel::edge::endpoint;
use std::io::{Read, Write};
use std::time::{Duration, Instant};

fn read_exact_polling(reader: &mut impl Read, len: usize) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut buf = vec![0u8; len];
    let mut filled = 0;
    while filled < len {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => panic!("endpoint closed after {filled} of {len} bytes"),
            Ok(n) => filled += n,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "endpoint read timed out");
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => panic!("endpoint read failed: {error}"),
        }
    }
    buf
}

#[test]
fn endpoint_round_trip() {
    let path = endpoint::endpoint_path("test", &uuid::Uuid::new_v4().simple().to_string());
    let mut listener = endpoint::bind(&path).unwrap();

    let client_path = path.clone();
    let client = std::thread::spawn(move || {
        #[cfg(windows)]
        let mut stream = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&client_path)
            .unwrap();
        #[cfg(unix)]
        let mut stream = std::os::unix::net::UnixStream::connect(&client_path).unwrap();
        stream.write_all(b"hello\n").unwrap();
        stream.flush().unwrap();
        let mut reply = [0u8; 6];
        stream.read_exact(&mut reply).unwrap();
        reply
    });

    let stream = listener.accept().unwrap();
    let (mut reader, mut writer) = stream.into_split();

    let request = read_exact_polling(&mut reader, 6);
    writer.write_all(b"world\n").unwrap();
    writer.flush().unwrap();

    let reply = client.join().unwrap();
    assert_eq!(&request, b"hello\n");
    assert_eq!(&reply, b"world\n");
}
