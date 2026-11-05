//! 端点传输往返：内核侧 listener/handler 与客户端连接能否双向通信。

use atelier_kernel::endpoint;
use std::io::{Read, Write};

#[test]
fn endpoint_round_trip() {
    let path = endpoint::endpoint_path("test", &uuid::Uuid::new_v4().simple().to_string());
    let mut listener = endpoint::bind(&path).unwrap();

    let client_path = path.clone();
    let client = std::thread::spawn(move || {
        let mut stream = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&client_path)
            .unwrap();
        stream.write_all(b"hello\n").unwrap();
        stream.flush().unwrap();
        let mut reply = [0u8; 6];
        stream.read_exact(&mut reply).unwrap();
        reply
    });

    let mut server = listener.accept().unwrap();
    let mut request = [0u8; 6];
    server.read_exact(&mut request).unwrap();
    server.write_all(b"world\n").unwrap();
    server.flush().unwrap();

    let reply = client.join().unwrap();
    assert_eq!(&request, b"hello\n");
    assert_eq!(&reply, b"world\n");
}
