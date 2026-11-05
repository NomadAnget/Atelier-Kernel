//! 命名端点传输：Windows 命名管道 / Unix domain socket。
//!
//! 内核是服务端：创建端点 → spawn 域 → accept → 拆成读/写两半供两个线程使用。
//! 协议（JSON-Lines）不变。

use std::io;

/// 由域 ID 与随机后缀生成端点路径。
///
/// Unix 的 `sun_path` 上限很小（macOS 约 104 字节），而 macOS 默认 temp 目录
/// （`/var/folders/...`）本身就很长，所以 socket 固定放 `/tmp` 且名字保持紧凑。
pub fn endpoint_path(domain: &str, nonce: &str) -> String {
    let safe: String = domain
        .chars()
        .take(12)
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    #[cfg(windows)]
    {
        format!(r"\\.\pipe\atelier-{safe}-{nonce}")
    }
    #[cfg(unix)]
    {
        format!("/tmp/atl-{safe}-{nonce}.sock")
    }
}

/// 绑定一个端点，等待域连接。
pub fn bind(path: &str) -> io::Result<EndpointListener> {
    imp::bind(path)
}

pub use imp::{EndpointListener, EndpointReader, EndpointStream, EndpointWriter};

#[cfg(unix)]
mod imp {
    use std::io;
    use std::os::unix::net::{UnixListener, UnixStream};

    pub struct EndpointListener {
        listener: UnixListener,
        path: String,
    }

    // 命名管道由内核对象自动回收；Unix socket 是真实文件，必须显式 unlink。
    impl Drop for EndpointListener {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    pub struct EndpointStream(UnixStream);

    pub struct EndpointReader(UnixStream);

    pub struct EndpointWriter(UnixStream);

    pub fn bind(path: &str) -> io::Result<EndpointListener> {
        let _ = std::fs::remove_file(path);
        Ok(EndpointListener {
            listener: UnixListener::bind(path)?,
            path: path.to_string(),
        })
    }

    impl EndpointListener {
        /// Unix 上 accept 必须可轮询：域在连接前退出时，阻塞 accept 会卡死终止流程。
        pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
            self.listener.set_nonblocking(nonblocking)
        }
        pub fn accept(&mut self) -> io::Result<EndpointStream> {
            Ok(EndpointStream(self.listener.accept()?.0))
        }
    }

    impl EndpointStream {
        pub fn into_split(self) -> (EndpointReader, EndpointWriter) {
            let writer = self.0.try_clone().expect("clone unix stream");
            (EndpointReader(self.0), EndpointWriter(writer))
        }
    }

    impl io::Read for EndpointStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.0.read(buf)
        }
    }

    impl io::Write for EndpointStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.write(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.0.flush()
        }
    }

    impl io::Read for EndpointReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.0.read(buf)
        }
    }

    impl EndpointReader {
        pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
            self.0.set_nonblocking(nonblocking)
        }
    }

    impl EndpointWriter {
        pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
            self.0.set_nonblocking(nonblocking)
        }
    }

    impl io::Write for EndpointWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.write(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.0.flush()
        }
    }
}

#[cfg(windows)]
mod imp {
    use std::ffi::c_void;
    use std::io::{self, Read, Write};
    use std::os::windows::io::AsRawHandle;
    use std::sync::Arc;

    const PIPE_ACCESS_DUPLEX: u32 = 0x0000_0003;
    const PIPE_TYPE_BYTE: u32 = 0x0000_0000;
    const PIPE_READMODE_BYTE: u32 = 0x0000_0000;
    const PIPE_WAIT: u32 = 0x0000_0000;
    const ERROR_PIPE_CONNECTED: u32 = 535;
    const BUFFER_SIZE: u32 = 65_536;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CreateNamedPipeW(
            name: *const u16,
            open_mode: u32,
            pipe_mode: u32,
            max_instances: u32,
            out_buffer: u32,
            in_buffer: u32,
            default_timeout: u32,
            security: *const c_void,
        ) -> *mut c_void;
        fn ConnectNamedPipe(handle: *mut c_void, overlapped: *mut c_void) -> i32;
        fn ReadFile(handle: *mut c_void, buf: *mut u8, len: u32, read: *mut u32, overlapped: *mut c_void) -> i32;
        fn WriteFile(handle: *mut c_void, buf: *const u8, len: u32, written: *mut u32, overlapped: *mut c_void) -> i32;
        fn PeekNamedPipe(
            handle: *mut c_void,
            buffer: *mut c_void,
            size: u32,
            read: *mut u32,
            avail: *mut u32,
            left: *mut u32,
        ) -> i32;
        fn CloseHandle(handle: *mut c_void) -> i32;
        fn GetLastError() -> u32;
    }

    pub struct EndpointListener {
        handle: *mut c_void,
    }

    // accept 在独立线程里进行，句柄本身由内核独占。
    unsafe impl Send for EndpointListener {}

    pub struct EndpointStream {
        shared: Arc<SharedHandle>,
    }

    pub struct EndpointReader(Arc<SharedHandle>);

    pub struct EndpointWriter(Arc<SharedHandle>);

    struct SharedHandle(*mut c_void);

    unsafe impl Send for SharedHandle {}
    unsafe impl Sync for SharedHandle {}

    impl Drop for SharedHandle {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    CloseHandle(self.0);
                }
            }
        }
    }

    pub fn bind(path: &str) -> io::Result<EndpointListener> {
        let wide: Vec<u16> = path.encode_utf16().chain(std::iter::once(0)).collect();
        let handle = unsafe {
            CreateNamedPipeW(
                wide.as_ptr(),
                PIPE_ACCESS_DUPLEX,
                PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
                1,
                BUFFER_SIZE,
                BUFFER_SIZE,
                0,
                std::ptr::null(),
            )
        };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(EndpointListener { handle })
    }

    impl EndpointListener {
        pub fn accept(&mut self) -> io::Result<EndpointStream> {
            let connected = unsafe { ConnectNamedPipe(self.handle, std::ptr::null_mut()) };
            if connected == 0 {
                let code = unsafe { GetLastError() };
                if code != ERROR_PIPE_CONNECTED {
                    return Err(io::Error::from_raw_os_error(code as i32));
                }
            }
            let handle = self.handle;
            self.handle = std::ptr::null_mut();
            Ok(EndpointStream {
                shared: Arc::new(SharedHandle(handle)),
            })
        }
    }

    impl EndpointStream {
        pub fn into_split(self) -> (EndpointReader, EndpointWriter) {
            (EndpointReader(self.shared.clone()), EndpointWriter(self.shared))
        }
    }

    impl Read for EndpointStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            read_handle(self.shared.0, buf)
        }
    }

    impl Write for EndpointStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            write_handle(self.shared.0, buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl EndpointReader {
        /// 是否有可读数据（等价于旧实现里的 PeekNamedPipe 轮询）。
        pub fn has_data(&self) -> io::Result<bool> {
            let mut avail = 0u32;
            let ok = unsafe {
                PeekNamedPipe(self.0.0, std::ptr::null_mut(), 0, std::ptr::null_mut(), &mut avail, std::ptr::null_mut())
            };
            if ok == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(avail > 0)
            }
        }
    }

    impl Read for EndpointReader {
        /// 与旧 `ipc_reader` 一致：无数据返回 `WouldBlock`，由上层轮询 `stop`。
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }
            let mut available = 0u32;
            let ok = unsafe {
                PeekNamedPipe(self.0.0, std::ptr::null_mut(), 0, std::ptr::null_mut(), &mut available, std::ptr::null_mut())
            };
            if ok == 0 {
                let error = io::Error::last_os_error();
                return if matches!(error.raw_os_error(), Some(109 | 232)) {
                    Ok(0)
                } else {
                    Err(error)
                };
            }
            if available == 0 {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let count = buf.len().min(available as usize);
            read_handle(self.0.0, &mut buf[..count])
        }
    }

    impl Write for EndpointWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            write_handle(self.0.0, buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn read_handle(handle: *mut c_void, buf: &mut [u8]) -> io::Result<usize> {
        let mut read = 0u32;
        let ok = unsafe { ReadFile(handle, buf.as_mut_ptr(), buf.len() as u32, &mut read, std::ptr::null_mut()) };
        if ok == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(read as usize)
        }
    }

    fn write_handle(handle: *mut c_void, buf: &[u8]) -> io::Result<usize> {
        let mut written = 0u32;
        let ok = unsafe { WriteFile(handle, buf.as_ptr(), buf.len() as u32, &mut written, std::ptr::null_mut()) };
        if ok == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(written as usize)
        }
    }

    impl AsRawHandle for EndpointStream {
        fn as_raw_handle(&self) -> std::os::windows::io::RawHandle {
            self.shared.0 as std::os::windows::io::RawHandle
        }
    }

    impl Drop for EndpointListener {
        fn drop(&mut self) {
            if !self.handle.is_null() {
                unsafe {
                    CloseHandle(self.handle);
                }
            }
        }
    }
}
