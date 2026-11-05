//! 命名端点传输：Windows 命名管道 / Unix domain socket。
//!
//! 内核是服务端：创建端点 → spawn 域 → accept → 双向读写 JSON-Lines。
//! `EndpointStream` 只实现 `Read`/`Write`，上层读/写线程与协议不变。

use std::io;

/// 由域 ID 与随机后缀生成端点路径。
/// - Windows: `\\.\pipe\atelier-<domain>-<nonce>`
/// - Unix:    `<temp>/atelier-<domain>-<nonce>.sock`
pub fn endpoint_path(domain: &str, nonce: &str) -> String {
    let safe: String = domain
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    #[cfg(windows)]
    {
        format!(r"\\.\pipe\atelier-{safe}-{nonce}")
    }
    #[cfg(unix)]
    {
        format!("{}/atelier-{safe}-{nonce}.sock", std::env::temp_dir().display())
    }
}

/// 绑定一个端点，等待域连接。
pub fn bind(path: &str) -> io::Result<EndpointListener> {
    imp::bind(path)
}

pub use imp::{EndpointListener, EndpointStream};

#[cfg(unix)]
mod imp {
    use std::io;
    use std::os::unix::net::{UnixListener, UnixStream};

    pub struct EndpointListener(UnixListener);

    pub struct EndpointStream(UnixStream);

    pub fn bind(path: &str) -> io::Result<EndpointListener> {
        let _ = std::fs::remove_file(path);
        Ok(EndpointListener(UnixListener::bind(path)?))
    }

    impl EndpointListener {
        pub fn accept(&self) -> io::Result<EndpointStream> {
            Ok(EndpointStream(self.0.accept()?.0))
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
}

#[cfg(windows)]
mod imp {
    use std::ffi::c_void;
    use std::io::{self, Read, Write};

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
        fn CloseHandle(handle: *mut c_void) -> i32;
        fn GetLastError() -> u32;
    }

    pub struct EndpointListener {
        handle: *mut c_void,
    }

    pub struct EndpointStream {
        handle: *mut c_void,
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
            Ok(EndpointStream { handle })
        }
    }

    impl Read for EndpointStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let mut read = 0u32;
            let ok = unsafe {
                ReadFile(self.handle, buf.as_mut_ptr(), buf.len() as u32, &mut read, std::ptr::null_mut())
            };
            if ok == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(read as usize)
            }
        }
    }

    impl Write for EndpointStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut written = 0u32;
            let ok = unsafe {
                WriteFile(self.handle, buf.as_ptr(), buf.len() as u32, &mut written, std::ptr::null_mut())
            };
            if ok == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(written as usize)
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
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

    impl Drop for EndpointStream {
        fn drop(&mut self) {
            if !self.handle.is_null() {
                unsafe {
                    CloseHandle(self.handle);
                }
                self.handle = std::ptr::null_mut();
            }
        }
    }
}
