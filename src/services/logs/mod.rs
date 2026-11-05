//! 日志服务：`logging`（公共日志/进度中枢）+ `log_spool`（私有持久缓冲）。

pub mod logging;
pub(crate) mod log_spool;
