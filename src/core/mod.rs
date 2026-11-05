//! 极简核心：协议帧 / 路由地址 / 消息核 / 传输。不得依赖 `crate::services`。

pub mod protocol;
pub mod routing;
pub mod nucleus;
pub mod transport;
