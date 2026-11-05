//! 核内服务：每个服务一个独立单元，不直接互相调用。

pub mod ports;
pub mod objects;
pub mod ledger;
pub mod storage;
pub mod observer_proj;
pub mod observer;
pub mod outbox;
pub mod logs;
