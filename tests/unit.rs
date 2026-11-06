//! 底层能力：账本不变式、消息身份、保留期回收、回执顺序、观察查询内存、分层纪律。
//!
//! 这一层不拉起任何子进程，全部在内存 / SQLite 上直接验证核的语义。

#[path = "cases/unit/invariants.rs"]
mod invariants;
#[path = "cases/unit/identity.rs"]
mod identity;
#[path = "cases/unit/retention.rs"]
mod retention;
#[path = "cases/unit/ordering.rs"]
mod ordering;
#[path = "cases/unit/observer_memory.rs"]
mod observer_memory;
#[path = "cases/unit/discipline.rs"]
mod discipline;
