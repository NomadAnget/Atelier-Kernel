//! Routing —— 代际绑定：`message_id → (origin, channel)` 的有界映射。
//!
//! 这是路由层的投影，不是消息状态。回执按 `origin` 路由到正确代际，
//! `accepts` 校验回执来自正确管道；代际退役即清，杜绝"旧代际结果发进同名新进程"
//! （审计 B1 / 宪法 C3"严禁发往同名新进程"）。

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub origin: u64,
    pub channel: String,
}

pub struct Routing {
    bindings: BTreeMap<String, Binding>,
}

impl Default for Routing {
    fn default() -> Self {
        Self::new()
    }
}

impl Routing {
    pub fn new() -> Self {
        Routing { bindings: BTreeMap::new() }
    }

    pub fn bind(&mut self, ids: &[String], origin: u64, channel: &str) {
        for id in ids {
            self.bindings.insert(
                id.clone(),
                Binding { origin, channel: channel.to_string() },
            );
        }
    }

    pub fn origin(&self, id: &str) -> Option<u64> {
        self.bindings.get(id).map(|b| b.origin)
    }

    /// 回执管道校验：仅当回执来自当初绑定的同一管道代际才接受。
    pub fn accepts(&self, id: &str, channel: &str) -> bool {
        self.bindings.get(id).map_or(false, |b| b.channel == channel)
    }

    /// 代际退役：清除该代际全部绑定（批量）。
    pub fn retire(&mut self, origin: u64) {
        self.bindings.retain(|_, b| b.origin != origin);
    }

    pub fn len(&self) -> usize {
        self.bindings.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipt_is_routed_to_its_own_generation_only() {
        let mut r = Routing::new();
        r.bind(&["m1".into()], 1, "chan-1");
        // 旧代际回执被接受
        assert!(r.accepts("m1", "chan-1"));
        // 同名域新代际（origin=2, chan-2）不能截获旧代际回执
        r.bind(&["m2".into()], 2, "chan-2");
        assert!(!r.accepts("m1", "chan-2"));
        // 旧代际退役后，其绑定清除，origin 查询返回 None
        r.retire(1);
        assert_eq!(r.origin("m1"), None);
        assert_eq!(r.origin("m2"), Some(2));
    }
}
