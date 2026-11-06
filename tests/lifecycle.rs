//! 域生命周期：拉起 / 注册 / 四阶段 / 停止 / 重启 / 滚动更新 / 环境隔离 / 端口 / 诊断。

#[path = "cases/lifecycle/integration.rs"]
mod integration;
#[path = "cases/lifecycle/environment.rs"]
mod environment;
#[path = "cases/lifecycle/state_observation.rs"]
mod state_observation;
#[path = "cases/lifecycle/ports.rs"]
mod ports;
#[path = "cases/lifecycle/diagnostics.rs"]
mod diagnostics;
#[path = "cases/lifecycle/registration.rs"]
mod registration;
