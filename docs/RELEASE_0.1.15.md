# Atelier Kernel 0.1.15

- 完整继承0.1.14的托管端口、启动就绪握手、滚动更新和域状态镜像；同时包含候选进程启动诊断排空修复及跨平台端口回归修正。
- Python SDK支持 request(..., timeout=None) 与 wait_result(..., timeout=None)；Node SDK支持 request(..., {timeout:null, signal}) 与 waitResult(..., {timeout:null, signal})。无执行总时限等待以最终回执、用户取消或连接断开收口，不因模型加载/下载等正常长操作总耗时判失败。
- request 的消息发送确认仍限30秒。默认有限超时行为不变；不把中间回执当心跳，不新增业务轮询或第二套执行账本。
- 修复Python接收确认与用户取消竞态，保留取消语义；取消本地等待不会自动取消远端处理器，调用方仍须使用所属域的取消契约。
- 新增双SDK各5项终态等待回归，覆盖生命周期回执、终态失败、断连、取消和有限超时兼容，并纳入三平台CI及SDK构建验证。
- 发布Kernel、Python SDK和Node SDK同版本0.1.15产物；标签发布必须通过Linux、Windows、macOS原生CI后才构建和发布。

本版本不修改业务域算法或业务任务状态；引擎Worker生命周期修正属于变体引擎域，不作为Kernel发布内容。
