use std::io::{self, BufRead, Write};
use std::time::Duration;

use serde_json::{Value, json};

use crate::{
    AtelierKernel, ContractStatus, DomainPipeClient, DomainProcessSpec, DomainStatus,
    KernelToDomainFrame,
};

/// Temporary Interactive Test Frontend Domain (`frontend`).
///
/// Connects to `AtelierKernel` as a standard Domain (`domain_id = "frontend"`),
/// registers `frontend.notify`, and provides an interactive console workbench
/// to exercise every feature of Atelier Kernel.
pub struct InteractiveFrontendDomain<'a> {
    kernel: &'a AtelierKernel,
    client: DomainPipeClient,
}

impl<'a> InteractiveFrontendDomain<'a> {
    /// Attaches the `frontend` domain to `kernel`, registers `frontend.notify`,
    /// and boots the external Python (`order`) and Node.js (`inventory`) domains.
    pub fn attach_and_boot(kernel: &'a AtelierKernel) -> Self {
        let client = kernel.attach_domain("frontend");
        client.register_handlers(&["notify", "render"]);
        let _ = client.drain_frames();

        let slf = Self { kernel, client };
        slf.spawn_external_domains();
        slf
    }

    /// Spawns the external Python (`order`) and Node.js (`inventory`) child-process domains via `Stdio` pipes.
    pub fn spawn_external_domains(&self) {
        let _ = self.kernel.spawn_process_domain(DomainProcessSpec {
            domain_id: "order".to_string(),
            command: "python3".to_string(),
            args: vec!["./domains/order_domain.py".to_string()],
            working_dir: None,
            ..Default::default()
        });

        let _ = self.kernel.spawn_process_domain(DomainProcessSpec {
            domain_id: "inventory".to_string(),
            command: "node".to_string(),
            args: vec!["./domains/inventory_domain.js".to_string()],
            working_dir: None,
            ..Default::default()
        });

        // Wait up to 800ms for both child processes to register their handlers over Stdio pipes
        for _ in 0..40 {
            let order_ok =
                self.kernel.router().domain_status("order") == Some(DomainStatus::Active);
            let inv_ok =
                self.kernel.router().domain_status("inventory") == Some(DomainStatus::Active);
            if order_ok && inv_ok {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Runs the interactive terminal menu loop.
    pub fn run_repl(&self) {
        let stdin = io::stdin();
        let mut reader = stdin.lock();

        self.print_banner();

        loop {
            // Process any incoming messages delivered to `frontend.*` before prompting
            self.process_incoming_deliveries();

            println!(
                "\n┌──────────────────────────────────────────────────────────────────────────┐"
            );
            println!(
                "│                 Atelier Kernel 交互式测试前端域 (domain: frontend)       │"
            );
            println!(
                "├──────────────────────────────────────────────────────────────────────────┤"
            );
            println!(
                "│  [1] 查看所有域生命周期状态 & <domain>.<handler> 自动路由表               │"
            );
            println!(
                "│  [2] 测试跨域调用 Python 域 (order.create) + 四阶段回执 + 自动落盘        │"
            );
            println!(
                "│  [3] 测试跨域调用 Node.js 域 (inventory.deduct) + 四阶段回执             │"
            );
            println!(
                "│  [4] 测试多目标扇出 (同时发往 Python域 + Node域 + 不存在的目标)          │"
            );
            println!(
                "│  [5] 测试无效目标前置拦截 (TargetInvalid 零盲发验证)                     │"
            );
            println!(
                "│  [6] 测试内核生成 message_id 与三级幂等投递 (同 seq 防重 & 回执去重)     │"
            );
            println!(
                "│  [7] 测试“存储即消息” CRUD + 内层 JSON 结构动态演进 + 跨域表物理隔离     │"
            );
            println!(
                "│  [8] 测试其他域反向推送消息给前端域 (order.callback_ui -> frontend.notify)│"
            );
            println!(
                "│  [9] 测试内核接管域生命周期 (停止 Node 库存域 -> 拦截 -> 重新拉起恢复)   │"
            );
            println!(
                "│  [10] 查看实时消息观察组件 (KernelObserver 全链路实时状态流)             │"
            );
            println!(
                "│  [c] 自定义发送任意两层结构消息 (手动输入 targets / type / params / data)│"
            );
            println!(
                "│  [a] 一键自动运行 1~10 全部功能测试演示                                  │"
            );
            println!(
                "│  [0] 退出测试前端                                                        │"
            );
            println!(
                "└──────────────────────────────────────────────────────────────────────────┘"
            );
            print!("请输入选项编号 (如 1~10, c, a, 0): ");
            let _ = io::stdout().flush();

            let mut input = String::new();
            if reader.read_line(&mut input).is_err() {
                break;
            }
            let choice = input.trim();
            if choice.is_empty() {
                continue;
            }

            match choice {
                "1" => self.action_show_routes(),
                "2" => self.action_call_python_order(),
                "3" => self.action_call_node_inventory(),
                "4" => self.action_multi_target_fanout(),
                "5" => self.action_target_invalid(),
                "6" => self.action_idempotency_test(),
                "7" => self.action_storage_crud_and_isolation(),
                "8" => self.action_reverse_callback_to_frontend(),
                "9" => self.action_lifecycle_stop_and_respawn(),
                "10" => self.action_show_observer_events(25),
                "c" | "C" => self.action_custom_message(&mut reader),
                "a" | "A" => self.run_all_automated_demos(),
                "0" | "q" | "Q" | "exit" => {
                    println!("正在关闭 Atelier Kernel 并回收所有托管子进程...");
                    break;
                }
                other => {
                    println!("未知选项: '{other}'，请输入 1~10、c、a 或 0。");
                }
            }
        }
    }

    /// Runs all feature demonstrations sequentially in one command.
    pub fn run_all_automated_demos(&self) {
        println!("\n================ 开始一键全功能自动演练 ================");
        self.action_show_routes();
        self.action_call_python_order();
        self.action_call_node_inventory();
        self.action_multi_target_fanout();
        self.action_target_invalid();
        self.action_idempotency_test();
        self.action_storage_crud_and_isolation();
        self.action_reverse_callback_to_frontend();
        self.action_lifecycle_stop_and_respawn();
        self.action_show_observer_events(30);
        println!("\n================ 全功能自动演练完成 ================");
    }

    fn print_banner(&self) {
        println!("============================================================================");
        println!("  Atelier Kernel V3.1 - 交互式测试前端域已就绪");
        println!("  - 当前前端域身份 (物理管道绑定): frontend");
        println!("  - 已托管子进程域 1: order     (Python3, Stdio JSON-Lines Pipe)");
        println!("  - 已托管子进程域 2: inventory (Node.js, Stdio JSON-Lines Pipe)");
        println!("============================================================================");
    }

    /// Action [1]: Inspect active domains and `<domain>.<handler>` route table.
    pub fn action_show_routes(&self) {
        println!("\n--- [1] 当前域生命周期状态与内核自动路由表 ---");
        for domain in ["frontend", "order", "inventory"] {
            let status = self.kernel.router().domain_status(domain);
            println!("  • 域 [{domain:<10}] 生命周期状态: {status:?}");
        }
        let routes = self.kernel.router().active_routes();
        println!("  • 已挂载的 <域.消息处理器> 路由 ({} 条):", routes.len());
        for r in routes {
            println!("      - {r}");
        }
    }

    /// Action [2]: Call Python `order.create` and observe 4-stage receipts.
    pub fn action_call_python_order(&self) {
        println!("\n--- [2] 测试跨域调用 Python 子进程域 `order.create` ---");
        let msg_id = self.client.send(
            vec!["order.create".to_string()],
            "CreateOrder",
            json!({"channel": "interactive_frontend"}),
            json!({"order_id": "ORD-2026-01", "sku": "ATELIER-GEAR-01", "qty": 2}),
        );
        println!("  -> 内核已生成全局唯一 message_id: {msg_id:?}");
        self.collect_and_print_receipts(&msg_id, 1, Duration::from_secs(2));
    }

    /// Action [3]: Call Node.js `inventory.deduct` and observe 4-stage receipts.
    pub fn action_call_node_inventory(&self) {
        println!("\n--- [3] 测试跨域调用 Node.js 子进程域 `inventory.deduct` ---");
        let msg_id = self.client.send(
            vec!["inventory.deduct".to_string()],
            "DeductStock",
            json!({"warehouse": "WH-MAIN"}),
            json!({"sku": "ATELIER-GEAR-01", "qty": 5}),
        );
        println!("  -> 内核已生成全局唯一 message_id: {msg_id:?}");
        self.collect_and_print_receipts(&msg_id, 1, Duration::from_secs(2));
    }

    /// Action [4]: Multi-target fan-out across Python domain, Node.js domain, and an invalid target.
    pub fn action_multi_target_fanout(&self) {
        println!(
            "\n--- [4] 测试多目标扇出: targets = [\"order.echo\", \"inventory.stock_info\", \"ghost.handler\"] ---"
        );
        let msg_id = self.client.send(
            vec![
                "order.echo".to_string(),
                "inventory.stock_info".to_string(),
                "ghost.handler".to_string(),
            ],
            "MultiTargetInspect",
            json!({"trace": "fanout_test"}),
            json!({"query": "status_check"}),
        );
        println!("  -> 内核已生成全局唯一 message_id: {msg_id:?}");
        println!(
            "  -> 等待 3 个目标各自独立返回回执（2 个走完四阶段，1 个立即返回 TargetInvalid）:"
        );
        self.collect_and_print_receipts(&msg_id, 3, Duration::from_secs(2));
    }

    /// Action [5]: Pre-send `TargetInvalid` interception.
    pub fn action_target_invalid(&self) {
        println!("\n--- [5] 测试无效目标前置拦截 (`TargetInvalid` 零盲发) ---");
        let msg_id = self.client.send(
            vec!["non_existent_domain.do_work".to_string()],
            "TestInvalidTarget",
            json!({}),
            json!({"hello": "world"}),
        );
        println!("  -> 发送至 `non_existent_domain.do_work`，内核分配 message_id: {msg_id:?}");
        self.collect_and_print_receipts(&msg_id, 1, Duration::from_millis(500));
    }

    /// Action [6]: Kernel-generated `message_id` & Three-Level Idempotency verification.
    pub fn action_idempotency_test(&self) {
        println!("\n--- [6] 测试内核生成 `message_id` 与三级幂等投递保证 ---");
        let fixed_seq = 888_888;
        println!(
            "  Step A: 使用固定请求序号 seq={fixed_seq} 首次向 `inventory.deduct` 发送扣减 1 件库存..."
        );
        let msg_id_first = self.client.send_with_seq(
            fixed_seq,
            vec!["inventory.deduct".to_string()],
            "DeductStock",
            json!({"idempotency_demo": true}),
            json!({"sku": "ATELIER-CORE-X", "qty": 1}),
        );
        self.collect_and_print_receipts(&msg_id_first, 1, Duration::from_secs(2));

        println!(
            "  Step B: 模拟发送端重复提交相同的 seq={fixed_seq}（数量改为 99 件尝试重复扣减）..."
        );
        let msg_id_duplicate = self.client.send_with_seq(
            fixed_seq,
            vec!["inventory.deduct".to_string()],
            "DeductStock",
            json!({"idempotency_demo": true}),
            json!({"sku": "ATELIER-CORE-X", "qty": 99}),
        );
        println!("  -> 首次 message_id = {msg_id_first:?}");
        println!(
            "  -> 重复提交返回的 message_id = {msg_id_duplicate:?} (完全一致，未生成新ID，未重复投递！)"
        );
        for frame in self.client.drain_frames() {
            if let KernelToDomainFrame::Receipt(r) = frame {
                println!(
                    "     [幂等重放回执] target={} | status={:?} | data={}",
                    r.target,
                    r.status,
                    serde_json::to_string(&r.data).unwrap_or_default()
                );
            }
        }
    }

    /// Action [7]: "Storage as Messaging" CRUD + Inner JSON evolution + Cross-domain isolation.
    pub fn action_storage_crud_and_isolation(&self) {
        println!("\n--- [7] 测试“存储即消息” (CRUD + 内层 JSON 演进 + 跨域物理隔离) ---");

        // 1. Insert v1 JSON structure into `store_frontend`
        println!("  1) 发送 `StorageInsert` 消息向本域表 `store_frontend` 写入 V1 数据结构...");
        let ins_id = self.client.storage_insert(
            json!({"record_id": "ui_cfg_01", "category": "ui_state", "upsert": true}),
            json!({"layout": "grid", "zoom": 100}),
        );
        self.collect_and_print_receipts(&[ins_id], 1, Duration::from_millis(500));

        // 2. Evolve inner `data` JSON structure (add nested panels & hotkeys) via `StorageUpdate`
        println!(
            "  2) 发送 `StorageUpdate` 动态演进内层 `data` JSON 结构 (新增嵌套字段，内核表结构零修改)..."
        );
        let upd_id = self.client.storage_update(
            json!({"record_id": "ui_cfg_01", "patch": true}),
            json!({
                "zoom": 125,
                "panels": {"inspector": true, "observer_dock": "bottom"},
                "recent_targets": ["order.create", "inventory.deduct"]
            }),
        );
        self.collect_and_print_receipts(&[upd_id], 1, Duration::from_millis(500));

        // 3. Query back the evolved JSON record via `StorageQuery`
        println!("  3) 发送 `StorageQuery` 查询演进后的记录...");
        let qry_id = self.client.storage_query(json!({"record_id": "ui_cfg_01"}));
        self.collect_and_print_receipts(&[qry_id], 1, Duration::from_millis(500));

        // 4. Verify cross-domain storage isolation: `frontend` tries to read `ORD-2026-01` from `order` domain
        println!(
            "  4) 验证物理管道身份隔离: `frontend` 域尝试查询 `order` 域表中的 `ORD-2026-01`..."
        );
        let iso_id = self
            .client
            .storage_query(json!({"domain": "order", "record_id": "ORD-2026-01"}));
        self.collect_and_print_receipts(&[iso_id], 1, Duration::from_millis(500));

        // 5. Delete temporary record via `StorageDelete`
        println!("  5) 发送 `StorageDelete` 删除测试记录 `ui_cfg_01`...");
        let del_id = self
            .client
            .storage_delete(json!({"record_id": "ui_cfg_01"}));
        self.collect_and_print_receipts(&[del_id], 1, Duration::from_millis(500));
    }

    /// Action [8]: Reverse message from Python `order` domain back to `frontend.notify`.
    pub fn action_reverse_callback_to_frontend(&self) {
        println!(
            "\n--- [8] 测试双向互调：`order.callback_ui` 主动向 `frontend.notify` 推送消息 ---"
        );
        let msg_id = self.client.send(
            vec!["order.callback_ui".to_string()],
            "TriggerCallback",
            json!({}),
            json!({"request": "ping_me_back"}),
        );
        self.collect_and_print_receipts(&msg_id, 1, Duration::from_secs(2));
        // Also process any incoming deliveries that arrived right after
        std::thread::sleep(Duration::from_millis(80));
        self.process_incoming_deliveries();
    }

    /// Action [9]: Supervisor lifecycle management (stop a domain -> verify `TargetInvalid` -> respawn -> verify recovery).
    pub fn action_lifecycle_stop_and_respawn(&self) {
        println!(
            "\n--- [9] 测试内核接管域生命周期 (停止子进程 -> 自动摘除路由拦截 -> 重新拉起恢复) ---"
        );
        println!("  1) 内核 Supervisor 停止 `inventory` (Node.js) 域子进程...");
        self.kernel.supervisor().stop_process_domain("inventory");
        println!(
            "     当前 `inventory` 域状态: {:?}",
            self.kernel.router().domain_status("inventory")
        );

        println!(
            "  2) 尝试向已停止的 `inventory.deduct` 发送消息（应立即被拦截并返回 TargetInvalid）:"
        );
        let fail_id = self.client.send(
            vec!["inventory.deduct".to_string()],
            "DeductWhileStopped",
            json!({}),
            json!({"sku": "ATELIER-GEAR-01", "qty": 1}),
        );
        self.collect_and_print_receipts(&fail_id, 1, Duration::from_millis(500));

        println!(
            "  3) 内核 Supervisor 重新拉起 `inventory` (Node.js) 子进程并等待其发送注册消息..."
        );
        let _ = self.kernel.spawn_process_domain(DomainProcessSpec {
            domain_id: "inventory".to_string(),
            command: "node".to_string(),
            args: vec!["./domains/inventory_domain.js".to_string()],
            working_dir: None,
            ..Default::default()
        });
        for _ in 0..40 {
            if self.kernel.router().domain_status("inventory") == Some(DomainStatus::Active) {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        println!(
            "     恢复后 `inventory` 域状态: {:?}",
            self.kernel.router().domain_status("inventory")
        );

        println!("  4) 再次调用 `inventory.stock_info` 验证路由已自动恢复:");
        let ok_id = self.client.send(
            vec!["inventory.stock_info".to_string()],
            "CheckRecoveredStock",
            json!({}),
            json!({}),
        );
        self.collect_and_print_receipts(&ok_id, 1, Duration::from_secs(2));
    }

    /// Action [10]: Inspect recent events captured by the real-time `KernelObserver`.
    pub fn action_show_observer_events(&self, limit: usize) {
        let events = self.kernel.observer().snapshot();
        let total = events.len();
        let start = total.saturating_sub(limit);
        println!(
            "\n--- [10] 内核实时观察组件 (`KernelObserver`) 事件流 (共 {} 条，展示最近 {} 条) ---",
            total,
            total - start
        );
        for ev in &events[start..] {
            println!(
                "  #{:03} | msg={} | {} -> {} | type={} | status={:?}",
                ev.event_id, ev.message_id, ev.source_domain, ev.target, ev.message_type, ev.status
            );
        }
    }

    /// Action [c]: Send a custom message entered interactively by the user.
    fn action_custom_message(&self, reader: &mut impl BufRead) {
        println!("\n--- [c] 自定义发送两层结构内核消息 ---");
        print!("  1) 请输入目标列表 (多个目标用逗号分隔，如 order.echo,inventory.stock_info): ");
        let _ = io::stdout().flush();
        let mut targets_raw = String::new();
        let _ = reader.read_line(&mut targets_raw);
        let targets: Vec<String> = targets_raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if targets.is_empty() {
            println!("  [取消] 目标列表为空。");
            return;
        }

        print!("  2) 请输入消息类型 message_type (默认 CustomInvoke): ");
        let _ = io::stdout().flush();
        let mut msg_type = String::new();
        let _ = reader.read_line(&mut msg_type);
        let msg_type = if msg_type.trim().is_empty() {
            "CustomInvoke".to_string()
        } else {
            msg_type.trim().to_string()
        };

        print!("  3) 请输入上层契约参数 params JSON (直接回车默认 {{}}): ");
        let _ = io::stdout().flush();
        let mut params_raw = String::new();
        let _ = reader.read_line(&mut params_raw);
        let params: Value = if params_raw.trim().is_empty() {
            json!({})
        } else {
            match serde_json::from_str(params_raw.trim()) {
                Ok(v) => v,
                Err(e) => {
                    println!("  [错误] params JSON 解析失败: {e}");
                    return;
                }
            }
        };

        print!("  4) 请输入内层业务数据包 data JSON (直接回车默认 {{\"test\":true}}): ");
        let _ = io::stdout().flush();
        let mut data_raw = String::new();
        let _ = reader.read_line(&mut data_raw);
        let data: Value = if data_raw.trim().is_empty() {
            json!({"test": true})
        } else {
            match serde_json::from_str(data_raw.trim()) {
                Ok(v) => v,
                Err(e) => {
                    println!("  [错误] data JSON 解析失败: {e}");
                    return;
                }
            }
        };

        let target_count = targets.len();
        let msg_id = self.client.send(targets, msg_type, params, data);
        println!("  -> 内核已生成 message_id: {msg_id:?}");
        self.collect_and_print_receipts(&msg_id, target_count, Duration::from_secs(2));
    }

    /// Collects and displays lifecycle receipts for `expected_message_id` until all `target_count`
    /// targets reach a terminal state (`Completed`, `TargetInvalid`, `ContractInvalid`, `ExecutionFailed`),
    /// while also automatically handling any incoming `Deliver` frames addressed to `frontend.*`.
    fn collect_and_print_receipts(
        &self,
        expected_message_ids: &[String],
        target_count: usize,
        timeout: Duration,
    ) {
        let deadline = std::time::Instant::now() + timeout;
        let mut terminal_seen = 0;

        while terminal_seen < target_count && std::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            match self
                .client
                .recv_frame_timeout(remaining.min(Duration::from_millis(250)))
            {
                Ok(KernelToDomainFrame::Receipt(r)) => {
                    let data_summary = match &r.data {
                        Some(v) => {
                            format!(" | data={}", serde_json::to_string(v).unwrap_or_default())
                        }
                        None => String::new(),
                    };
                    println!(
                        "     [回执] msg={} | target={:<22} | status={:<15?}{}",
                        r.message_id, r.target, r.status, data_summary
                    );
                    if expected_message_ids.contains(&r.message_id) && r.status.is_terminal() {
                        terminal_seen += 1;
                    }
                }
                Ok(KernelToDomainFrame::Deliver(msg)) => {
                    self.handle_delivered_to_frontend(msg);
                }
                Err(_) => break,
            }
        }
    }

    fn process_incoming_deliveries(&self) {
        for frame in self.client.drain_frames() {
            if let KernelToDomainFrame::Deliver(msg) = frame {
                self.handle_delivered_to_frontend(msg);
            }
        }
    }

    fn handle_delivered_to_frontend(&self, msg: crate::KernelMessage) {
        println!(
            "     [前端域收到推送消息] 来自域 `{}` -> 目标 `{}` | message_id={} | data={}",
            msg.source_domain,
            msg.target,
            msg.message_id,
            serde_json::to_string(&msg.data).unwrap_or_default()
        );
        // Automatically emit Received -> Started -> Completed back to Kernel
        self.client
            .report_receipt(&msg.message_id, &msg.target, ContractStatus::Received, None);
        self.client
            .report_receipt(&msg.message_id, &msg.target, ContractStatus::Started, None);
        self.client.report_receipt(
            &msg.message_id,
            &msg.target,
            ContractStatus::Completed,
            Some(json!({"displayed_on_frontend": true})),
        );
    }
}
