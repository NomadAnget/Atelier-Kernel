//! C4 存储枢纽虚拟文件索引的端到端验证（宪法 C4）。
//!
//! 域按自己的虚拟路径操作；内核把它映射成唯一 id + 平铺物理文件。消息里只带唯一 id。
//! 跨域可读、可写、可查询、可删除；创建只发生在归属域自己的命名空间；删除对象 = 删索引 + 删物理文件。

use atelier_kernel::{
    AtelierKernel, ContractStatus as S, DomainPipeClient, KernelToDomainFrame as F,
    MSG_STORAGE_OBJECT_COMMIT, MSG_STORAGE_OBJECT_DELETE, MSG_STORAGE_OBJECT_GET,
    MSG_STORAGE_OBJECT_PUT,
};
use serde_json::json;

fn terminal(frames: Vec<F>) -> atelier_kernel::MessageReceipt {
    frames
        .into_iter()
        .find_map(|f| match f {
            F::Receipt(r) if r.status.is_terminal() => Some(r),
            _ => None,
        })
        .unwrap()
}

fn put(client: &DomainPipeClient, virtual_path: &str) -> (String, String) {
    client.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_PUT,
        json!({"virtual_path": virtual_path}),
        json!({}),
    );
    let rec = terminal(client.drain_frames());
    assert_eq!(rec.status, S::Completed, "allocate failed: {:?}", rec.data);
    let d = rec.data.unwrap();
    (
        d["object_id"].as_str().unwrap().to_string(),
        d["physical_path"].as_str().unwrap().to_string(),
    )
}

fn commit(client: &DomainPipeClient, object_id: &str, size: u64) {
    client.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_COMMIT,
        json!({"object_id": object_id, "size": size}),
        json!({}),
    );
    assert_eq!(terminal(client.drain_frames()).status, S::Completed);
}

#[test]
fn virtual_path_maps_to_flat_unique_id_and_cross_domain_reads() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let producer = k.attach_domain("producer");
    let consumer = k.attach_domain("consumer");
    let rogue = k.attach_domain("rogue");

    // 1. 分配虚拟路径 → 唯一 id + 平铺物理路径（物理文件名就是 id，没有子目录）
    let (object_id, physical) = put(&producer, "users/avatars/a.png");
    assert!(object_id.starts_with("obj_"));
    assert!(
        physical.ends_with(&object_id),
        "physical file must be flat named by unique id: {physical}"
    );

    // 2. 域写入物理文件（SDK 拦截后会写到这里）
    let bytes = vec![7u8; 2 * 1024 * 1024 + 1];
    std::fs::write(&physical, &bytes).unwrap();
    commit(&producer, &object_id, bytes.len() as u64);

    // 3. 跨域按 id 只读解析：拿到同一个平铺物理文件
    consumer.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_GET,
        json!({"object_id": object_id}),
        json!({}),
    );
    let get = terminal(consumer.drain_frames());
    assert_eq!(get.status, S::Completed, "cross read failed: {:?}", get.data);
    let d = get.data.unwrap();
    assert_eq!(d["size"].as_u64().unwrap(), bytes.len() as u64);
    assert_eq!(d["physical_path"].as_str().unwrap(), physical);

    // 4. 跨域删除允许，且删除对象 = 删除物理文件
    rogue.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_DELETE,
        json!({"object_id": object_id}),
        json!({}),
    );
    let deleted = terminal(rogue.drain_frames());
    assert_eq!(deleted.status, S::Completed, "cross delete failed: {:?}", deleted.data);
    assert_eq!(deleted.data.unwrap()["deleted"], true);
    assert!(!std::path::Path::new(&physical).exists(), "physical file must be removed");

    // 5. 删后按 id 解析 → not_found
    consumer.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_GET,
        json!({"object_id": object_id}),
        json!({}),
    );
    let gone = terminal(consumer.drain_frames());
    assert_eq!(gone.status, S::ExecutionFailed);
    assert_eq!(gone.data.unwrap()["error"], "object_not_found");

    // 6. 再删一次幂等：Completed + deleted=false
    producer.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_DELETE,
        json!({"object_id": object_id}),
        json!({}),
    );
    let again = terminal(producer.drain_frames());
    assert_eq!(again.status, S::Completed);
    assert_eq!(again.data.unwrap()["deleted"], false);
}

#[test]
fn virtual_path_resolves_after_restart() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tmp/c4-restart");
    std::fs::create_dir_all(&root).unwrap();
    let db = root.join(format!("kernel-{}.db", uuid::Uuid::new_v4()));

    let (object_id, physical) = {
        let k = AtelierKernel::open_with_db(&db).unwrap();
        let producer = k.attach_domain("producer");
        let (object_id, physical) = put(&producer, "a/b.bin");
        std::fs::write(&physical, b"restart-persistent-object").unwrap();
        commit(&producer, &object_id, 25);
        (object_id, physical)
    };

    // 域重启后：内核仍是唯一真相源，虚拟路径仍解析到同一 id / 物理文件
    let k = AtelierKernel::open_with_db(&db).unwrap();
    let producer = k.attach_domain("producer");
    producer.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_GET,
        json!({"virtual_path": "a/b.bin"}),
        json!({}),
    );
    let get = terminal(producer.drain_frames());
    assert_eq!(get.status, S::Completed, "index survives restart: {:?}", get.data);
    let d = get.data.unwrap();
    assert_eq!(d["object_id"].as_str().unwrap(), object_id);
    assert_eq!(d["physical_path"].as_str().unwrap(), physical);
    let _ = std::fs::remove_dir_all(&root);
}
