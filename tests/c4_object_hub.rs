//! C4 存储枢纽对象索引的端到端验证（宪法 C4）。
//!
//! 大文件不进消息管道：域把本地文件交给内核（StorageObjectPut），内核复制进自己的对象目录、
//! 算 checksum、给 object_id；消息 data 只带 object_id 引用；跨域读取只读；删除权随域走。

use atelier_kernel::{
    AtelierKernel, ContractStatus as S, KernelToDomainFrame as F, MSG_STORAGE_OBJECT_DELETE,
    MSG_STORAGE_OBJECT_GET, MSG_STORAGE_OBJECT_PUT,
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

#[test]
fn c4_object_flow_write_read_cross_domain_delete() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let producer = k.attach_domain("producer");
    let consumer = k.attach_domain("consumer");
    let rogue = k.attach_domain("rogue");

    // 写对象：域本地文件，内核复制进对象目录，域只拿 object_id/size/checksum。
    let dir = std::env::temp_dir().join(format!("c4-src-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let src = dir.join("video.bin");
    let bytes = vec![7u8; 2 * 1024 * 1024 + 1]; // 2 MiB + 1，超过 LARGE_OBJECT_THRESHOLD 的典型对象
    std::fs::write(&src, &bytes).unwrap();

    producer.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_PUT,
        json!({"path": src.to_string_lossy()}),
        json!({}),
    );
    let put = terminal(producer.drain_frames());
    assert_eq!(put.status, S::Completed, "put failed: {:?}", put.data);
    let put_data = put.data.unwrap();
    let object_id = put_data["object_id"].as_str().unwrap().to_string();
    assert_eq!(put_data["size"].as_u64().unwrap(), bytes.len() as u64);

    // 跨域只读解析：任何域都能拿到内核提供的物理文件句柄，但不可写。
    consumer.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_GET,
        json!({"object_id": object_id}),
        json!({}),
    );
    let get = terminal(consumer.drain_frames());
    assert_eq!(get.status, S::Completed, "cross-domain read failed: {:?}", get.data);
    let get_data = get.data.unwrap();
    assert_eq!(get_data["checksum"], put_data["checksum"]);
    assert_eq!(get_data["size"], put_data["size"]);
    assert!(!get_data["path"].as_str().unwrap().is_empty());

    // 非归属域不能删（M3 写权随域）。
    rogue.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_DELETE,
        json!({"object_id": object_id}),
        json!({}),
    );
    let forbidden = terminal(rogue.drain_frames());
    assert_eq!(forbidden.status, S::ExecutionFailed, "cross-domain delete must be forbidden");
    assert_eq!(forbidden.data.unwrap()["error"], "cross_domain_delete_forbidden");

    // 归属域显式删除：索引 + 物理文件一起清掉。
    producer.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_DELETE,
        json!({"object_id": object_id}),
        json!({}),
    );
    let deleted = terminal(producer.drain_frames());
    assert_eq!(deleted.status, S::Completed, "owner delete failed: {:?}", deleted.data);

    // 删除后跨域读取 → object_not_found。
    consumer.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_GET,
        json!({"object_id": object_id}),
        json!({}),
    );
    let gone = terminal(consumer.drain_frames());
    assert_eq!(gone.status, S::ExecutionFailed);
    assert_eq!(gone.data.unwrap()["error"], "object_not_found");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn c4_object_index_survives_restart() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tmp/c4-restart");
    std::fs::create_dir_all(&root).unwrap();
    let db = root.join(format!("kernel-{}.db", uuid::Uuid::new_v4()));
    let src = root.join("src.bin");
    std::fs::write(&src, b"restart-persistent-object").unwrap();

    let object_id = {
        let k = AtelierKernel::open_with_db(&db).unwrap();
        let producer = k.attach_domain("producer");
        producer.send(
            vec!["kernel.storage".into()],
            MSG_STORAGE_OBJECT_PUT,
            json!({"path": src.to_string_lossy()}),
            json!({}),
        );
        let put = terminal(producer.drain_frames());
        assert_eq!(put.status, S::Completed);
        put.data.unwrap()["object_id"].as_str().unwrap().to_string()
    };

    // 跨重启：对象索引持久化（宪法 C4:71），物理文件与索引一致。
    let k = AtelierKernel::open_with_db(&db).unwrap();
    let consumer = k.attach_domain("consumer");
    consumer.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_GET,
        json!({"object_id": object_id}),
        json!({}),
    );
    let get = terminal(consumer.drain_frames());
    assert_eq!(get.status, S::Completed, "object index must survive restart: {:?}", get.data);
    assert_eq!(get.data.unwrap()["size"].as_u64().unwrap(), 25);
    let _ = std::fs::remove_dir_all(&root);
}
