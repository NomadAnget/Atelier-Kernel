//! C4 存储枢纽端到端验证（宪法 C4）：
//! 业务域只用逻辑路径；内核分配内部 id 并把文件平铺到域根；
//! 读写查询删可跨域，创建只写本域；删除对象 = 删物理文件 + 删索引行。

use atelier_kernel::{
    AtelierKernel, ContractStatus as S, DomainPipeClient, KernelToDomainFrame as F,
    MSG_STORAGE_OBJECT_DELETE, MSG_STORAGE_OBJECT_GET, MSG_STORAGE_OBJECT_LIST,
    MSG_STORAGE_OBJECT_PUT,
};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};

fn terminal(frames: Vec<F>) -> atelier_kernel::MessageReceipt {
    frames
        .into_iter()
        .find_map(|f| match f {
            F::Receipt(r) if r.status.is_terminal() => Some(r),
            _ => None,
        })
        .unwrap()
}

fn temp_root(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("atelier-c4-{tag}-{}", uuid::Uuid::new_v4()))
}

fn stage(root: &Path, owner: &str, name: &str, bytes: &[u8]) -> String {
    let dir = root.join(".staging").join(owner);
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    fs::write(&path, bytes).unwrap();
    path.to_string_lossy().to_string()
}

fn put(
    client: &DomainPipeClient,
    root: &Path,
    owner: &str,
    virtual_path: &str,
    bytes: &[u8],
) -> Value {
    let staging = stage(root, owner, &format!("{}.bin", uuid::Uuid::new_v4()), bytes);
    client.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_PUT,
        json!({"virtual_path": virtual_path, "staging_path": staging}),
        json!({}),
    );
    let rec = terminal(client.drain_frames());
    assert_eq!(rec.status, S::Completed, "put failed: {:?}", rec.data);
    rec.data.unwrap()
}

fn get(
    client: &DomainPipeClient,
    virtual_path: &str,
    owner: Option<&str>,
) -> atelier_kernel::MessageReceipt {
    let params = match owner {
        Some(owner) => json!({"virtual_path": virtual_path, "owner": owner}),
        None => json!({"virtual_path": virtual_path}),
    };
    client.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_GET,
        params,
        json!({}),
    );
    terminal(client.drain_frames())
}

fn delete(
    client: &DomainPipeClient,
    virtual_path: &str,
    owner: Option<&str>,
) -> atelier_kernel::MessageReceipt {
    let params = match owner {
        Some(owner) => json!({"virtual_path": virtual_path, "owner": owner}),
        None => json!({"virtual_path": virtual_path}),
    };
    client.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_DELETE,
        params,
        json!({}),
    );
    terminal(client.drain_frames())
}

fn kernel_with_root(root: &Path) -> AtelierKernel {
    fs::create_dir_all(root).unwrap();
    let db = root.join("kernel.db");
    AtelierKernel::open_with_db_and_root(&db, root).unwrap()
}

#[test]
fn logical_path_is_the_only_address_and_ids_stay_inside_the_kernel() {
    let root = temp_root("address");
    let k = kernel_with_root(&root);
    let producer = k.attach_domain("producer");
    let consumer = k.attach_domain("consumer");

    let obj = put(&producer, &root, "producer", "users/avatars/a.png", b"avatar");
    // 业务可见字段里没有 object_id
    assert!(obj.get("object_id").is_none(), "object_id must not leak: {obj}");
    assert_eq!(obj["virtual_path"], "users/avatars/a.png");
    assert_eq!(obj["size"], 6);
    assert_eq!(obj["file_type"], "image/png");
    let physical = obj["physical_path"].as_str().unwrap().to_string();
    let physical_path = std::path::Path::new(&physical);
    assert_eq!(
        physical_path.parent().and_then(|p| p.file_name()).and_then(|n| n.to_str()),
        Some("producer"),
        "physical file lives flat under the owner root: {physical}"
    );
    assert!(
        physical_path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.len() == 32 && n.chars().all(|c| c.is_ascii_hexdigit())),
        "physical file is named by the uuid object id: {physical}"
    );

    // 跨域按逻辑路径读取（带 owner）
    let read = get(&consumer, "users/avatars/a.png", Some("producer"));
    assert_eq!(read.status, S::Completed, "{:?}", read.data);
    let read = read.data.unwrap();
    assert_eq!(read["physical_path"], obj["physical_path"]);
    assert_eq!(read["size"], 6);

    // 跨域查询
    consumer.send(
        vec!["kernel.storage".into()],
        MSG_STORAGE_OBJECT_LIST,
        json!({"owner": "producer"}),
        json!({}),
    );
    let listed = terminal(consumer.drain_frames());
    assert_eq!(listed.status, S::Completed);
    let objects = listed.data.unwrap()["objects"].as_array().unwrap().clone();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0]["virtual_path"], "users/avatars/a.png");

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn cross_domain_delete_removes_the_physical_file() {
    let root = temp_root("delete");
    let k = kernel_with_root(&root);
    let producer = k.attach_domain("producer");
    let rogue = k.attach_domain("rogue");

    let obj = put(&producer, &root, "producer", "out/a.bin", b"payload");
    let physical = PathBuf::from(obj["physical_path"].as_str().unwrap());
    assert!(physical.exists());

    // 跨域删除允许，且删除文件
    let deleted = delete(&rogue, "out/a.bin", Some("producer"));
    assert_eq!(deleted.status, S::Completed, "{:?}", deleted.data);
    assert_eq!(deleted.data.unwrap()["deleted"], true);
    assert!(!physical.exists(), "physical file must be removed");

    // 删后读取 → not_found
    let gone = get(&rogue, "out/a.bin", Some("producer"));
    assert_eq!(gone.status, S::ExecutionFailed);
    assert_eq!(gone.data.unwrap()["error"], "object_not_found");

    // 幂等
    let again = delete(&rogue, "out/a.bin", Some("producer"));
    assert_eq!(again.status, S::Completed);
    assert_eq!(again.data.unwrap()["deleted"], false);

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn overwrite_reuses_the_same_object_path() {
    let root = temp_root("overwrite");
    let k = kernel_with_root(&root);
    let producer = k.attach_domain("producer");

    let first = put(&producer, &root, "producer", "final/clip.mp4", b"old");
    let second = put(&producer, &root, "producer", "final/clip.mp4", b"new-content");
    assert_eq!(first["physical_path"], second["physical_path"]);
    assert_eq!(second["size"], 11);
    assert_eq!(second["file_type"], "video/mp4");
    assert_eq!(
        fs::read(second["physical_path"].as_str().unwrap()).unwrap(),
        b"new-content"
    );

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn index_survives_restart() {
    let root = temp_root("restart");
    let db = root.join("kernel.db");
    fs::create_dir_all(&root).unwrap();

    let (physical, size) = {
        let k = AtelierKernel::open_with_db_and_root(&db, &root).unwrap();
        let producer = k.attach_domain("producer");
        let obj = put(&producer, &root, "producer", "a/b.bin", b"restart-persistent");
        (
            obj["physical_path"].as_str().unwrap().to_string(),
            obj["size"].as_u64().unwrap(),
        )
    };

    let k = AtelierKernel::open_with_db_and_root(&db, &root).unwrap();
    let producer = k.attach_domain("producer");
    let read = get(&producer, "a/b.bin", None);
    assert_eq!(read.status, S::Completed, "{:?}", read.data);
    let read = read.data.unwrap();
    assert_eq!(read["physical_path"], physical);
    assert_eq!(read["size"], size);

    let _ = fs::remove_dir_all(&root);
}
