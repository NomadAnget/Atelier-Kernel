use atelier_kernel::logging::{LOG_PAGE_MAX_BYTES, LOG_PAGE_MAX_ROWS};
use atelier_kernel::{
    AtelierKernel, ContractStatus, KernelLogHub, KernelToDomainFrame, LogQuery, LogRecord, LogStore,
};
use rusqlite::Connection;
use serde_json::json;
use std::{
    collections::HashSet,
    path::PathBuf,
    time::{Duration, Instant},
};
fn path() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tmp/log-pagination-tests");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("{}.db", uuid::Uuid::new_v4()))
}
fn record(i: usize) -> LogRecord {
    LogRecord {
        ts: 100.0,
        message: "identical legitimate text".into(),
        source_domain: Some(if i % 2 == 0 { "alpha" } else { "beta" }.into()),
        level: if i % 3 == 0 { "WARNING" } else { "INFO" }.into(),
        phase: if i % 2 == 0 { "finish" } else { "start" }.into(),
        ..LogRecord::default()
    }
}
#[test]
fn traverse_history_over_old_cap_with_append_restart_and_filters() {
    let db = path();
    let store = LogStore::new(Some(&db)).unwrap();
    for i in 0..2507 {
        store.emit(record(i)).unwrap();
    }
    store.flush(Duration::from_secs(10)).unwrap();
    let first = store
        .query_page(&LogQuery {
            limit: usize::MAX,
            ..LogQuery::default()
        })
        .unwrap();
    assert_eq!(first.total, LOG_PAGE_MAX_ROWS);
    assert!(first.has_more);
    let cursor = first.next_before_id.unwrap();
    let mut ids: Vec<_> = first.logs.iter().map(|r| r.id.unwrap()).collect();
    for i in 0..10 {
        store.emit(record(i)).unwrap();
    }
    store.flush(Duration::from_secs(5)).unwrap();
    drop(store);
    let store = LogStore::new(Some(&db)).unwrap();
    let second = store
        .query_page(&LogQuery {
            before_id: Some(cursor),
            limit: 2000,
            ..LogQuery::default()
        })
        .unwrap();
    assert_eq!(second.total, 507);
    assert!(!second.has_more);
    assert_eq!(second.next_before_id, None);
    ids.extend(second.logs.iter().map(|r| r.id.unwrap()));
    assert_eq!(ids.len(), 2507);
    assert_eq!(ids.iter().copied().collect::<HashSet<_>>().len(), 2507);
    assert!(ids.windows(2).all(|p| p[0] > p[1]));
    assert_eq!(ids[0], 2507);
    assert_eq!(*ids.last().unwrap(), 1);
    let mut cursor = None;
    let mut selected = Vec::new();
    loop {
        let page = store
            .query_page(&LogQuery {
                limit: 17,
                before_id: cursor,
                source_domain: Some("alpha"),
                level: Some("warning"),
                phase: Some("finish"),
                since_ts: Some(100.0),
                ..LogQuery::default()
            })
            .unwrap();
        assert!(
            page.logs
                .iter()
                .all(|r| r.source_domain.as_deref() == Some("alpha")
                    && r.level == "WARNING"
                    && r.phase == "finish")
        );
        selected.extend(page.logs.iter().map(|r| r.id.unwrap()));
        if !page.has_more {
            break;
        }
        cursor = page.next_before_id;
    }
    assert_eq!(selected.len(), 418 + 2);
    assert_eq!(
        selected.len(),
        selected.iter().copied().collect::<HashSet<_>>().len()
    );
}
#[test]
fn byte_budget_ends_pages_without_skipping_and_oversize_is_explicit() {
    let db = path();
    let store = LogStore::new(Some(&db)).unwrap();
    for _ in 0..3 {
        store
            .emit(LogRecord {
                message: "x".repeat(900_000),
                ..LogRecord::default()
            })
            .unwrap();
    }
    store.flush(Duration::from_secs(5)).unwrap();
    let page = store.query_page(&LogQuery::default()).unwrap();
    assert_eq!(page.total, 2);
    assert!(page.has_more);
    assert!(serde_json::to_vec(&page).unwrap().len() <= LOG_PAGE_MAX_BYTES);
    let next = store
        .query_page(&LogQuery {
            before_id: page.next_before_id,
            ..LogQuery::default()
        })
        .unwrap();
    assert_eq!(next.total, 1);
    assert_eq!(next.logs[0].id, Some(1));
    assert!(!next.has_more);
    store
        .emit(LogRecord {
            message: "x".repeat(LOG_PAGE_MAX_BYTES),
            ..LogRecord::default()
        })
        .unwrap();
    store.flush(Duration::from_secs(5)).unwrap();
    let error = store
        .query_page(&LogQuery::default())
        .unwrap_err()
        .to_string();
    assert!(error.contains("id=4"));
    assert!(error.contains("budget"));
}
#[test]
fn cursor_validation_empty_and_exact_end_metadata() {
    let store = LogStore::new(None).unwrap();
    let empty = store.query_page(&LogQuery::default()).unwrap();
    assert_eq!(empty.total, 0);
    assert!(!empty.has_more);
    assert_eq!(empty.next_before_id, None);
    for value in [
        json!({"before_id":0}),
        json!({"before_id":-1}),
        json!({"before_id":1.5}),
        json!({"before_id":"1"}),
        json!({"before_id":true}),
        json!({"limit":0}),
        json!({"limit":"10"}),
        json!({"since_ts":"yesterday"}),
    ] {
        assert!(LogQuery::from_params(&value).is_err(), "{value}");
    }
    assert!(LogQuery::from_params(&json!({"before_id":null})).is_ok());
    for i in 0..2 {
        store.emit(record(i)).unwrap();
    }
    store.flush(Duration::from_secs(3)).unwrap();
    let page = store
        .query_page(&LogQuery {
            limit: 2,
            ..LogQuery::default()
        })
        .unwrap();
    assert_eq!(page.total, 2);
    assert!(!page.has_more);
    assert_eq!(page.next_before_id, None);
    let end = store
        .query_page(&LogQuery {
            before_id: Some(1),
            ..LogQuery::default()
        })
        .unwrap();
    assert!(end.logs.is_empty());
    assert!(!end.has_more);
}
#[test]
fn backlog_never_mixes_unstable_ids_or_deduplicates_distinct_persisted_logs() {
    let dbpath = path();
    let hub = KernelLogHub::new(Some(&dbpath)).unwrap();
    hub.set_console_enabled(false);
    let db = Connection::open(dbpath).unwrap();
    hub.reinject(record(0));
    let deadline = Instant::now() + Duration::from_secs(3);
    while hub.query_logs_page(&LogQuery::default()).unwrap().total < 1 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    db.execute_batch("BEGIN IMMEDIATE").unwrap();
    let mut duplicate = record(0);
    duplicate.source_domain = Some("beta".into());
    duplicate.component = "second".into();
    hub.reinject(duplicate);
    let page = hub
        .query_logs_page(&LogQuery {
            limit: 1,
            ..LogQuery::default()
        })
        .unwrap();
    assert_eq!(page.total, 1);
    assert!(page.logs[0].id.is_some());
    assert!(!page.has_more);
    db.execute_batch("ROLLBACK").unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let page = hub.query_logs_page(&LogQuery::default()).unwrap();
        if page.total == 2 {
            assert_ne!(page.logs[0].id, page.logs[1].id);
            assert_ne!(page.logs[0].source_domain, page.logs[1].source_domain);
            assert_eq!(page.logs[0].message, page.logs[1].message);
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
}
#[test]
fn query_logs_wire_preserves_shape_and_rejects_invalid_cursor() {
    let kernel = AtelierKernel::new_in_memory().unwrap();
    let client = kernel.attach_domain("page_client");
    for (params, status) in [
        (json!({}), ContractStatus::Completed),
        (json!({"before_id":-1}), ContractStatus::ContractInvalid),
        (json!({"before_id":"1"}), ContractStatus::ContractInvalid),
        (json!({"limit":0}), ContractStatus::ContractInvalid),
    ] {
        let ids = client.send(vec!["kernel.logger".into()], "QueryLogs", params, json!({}));
        let receipt = client
            .drain_frames()
            .into_iter()
            .find_map(|f| match f {
                KernelToDomainFrame::Receipt(r) if r.message_id == ids[0] && r.status == status => {
                    Some(r)
                }
                _ => None,
            })
            .unwrap();
        if status == ContractStatus::Completed {
            let data = receipt.data.unwrap();
            assert_eq!(data["total"], 0);
            assert_eq!(data["logs"], json!([]));
            assert_eq!(data["has_more"], false);
            assert!(data["next_before_id"].is_null());
        }
    }
}
