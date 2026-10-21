use atelier_kernel::{LogRecord, LogStore};
use rusqlite::Connection;
use std::time::{Duration, Instant};
fn path() -> std::path::PathBuf {
    let root =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tmp/log-spool-acceptance");
    std::fs::create_dir_all(&root).unwrap();
    root.join(format!("{}.db", uuid::Uuid::new_v4()))
}
#[test]
fn more_than_old_capacity_survives_locked_database_and_store_restart() {
    let path = path();
    let store = LogStore::new(Some(&path)).unwrap();
    let blocker = Connection::open(&path).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE;").unwrap();
    let start = Instant::now();
    for i in 0..12_500 {
        store
            .emit(LogRecord {
                message: format!("backlog-{i}"),
                ..LogRecord::default()
            })
            .unwrap();
    }
    let accepted = start.elapsed();
    assert!(store.flush(Duration::from_millis(20)).is_err());
    drop(store); // Import remains blocked; accepted records must survive writer shutdown.
    blocker.execute_batch("ROLLBACK;").unwrap();
    let restarted = LogStore::new(Some(&path)).unwrap();
    restarted.flush(Duration::from_secs(15)).unwrap();
    let count: i64 = blocker
        .query_row("SELECT count(*) FROM hub_logs", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 12_500);
    let unique: i64 = blocker
        .query_row("SELECT count(DISTINCT message) FROM hub_logs", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(unique, count);
    drop(restarted);
    let again = LogStore::new(Some(&path)).unwrap();
    again.flush(Duration::from_secs(2)).unwrap();
    assert_eq!(
        blocker
            .query_row("SELECT count(*) FROM hub_logs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        count
    );
    eprintln!(
        "accepted=12500 while target locked, append_elapsed={accepted:?}, recovered={count}, duplicates=0"
    );
}
#[test]
fn flush_confirms_commit_and_transient_records_remain_unpersisted() {
    let path = path();
    let store = LogStore::new(Some(&path)).unwrap();
    store
        .emit(LogRecord {
            message: "durable".into(),
            ..LogRecord::default()
        })
        .unwrap();
    store
        .emit(LogRecord {
            message: "progress".into(),
            ephemeral: true,
            ..LogRecord::default()
        })
        .unwrap();
    store
        .emit(LogRecord {
            message: "console".into(),
            console_only: true,
            ..LogRecord::default()
        })
        .unwrap();
    store.flush(Duration::from_secs(3)).unwrap();
    let db = Connection::open(&path).unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM hub_logs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        db.query_row("SELECT message FROM hub_logs", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "durable"
    );
}

#[test]
#[ignore = "subprocess crash-window fixture, invoked by recovery test"]
fn committed_before_cleanup_exit_fixture() {
    let path = std::env::var("ATELIER_LOG_CRASH_FIXTURE").unwrap();
    let store = LogStore::new(Some(std::path::Path::new(&path))).unwrap();
    let spool = Connection::open(format!("{path}.log-spool.sqlite")).unwrap();
    spool.execute_batch("CREATE TRIGGER stop_cleanup BEFORE DELETE ON pending_logs BEGIN SELECT RAISE(FAIL,'crash window'); END;").unwrap();
    store
        .emit(LogRecord {
            message: "committed-before-exit".into(),
            ..LogRecord::default()
        })
        .unwrap();
    store.flush(Duration::from_secs(3)).unwrap();
    assert_eq!(
        spool
            .query_row("SELECT count(*) FROM pending_logs", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    std::process::exit(0); // Deliberately skip destructor and leave committed journal rows behind.
}
#[test]
fn actual_process_exit_between_commit_and_cleanup_replays_without_duplicates() {
    let path = path();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "committed_before_cleanup_exit_fixture",
            "--ignored",
            "--nocapture",
        ])
        .env("ATELIER_LOG_CRASH_FIXTURE", &path)
        .status()
        .unwrap();
    assert!(status.success());
    let spool = Connection::open(format!("{}.log-spool.sqlite", path.display())).unwrap();
    spool.execute_batch("DROP TRIGGER stop_cleanup;").unwrap();
    let store = LogStore::new(Some(&path)).unwrap();
    store.flush(Duration::from_secs(3)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let pending: i64 = spool
            .query_row("SELECT count(*) FROM pending_logs", [], |r| r.get(0))
            .unwrap();
        if pending == 0 {
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    let db = Connection::open(&path).unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM hub_logs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
}
