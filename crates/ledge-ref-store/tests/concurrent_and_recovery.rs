use std::sync::Arc;
use ledge_core::{HLC, ObjectId, RefName};
use ledge_ref_store::RefStoreImpl;
use ledge_core::RefStore;
use tempfile::tempdir;
use tokio::task::JoinSet;

fn make_oid(n: u8) -> ObjectId { ObjectId::from_bytes([n; 32]) }

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_writes_64_tasks_disjoint_prefixes() {
    let dir = tempdir().unwrap();
    let store = Arc::new(RefStoreImpl::open(dir.path().to_path_buf(), Arc::new(HLC::new())).unwrap());
    let mut set = JoinSet::new();
    for task_id in 0u8..64 {
        let store = Arc::clone(&store);
        set.spawn(async move {
            for seq in 0u8..10 {
                let name_str = format!("refs/writers/{task_id}/ref{seq:03}");
                let name = RefName::new(&name_str).unwrap();
                let oid = make_oid(task_id.wrapping_add(seq));
                store.update(&name, oid, None).await
                    .unwrap_or_else(|e| panic!("task {task_id} seq {seq} failed: {e:?}"));
            }
        });
    }
    while let Some(result) = set.join_next().await { result.expect("task panicked"); }
    for task_id in 0u8..64 {
        for seq in 0u8..10 {
            let name_str = format!("refs/writers/{task_id}/ref{seq:03}");
            let name = RefName::new(&name_str).unwrap();
            let entry = store.get(&name).await.unwrap()
                .unwrap_or_else(|| panic!("missing ref {name_str}"));
            assert_eq!(entry.target, make_oid(task_id.wrapping_add(seq)));
            assert_eq!(entry.version, 1);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_cas_single_ref() {
    let dir = tempdir().unwrap();
    let store = Arc::new(RefStoreImpl::open(dir.path().to_path_buf(), Arc::new(HLC::new())).unwrap());
    let name = RefName::new("refs/heads/counter").unwrap();
    store.update(&name, make_oid(0), None).await.unwrap();
    let mut set = JoinSet::new();
    for task_id in 0u16..16 {
        let store = Arc::clone(&store);
        let name = name.clone();
        set.spawn(async move {
            let mut attempts = 0u32;
            loop {
                attempts += 1;
                if attempts > 10000 { panic!("task {task_id} exceeded retry limit"); }
                let current = store.get(&name).await.unwrap();
                let Some(current_entry) = current else { continue };
                let new_byte = current_entry.target.as_bytes()[0].wrapping_add(1);
                let new_oid = make_oid(new_byte);
                match store.update(&name, new_oid, Some(current_entry.target)).await {
                    Ok(_) => break,
                    Err(ledge_core::LedgeError::Conflict { .. }) => continue,
                    Err(e) => panic!("unexpected error in task {task_id}: {e:?}"),
                }
            }
        });
    }
    while let Some(result) = set.join_next().await { result.expect("task panicked"); }
    let final_entry = store.get(&name).await.unwrap().expect("ref missing after concurrent CAS");
    assert_eq!(final_entry.version, 17, "expected 17 versions (1 create + 16 updates), got {}", final_entry.version);
}

#[tokio::test]
async fn test_wal_recovery_arbitrary_truncation() {
    let dir = tempdir().unwrap();
    let written: Vec<(String, ObjectId)> = (0u8..20).map(|i| (format!("refs/heads/branch{i:02}"), make_oid(i))).collect();
    {
        let store = RefStoreImpl::open(dir.path().to_path_buf(), Arc::new(HLC::new())).unwrap();
        for (name_str, oid) in &written {
            store.update(&RefName::new(name_str).unwrap(), *oid, None).await.unwrap();
        }
    }
    let wal_path = dir.path().join("refs").join("wal");
    let original_size = std::fs::metadata(&wal_path).unwrap().len();
    let truncate_at = (original_size as f64 * 0.40) as u64;
    { let f = std::fs::OpenOptions::new().write(true).open(&wal_path).unwrap(); f.set_len(truncate_at).unwrap(); }
    let store2 = RefStoreImpl::open(dir.path().to_path_buf(), Arc::new(HLC::new())).unwrap();
    let all = store2.list("refs/heads/").await.unwrap();
    assert!(!all.is_empty(), "at least some refs must survive truncation recovery");
    for (name, entry) in &all {
        let name_str = name.as_str();
        let original = written.iter().find(|(n, _)| n == name_str).expect(&format!("unknown ref {name_str}"));
        assert_eq!(&entry.target, &original.1);
    }
}

#[tokio::test]
async fn test_wal_compaction_then_recovery() {
    let dir = tempdir().unwrap();
    let refs_to_write = 30u8;
    {
        let store = RefStoreImpl::open(dir.path().to_path_buf(), Arc::new(HLC::new())).unwrap();
        for i in 0..refs_to_write {
            store.update(&RefName::new(&format!("refs/heads/r{i:02}")).unwrap(), make_oid(i), None).await.unwrap();
        }
        store.compact_wal().await.unwrap();
        for i in refs_to_write..refs_to_write + 5 {
            store.update(&RefName::new(&format!("refs/heads/post{i}")).unwrap(), make_oid(i), None).await.unwrap();
        }
    }
    let store2 = RefStoreImpl::open(dir.path().to_path_buf(), Arc::new(HLC::new())).unwrap();
    for i in 0..refs_to_write {
        let entry = store2.get(&RefName::new(&format!("refs/heads/r{i:02}")).unwrap()).await.unwrap()
            .unwrap_or_else(|| panic!("pre-compaction ref r{i:02} missing"));
        assert_eq!(entry.target, make_oid(i));
    }
    for i in refs_to_write..refs_to_write + 5 {
        let entry = store2.get(&RefName::new(&format!("refs/heads/post{i}")).unwrap()).await.unwrap()
            .unwrap_or_else(|| panic!("post-compaction ref post{i} missing"));
        assert_eq!(entry.target, make_oid(i));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_background_compaction_triggers_on_threshold() {
    let dir = tempdir().unwrap();
    let store = Arc::new(
        RefStoreImpl::open_with_compaction_threshold(dir.path().to_path_buf(), Arc::new(HLC::new()), 1).unwrap(),
    );
    store.spawn_compaction_task();
    for i in 0u8..20 {
        let name = RefName::new(&format!("refs/heads/compact{i}")).unwrap();
        store.update(&name, make_oid(i), None).await.unwrap();
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    let wal_path = dir.path().join("refs").join("wal");
    let wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    assert!(wal_size < 4096, "WAL size {wal_size} after compaction should be < 4096 bytes");
    for i in 0u8..20 {
        let name = RefName::new(&format!("refs/heads/compact{i}")).unwrap();
        let entry = store.get(&name).await.unwrap().unwrap_or_else(|| panic!("ref compact{i} missing"));
        assert_eq!(entry.target, make_oid(i));
    }
}
