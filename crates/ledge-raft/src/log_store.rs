//! In-memory Raft log storage for tests (durable WAL-backed log is Task 6).
//!
//! A `BTreeMap`-backed log plus an in-memory vote/committed cell, modeled on the
//! openraft 0.9 `raft-kv-memstore` example and the `Adaptor` reference in the
//! resolved 0.9.24 source (`src/storage/adapter.rs`).
//!
//! # openraft 0.9.24 trait surface (verified against `src/storage/v2.rs` + `mod.rs`)
//! - `RaftLogReader::try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(range)`.
//! - `RaftLogStorage::get_log_state() -> Result<LogState<C>, _>` where
//!   `LogState { last_purged_log_id, last_log_id }`.
//! - `append<I>(entries, callback: LogFlushed<C>)` — insert, then
//!   `callback.log_io_completed(Ok(()))` (the canonical `Adaptor` shape).
//! - `truncate(log_id)` removes entries with index `>= log_id.index`.
//! - `purge(log_id)` removes entries with index `<= log_id.index`, advances
//!   `last_purged`.
//! - `save_vote`/`read_vote` and `save_committed`/`read_committed` live on
//!   `RaftLogStorage` in 0.9.24 (committed has a defaulted no-op; we persist it).
//! - `type LogReader; get_log_reader() -> Self::LogReader` — we return a clone
//!   sharing the same `Arc<Mutex<_>>`.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, RaftLogReader, StorageError, Vote};
use tokio::sync::Mutex;

use crate::type_config::TypeConfig;

/// Mutable in-memory log state, guarded by an async mutex so a cloned reader
/// shares it.
#[derive(Default)]
struct Inner {
    /// index -> entry
    log: BTreeMap<u64, Entry<TypeConfig>>,
    /// Last purged log id (entries `<=` this are gone, compacted into a snapshot).
    last_purged: Option<LogId<u64>>,
    /// Persisted hard state.
    vote: Option<Vote<u64>>,
    /// Last committed log id (persisted; openraft defaults this to a no-op).
    committed: Option<LogId<u64>>,
}

/// In-memory Raft log storage. Cloning yields a handle sharing the same state,
/// which is exactly what `get_log_reader` needs.
#[derive(Clone, Default)]
pub struct LogStore {
    inner: Arc<Mutex<Inner>>,
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        let entries = inner
            .log
            .range(range)
            .map(|(_, e)| e.clone())
            .collect::<Vec<_>>();
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        let last = inner
            .log
            .iter()
            .next_back()
            .map(|(_, e)| e.log_id)
            // No present entry: the last log id is the last purged id (if any).
            .or(inner.last_purged);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: last,
        })
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.inner.lock().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().await.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.inner.lock().await.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().await.committed)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut inner = self.inner.lock().await;
            for entry in entries {
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        // In-memory: the entries are durable the instant they are in the map, so
        // signal completion immediately (matches the `Adaptor` flush-before-return
        // shape).
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        // Remove conflicting entries since `log_id`, inclusive.
        let mut inner = self.inner.lock().await;
        inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        inner.last_purged = Some(log_id);
        // Retain only entries strictly after `log_id.index`.
        inner.log = inner.log.split_off(&(log_id.index + 1));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op::LedgeOp;
    use openraft::storage::RaftLogStorage;
    use openraft::{CommittedLeaderId, EntryPayload, RaftLogReader, Vote};

    fn entry(index: u64) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(LedgeOp::RefUpdate {
                name: "refs/heads/main".into(),
                target_bytes: [index as u8; 32],
                expected_bytes: None,
                hlc: index,
            }),
        }
    }

    // openraft's LogFlushed has no public constructor, so the append+read test
    // exercises insertion via the same map path append uses, then verifies the
    // reader range semantics. The append callback wiring is exercised by the
    // cluster harness in Task 3 (which constructs LogFlushed internally).
    async fn insert(store: &LogStore, indices: &[u64]) {
        let mut inner = store.inner.lock().await;
        for &i in indices {
            inner.log.insert(i, entry(i));
        }
    }

    #[tokio::test]
    async fn append_then_read_range_roundtrip() {
        let store = LogStore::default();
        insert(&store, &[0, 1, 2]).await;
        let mut reader = store.clone();
        let got = reader.try_get_log_entries(0..3).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].log_id.index, 0);
        assert_eq!(got[2].log_id.index, 2);

        // Half-open range [1, 3) -> indices 1, 2.
        let got = reader.try_get_log_entries(1..3).await.unwrap();
        assert_eq!(got.iter().map(|e| e.log_id.index).collect::<Vec<_>>(), vec![1, 2]);
    }

    #[tokio::test]
    async fn save_and_read_vote() {
        let mut store = LogStore::default();
        assert_eq!(store.read_vote().await.unwrap(), None);
        let v = Vote::new(3, 1);
        store.save_vote(&v).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(v));
    }

    #[tokio::test]
    async fn save_and_read_committed() {
        let mut store = LogStore::default();
        assert_eq!(store.read_committed().await.unwrap(), None);
        let c = LogId::new(CommittedLeaderId::new(1, 1), 5);
        store.save_committed(Some(c)).await.unwrap();
        assert_eq!(store.read_committed().await.unwrap(), Some(c));
    }

    #[tokio::test]
    async fn get_log_state_reports_last() {
        let mut store = LogStore::default();
        let st = store.get_log_state().await.unwrap();
        assert_eq!(st.last_log_id, None);
        assert_eq!(st.last_purged_log_id, None);

        insert(&store, &[0, 1, 2]).await;
        let st = store.get_log_state().await.unwrap();
        assert_eq!(st.last_log_id.unwrap().index, 2);
    }

    #[tokio::test]
    async fn truncate_removes_from_index_inclusive() {
        let mut store = LogStore::default();
        insert(&store, &[0, 1, 2, 3, 4]).await;
        store
            .truncate(LogId::new(CommittedLeaderId::new(1, 1), 2))
            .await
            .unwrap();
        let mut reader = store.clone();
        let got = reader.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(got.iter().map(|e| e.log_id.index).collect::<Vec<_>>(), vec![0, 1]);
    }

    #[tokio::test]
    async fn purge_removes_upto_index_inclusive_and_sets_last_purged() {
        let mut store = LogStore::default();
        insert(&store, &[0, 1, 2, 3, 4]).await;
        let upto = LogId::new(CommittedLeaderId::new(1, 1), 2);
        store.purge(upto).await.unwrap();
        let mut reader = store.clone();
        let got = reader.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(got.iter().map(|e| e.log_id.index).collect::<Vec<_>>(), vec![3, 4]);

        let st = store.get_log_state().await.unwrap();
        assert_eq!(st.last_purged_log_id, Some(upto));
        assert_eq!(st.last_log_id.unwrap().index, 4);
    }

    #[tokio::test]
    async fn get_log_state_after_full_purge_returns_purged_as_last() {
        let mut store = LogStore::default();
        insert(&store, &[0, 1, 2]).await;
        let upto = LogId::new(CommittedLeaderId::new(1, 1), 2);
        store.purge(upto).await.unwrap();
        let st = store.get_log_state().await.unwrap();
        // No present entries: last_log_id falls back to last_purged.
        assert_eq!(st.last_log_id, Some(upto));
    }
}
