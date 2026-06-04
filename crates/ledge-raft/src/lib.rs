//! `ledge-raft` — the Ledge replicated state machine and Raft storage glue.
//!
//! openraft 0.9.24 is the consensus core. This crate provides the Ledge-specific
//! state machine (`StateMachineStore`), the log storage (`LogStore`), the
//! application command/response types (`LedgeOp`/`LedgeResp`), and the
//! `declare_raft_types!` `TypeConfig`.
//!
//! # openraft 0.9.24 notes (verified against the resolved crate source)
//! - The storage traits live in `openraft::storage` (the v2 `RaftLogStorage` +
//!   `RaftStateMachine`), gated behind the `storage-v2` feature (enabled in our
//!   manifest). They are *sealed*: the blanket `Sealed` impl is what `storage-v2`
//!   unlocks, allowing 3rd-party impls.
//! - The traits use native `async fn` in trait via openraft's `add_async_trait`
//!   macro (which adds `Send` bounds), so impls are plain `async fn` — **not**
//!   `#[async_trait]`.
//! - `RaftLogStorage::append` takes a `LogFlushed<C>` callback; on completion the
//!   impl calls `callback.log_io_completed(Ok(()))`.
//! - `apply` returns `Result<Vec<C::R>, StorageError<C::NodeId>>` (no 0.10
//!   `EntryResponder`/`IOFlushed`).
pub mod log_store;
pub mod log_wal;
pub mod op;
pub mod state_machine;
pub mod type_config;

pub use log_store::LogStore;
pub use log_wal::WalLogStore;
pub use op::{outcome_to_resp, BatchOp, BatchOutcome, LedgeOp, LedgeResp, TxnDecision};
pub use state_machine::{ReadHandle, StateMachineStore};
pub use type_config::TypeConfig;

/// 128-bit transaction id (re-exported from `ledge-core` so cluster-level code
/// can name `ledge_raft::TxnId` alongside the 2PC ops that carry it).
pub use ledge_core::TxnId;

/// Raft node id type for the Ledge cluster (re-exported for downstream crates).
pub type NodeId = u64;
/// Address-bearing node descriptor (openraft `BasicNode`).
pub use openraft::BasicNode as Node;
