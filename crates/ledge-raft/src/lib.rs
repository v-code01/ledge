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
pub mod op;
pub mod state_machine;
pub mod type_config;

pub use log_store::LogStore;
pub use op::{outcome_to_resp, LedgeOp, LedgeResp};
pub use state_machine::StateMachineStore;
pub use type_config::TypeConfig;
