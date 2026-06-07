//! `ledge-workspace` — ephemeral workspace lifecycle for Ledge.
//!
//! A workspace is a ref namespace (`refs/workspaces/<id>/*`) in the Phase 1
//! ref store, governed by a durable [`Lease`]. This crate owns the lease
//! store (Task 2), the lifecycle manager (Task 3), and the mark-and-sweep
//! GC (Task 5).

pub mod gc;
pub mod id;
pub mod lease;
pub mod manager;
pub mod quota;

pub use gc::{Gc, GcStats};
pub use id::WorkspaceId;
pub use lease::{Lease, LeaseStore};
pub use manager::{CommitOutcome, WorkspaceManager, WorkspaceView};
pub use quota::{QuotaLimits, TenantUsage, UsageMap};
