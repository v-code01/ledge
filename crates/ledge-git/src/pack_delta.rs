//! Git packfile delta decoding re-exports.
//!
//! The delta codec (varints + copy/insert applier) now lives in
//! `ledge_core::delta` so it can be shared with `ledge-object-store` (which
//! cannot depend on `ledge-git`). This module re-exports the items the
//! pack-resolving decoder in `push.rs` consumes.
pub(crate) use ledge_core::delta::{apply_delta, read_ofs_varint};
