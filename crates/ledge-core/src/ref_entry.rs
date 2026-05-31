use crate::ObjectId;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RefEntry {
    pub target: ObjectId,
    pub hlc: u64,
    pub version: u64,
}
