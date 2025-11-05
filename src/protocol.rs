use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum RequestType {
    AppendEntriesRequest(Vec<u8>),
    InstallSnapshotRequest(Vec<u8>),
    VoteRequest(Vec<u8>),
    AppRequest(crate::store::Request),
}
