use serde::{Deserialize, Serialize};

/// Value stored in the state machine: (revision, data)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoredValue {
    pub revision: u64,
    pub data: Vec<u8>,
}
