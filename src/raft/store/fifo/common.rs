use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct QueueMeta {
    pub head: u64, // last popped index
    pub tail: u64, // last pushed index
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueueItem {
    pub index: u64,
    pub data: Vec<u8>,
}

pub struct FIFOOverlayQueue {
    pub meta: QueueMeta,
    pub items: Vec<QueueItem>,
}

pub struct FIFOOverlay {
    pub meta: HashMap<Vec<u8>, FIFOOverlayQueue>,
}
