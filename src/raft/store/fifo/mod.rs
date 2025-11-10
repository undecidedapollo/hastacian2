pub mod operation_dequeue;
pub mod operation_enqueue;

pub mod common;

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::raft::store::fifo::common::QueueMeta;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FIFOEnqueue {
    pub queue_key: Vec<u8>,
    pub values: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FIFODequeue {
    pub queue_key: Vec<u8>,
    pub count: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FIFOOperation {
    Enqueue(FIFOEnqueue),
    Dequeue(FIFODequeue),
}

impl fmt::Display for FIFOOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            FIFOOperation::Enqueue(FIFOEnqueue { values, .. }) => {
                write!(f, "Enqueue {{ values: Vec<Vec<u8>>[{}] }}", values.len(),)
            }
            FIFOOperation::Dequeue(FIFODequeue { count, .. }) => {
                write!(f, "Dequeue {{ count: {} }}", count)
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EnqueueResponse {
    pub queue_meta: QueueMeta,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DequeueResponse {
    pub queue_meta: QueueMeta,
    pub items: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FIFOResponse {
    Enqueue(EnqueueResponse),
    Dequeue(DequeueResponse),
}
