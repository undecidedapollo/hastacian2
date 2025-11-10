mod common;
mod operation_cas;
mod operation_del;
mod operation_set;

pub use common::StoredValue;
pub use operation_cas::operation_cas;
pub use operation_del::operation_del;
pub use operation_set::operation_set;
use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KVSet {
    pub key: String,
    pub value: Vec<u8>,
    pub return_previous: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KVCas {
    pub key: String,
    pub expected_revision: u64,
    pub value: Vec<u8>,
    pub return_previous: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KVOperation {
    Set(KVSet),
    Del { key: String },
    Cas(KVCas),
}

impl fmt::Display for KVOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            KVOperation::Set(KVSet {
                key,
                value,
                return_previous,
            }) => {
                write!(
                    f,
                    "Set {{ key: {}, value: Vec<u8>[{}], return_previous: {} }}",
                    key,
                    value.len(),
                    return_previous
                )
            }
            KVOperation::Del { key } => {
                write!(f, "Del {{ key: {} }}", key)
            }
            KVOperation::Cas(KVCas {
                key,
                expected_revision,
                value,
                return_previous,
            }) => {
                write!(
                    f,
                    "Cas {{ key: {}, expected_revision: {}, value: Vec<u8>[{}], return_previous: {} }}",
                    key,
                    expected_revision,
                    value.len(),
                    return_previous
                )
            } // RequestOperation::FIFO(FIFOOperation::Enqueue { values }) => {
              //     format!("Enqueue {{ values: Vec<u8>[{}] }}", values.len())
              // }
              // RequestOperation::FIFO(FIFOOperation::Dequeue { count }) => {
              //     format!("Dequeue {{ count: {} }}", count)
              // }
        }
    }
}
