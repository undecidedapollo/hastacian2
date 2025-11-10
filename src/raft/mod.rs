use std::fmt;

use serde::{Deserialize, Serialize};

pub mod store;
pub use store::fifo::FIFOOperation;
pub use store::kv::KVOperation;

use crate::raft::store::fifo::FIFOResponse;

pub type NodeId = u64;

/**
 * Here you will set the types of request that will interact with the raft nodes.
 * For example the `Set` will be used to write data (key and value) to the raft database.
 * The `AddNode` will append a new node to the current existing shared list of nodes.
 * You will want to add any request that can write data in all nodes here.
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Request {
    pub client_id: NodeId,
    pub seq_id: Option<u64>,
    pub op: RequestOperation,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RequestOperation {
    KV(KVOperation),
    FIFO(FIFOOperation),
}

impl fmt::Display for RequestOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            RequestOperation::KV(kv_op) => write!(f, "{}", kv_op),
            RequestOperation::FIFO(fifo_op) => write!(f, "{}", fifo_op),
        }
    }
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Request {{ client_id: {}, seq_id: {:?}, op: {} }}",
            self.client_id, self.seq_id, self.op
        )
    }
}

/**
 * Here you define the response type for client read/write requests.
 *
 * This Response type is used as the `AppDataResponse` in the `TypeConfig`.
 * It represents the result returned to clients after applying operations
 * to the state machine.
 *
 * In this example, it returns an optional value for a given key.
 *
 * ## Using Multiple Response Types
 *
 * For applications with diverse operations, you can use an enum:
 *
 * ```ignore
 * #[derive(Serialize, Deserialize, Debug, Clone)]
 * pub enum Response {
 *     Get { value: Option<String> },
 *     Set { prev_value: Option<String> },
 *     Delete { existed: bool },
 *     List { keys: Vec<String> },
 * }
 * ```
 *
 * Each variant corresponds to a different operation in your `Request` enum,
 * providing strongly-typed responses for different client operations.
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    Empty,
    Result {
        client_id: NodeId,
        seq_id: Option<u64>,
        res: ResponseResult,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ResponseResult {
    Empty,
    KV(KVResponse),
    FIFO(FIFOResponse),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SetResponse {
    pub prev_value: Option<Vec<u8>>,
    pub revision: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KVResponse {
    Set(SetResponse),
    Del {
        existed: bool,
    },
    Cas {
        success: bool,
        response: SetResponse,
    },
}

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = Request,
        R = Response,
);

pub type StateMachineStore = store::RocksStateMachine;
pub type Raft = openraft::Raft<TypeConfig>;
