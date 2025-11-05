pub mod memory;
pub mod rocks;

use serde::Deserialize;
use serde::Serialize;
use std::fmt;

/**
 * Here you will set the types of request that will interact with the raft nodes.
 * For example the `Set` will be used to write data (key and value) to the raft database.
 * The `AddNode` will append a new node to the current existing shared list of nodes.
 * You will want to add any request that can write data in all nodes here.
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Set { key: String, value: String },
    Del { key: String },
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Request::Set { key, value, .. } => {
                write!(f, "Set {{ key: {}, value: {} }}", key, value)
            }
            Request::Del { key } => {
                write!(f, "Del {{ key: {} }}", key)
            }
        }
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
pub struct Response {
    pub value: Option<String>,
}
