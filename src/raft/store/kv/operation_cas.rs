use std::sync::Arc;

use rocksdb::DB;

use crate::{
    SetResponse,
    raft::{
        KVResponse, Response, ResponseResult,
        store::common::{deserialize, get_cf_handle, rocksdb_err_to_io, serialize},
        store::kv::{KVCas, common::StoredValue},
    },
};

pub fn operation_cas(
    op: KVCas,
    db: Arc<DB>,
    client_id: u64,
    seq_id: Option<u64>,
    pending_state: &mut std::collections::HashMap<Vec<u8>, Option<(Vec<u8>, u64)>>,
    batch: &mut rocksdb::WriteBatchWithTransaction<false>,
) -> Result<crate::raft::Response, std::io::Error> {
    let key_bytes = op.key.as_bytes().to_vec();
    let sm_data = get_cf_handle(&db, "sm_data")?;

    // Get current value and revision (only if return_previous is true)
    let (prev_value, current_revision) = if op.return_previous {
        if let Some(pending) = pending_state.get(&key_bytes) {
            // Value is in pending state from earlier entry in this batch
            match pending {
                Some((data, rev)) => (Some(data.clone()), *rev),
                None => (None, 0),
            }
        } else {
            // Not in pending state, read from DB
            match db.get_cf(sm_data, &key_bytes).map_err(rocksdb_err_to_io)? {
                Some(bytes) => {
                    let stored = deserialize::<StoredValue>(&bytes)?;
                    (Some(stored.data), stored.revision)
                }
                None => (None, 0),
            }
        }
    } else {
        // Only get revision, skip fetching the value
        let current_revision = if let Some(pending) = pending_state.get(&key_bytes) {
            match pending {
                Some((_, rev)) => *rev,
                None => 0,
            }
        } else {
            match db.get_cf(sm_data, &key_bytes).map_err(rocksdb_err_to_io)? {
                Some(bytes) => deserialize::<StoredValue>(&bytes)?.revision,
                None => 0,
            }
        };
        (None, current_revision)
    };

    // Check if revision matches
    let success = current_revision == op.expected_revision;

    if success {
        // CAS succeeds - increment revision and store new value
        let new_revision = current_revision + 1;
        let stored_value = StoredValue {
            revision: new_revision,
            data: op.value.clone(),
        };
        let stored_bytes = serialize(&stored_value)?;

        batch.put_cf(sm_data, &key_bytes, &stored_bytes);
        pending_state.insert(key_bytes.clone(), Some((op.value.clone(), new_revision)));

        Ok(Response::Result {
            client_id,
            seq_id,
            res: ResponseResult::KV(KVResponse::Cas {
                success: true,
                response: SetResponse {
                    prev_value,
                    revision: new_revision,
                },
            }),
        })
    } else {
        // CAS fails - return current revision
        Ok(Response::Result {
            client_id,
            seq_id,
            res: ResponseResult::KV(KVResponse::Cas {
                success: false,
                response: SetResponse {
                    prev_value,
                    revision: current_revision,
                },
            }),
        })
    }
}
