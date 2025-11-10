use std::sync::Arc;

use rocksdb::DB;

use crate::raft::{
    KVResponse, Response, ResponseResult,
    store::common::{deserialize, get_cf_handle, rocksdb_err_to_io, serialize},
};

pub fn operation_del(
    key: String,
    db: Arc<DB>,
    client_id: u64,
    seq_id: Option<u64>,
    pending_state: &mut std::collections::HashMap<Vec<u8>, Option<(Vec<u8>, u64)>>,
    batch: &mut rocksdb::WriteBatchWithTransaction<false>,
) -> Result<crate::raft::Response, std::io::Error> {
    let key_bytes = key.as_bytes().to_vec();
    let sm_data = get_cf_handle(&db, "sm_data")?;

    // Check pending state first for read-your-writes semantics
    let existed = if let Some(pending) = pending_state.get(&key_bytes) {
        // Value is in pending state from earlier entry in this batch
        pending.is_some()
    } else {
        // Not in pending state, read from DB
        db.get_cf(sm_data, &key_bytes)
            .map_err(rocksdb_err_to_io)?
            .is_some()
    };

    // Track this deletion in pending state
    pending_state.insert(key_bytes.clone(), None);
    batch.delete_cf(sm_data, &key_bytes);

    Ok(Response::Result {
        client_id,
        seq_id,
        res: ResponseResult::KV(KVResponse::Del { existed }),
    })
}
