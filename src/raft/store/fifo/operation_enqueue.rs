use std::sync::Arc;

use rocksdb::DB;

use crate::raft::{
    Response, ResponseResult,
    store::{
        common::{deserialize, get_cf_handle, rocksdb_err_to_io, serialize},
        fifo::{
            EnqueueResponse, FIFOEnqueue, FIFOResponse,
            common::{FIFOOverlay, FIFOOverlayQueue, QueueMeta},
        },
    },
};

pub fn operation_enqueue(
    op: FIFOEnqueue,
    db: Arc<DB>,
    client_id: u64,
    seq_id: Option<u64>,
    pending_state: &mut FIFOOverlay,
    batch: &mut rocksdb::WriteBatchWithTransaction<false>,
) -> Result<crate::raft::Response, std::io::Error> {
    let key_bytes = op.queue_key.clone();
    let fifo_queue_meta = get_cf_handle(&db, "fifo_queue_meta")?;
    let fifo_queue_data = get_cf_handle(&db, "fifo_queue_data")?;

    let overlay_queue = pending_state
        .meta
        .entry(key_bytes.clone())
        .or_insert_with(|| {
            // Only load from DB if not already in overlay
            let meta = match db.get_cf(&fifo_queue_meta, &key_bytes) {
                Ok(Some(value_bytes)) => match deserialize(&value_bytes) {
                    Ok(m) => m,
                    Err(_) => QueueMeta { head: 0, tail: 0 },
                },
                _ => QueueMeta { head: 0, tail: 0 },
            };
            FIFOOverlayQueue {
                meta,
                items: vec![],
            }
        });

    for value in op.values.iter() {
        overlay_queue.meta.tail += 1;
        let item = crate::raft::store::fifo::common::QueueItem {
            index: overlay_queue.meta.tail,
            data: value.clone(),
        };
        let item_bytes = serialize(&item)?;
        let mut item_key = key_bytes.clone();
        item_key.extend_from_slice(&overlay_queue.meta.tail.to_be_bytes());
        batch.put_cf(fifo_queue_data, &item_key, &item_bytes);
        overlay_queue.items.push(item);
    }

    batch.put_cf(
        fifo_queue_meta,
        &key_bytes,
        &serialize(&overlay_queue.meta)?,
    );

    Ok(Response::Result {
        client_id: client_id,
        seq_id: seq_id,
        res: ResponseResult::FIFO(FIFOResponse::Enqueue(EnqueueResponse {
            queue_meta: overlay_queue.meta,
        })),
    })
}
