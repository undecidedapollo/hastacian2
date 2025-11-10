use std::sync::Arc;

use rocksdb::DB;

use crate::raft::{
    Response, ResponseResult,
    store::{
        common::{deserialize, get_cf_handle, rocksdb_err_to_io, serialize},
        fifo::{
            DequeueResponse, FIFODequeue, FIFOResponse,
            common::{FIFOOverlay, FIFOOverlayQueue, QueueItem, QueueMeta},
        },
    },
};

pub fn operation_dequeue(
    op: FIFODequeue,
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

    // Calculate how many items we can actually dequeue
    let available_items = if overlay_queue.meta.tail > overlay_queue.meta.head {
        (overlay_queue.meta.tail - overlay_queue.meta.head) as usize
    } else {
        0
    };

    let items_to_dequeue = std::cmp::min(op.count, available_items);

    let mut dequeued_items = Vec::with_capacity(items_to_dequeue);

    // Dequeue items by advancing the head pointer and deleting from RocksDB
    for _ in 0..items_to_dequeue {
        overlay_queue.meta.head += 1;

        // Create key for the item to read and delete
        let mut item_key = key_bytes.clone();
        item_key.extend_from_slice(&overlay_queue.meta.head.to_be_bytes());

        // First, check if the item is in the overlay
        let item_data = if let Some(item) = overlay_queue
            .items
            .iter()
            .find(|i| i.index == overlay_queue.meta.head)
        {
            item.data.clone()
        } else {
            // If not in overlay, read from database before deleting
            match db.get_cf(&fifo_queue_data, &item_key) {
                Ok(Some(value_bytes)) => match deserialize::<QueueItem>(&value_bytes) {
                    Ok(item) => item.data,
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Failed to deserialize FIFO queue item: {}", e),
                        ));
                    }
                },
                Ok(None) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "FIFO queue item not found in database",
                    ));
                }
                Err(e) => {
                    return Err(rocksdb_err_to_io(e));
                }
            }
        };

        dequeued_items.push(item_data);

        // Delete the item from the database
        batch.delete_cf(fifo_queue_data, &item_key);

        // Remove from overlay items if present
        overlay_queue
            .items
            .retain(|item| item.index > overlay_queue.meta.head);
    }

    // Update the queue metadata in the database
    batch.put_cf(
        fifo_queue_meta,
        &key_bytes,
        &serialize(&overlay_queue.meta)?,
    );

    Ok(Response::Result {
        client_id: client_id,
        seq_id: seq_id,
        res: ResponseResult::FIFO(FIFOResponse::Dequeue(DequeueResponse {
            queue_meta: overlay_queue.meta,
            items: dequeued_items,
        })),
    })
}
