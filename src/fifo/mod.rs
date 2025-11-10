use std::{io, sync::Arc};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    core::DistaceanCore,
    raft::{
        FIFOOperation, RequestOperation, Response, ResponseResult,
        store::fifo::{FIFODequeue, FIFOEnqueue, FIFOResponse},
    },
};

#[derive(Clone)]
pub struct DistFIFO {
    pub(crate) distacean: Arc<DistaceanCore>,
}

impl DistFIFO {
    pub async fn enqueue<TKey: Serialize, TVal: Serialize>(
        self: &DistFIFO,
        queue_name: TKey,
        values: Vec<TVal>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.distacean
            .write_or_forward_to_leader(RequestOperation::FIFO(FIFOOperation::Enqueue(
                FIFOEnqueue {
                    queue_key: rmp_serde::to_vec(&queue_name)?,
                    values: values
                        .into_iter()
                        .map(|v| rmp_serde::to_vec(&v).unwrap())
                        .collect(),
                },
            )))
            .await?;
        Ok(())
    }

    pub async fn dequeue<TKey: Serialize, TVal: DeserializeOwned>(
        self: &DistFIFO,
        queue_name: TKey,
        count: usize,
    ) -> Result<Vec<TVal>, Box<dyn std::error::Error + Send + Sync>> {
        let res = self
            .distacean
            .write_or_forward_to_leader(RequestOperation::FIFO(FIFOOperation::Dequeue(
                FIFODequeue {
                    queue_key: rmp_serde::to_vec(&queue_name)?,
                    count,
                },
            )))
            .await?;

        let res = match res {
            Response::Empty => {
                return Err("Unexpected response type".into());
            }
            Response::Result { res, .. } => res,
        };
        if let ResponseResult::FIFO(FIFOResponse::Dequeue(dequeue_res)) = res {
            let items = dequeue_res
                .items
                .into_iter()
                .map(|item_bytes| rmp_serde::from_slice::<TVal>(&item_bytes))
                .collect::<Result<Vec<TVal>, _>>()?;
            Ok(items)
        } else {
            Err("Unexpected FIFO response type".into())
        }
    }
}
