pub mod operator_read;
pub mod operator_set;

use std::sync::Arc;

use crate::core::DistaceanCore;
use crate::distkv::operator_read::ReadRequest;
use crate::distkv::operator_read::ReadRequestBuilder;
use crate::distkv::operator_set::SetRequest;
use crate::distkv::operator_set::SetRequestBuilder;
use crate::raft::KVOperation;
use crate::raft::RequestOperation;
use serde::Serialize;

pub use self::operator_set::SetError;

pub(crate) struct DistKVCore {
    pub(crate) distacean: Arc<DistaceanCore>,
}

#[derive(Clone)]
pub struct DistKV {
    pub(crate) core: Arc<DistKVCore>,
}

pub type InitialSetBuilder =
    SetRequestBuilder<operator_set::SetValue<operator_set::SetKey<operator_set::SetDistacean>>>;
pub type InitialReadBuilder =
    ReadRequestBuilder<operator_read::SetKey<operator_read::SetDistacean>>;

impl DistKVCore {}

impl DistKV {
    pub fn set<T: Serialize, V: std::borrow::Borrow<T>>(
        self: &DistKV,
        key: impl Into<String>,
        value: V,
    ) -> InitialSetBuilder {
        SetRequest::builder()
            .distacean(self.core.distacean.clone())
            .key(key.into())
            .value(rmp_serde::to_vec(value.borrow()).expect("Failed to serialize value"))
    }

    pub async fn delete(
        self: &DistKV,
        key: impl Into<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.core
            .distacean
            .write_or_forward_to_leader(RequestOperation::KV(KVOperation::Del { key: key.into() }))
            .await?;
        Ok(())
    }

    pub fn read(self: &DistKV, key: impl Into<String>) -> InitialReadBuilder {
        ReadRequest::builder()
            .distacean(self.core.distacean.clone())
            .key(key.into())
    }
}
