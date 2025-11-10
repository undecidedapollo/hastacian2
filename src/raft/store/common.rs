use openraft::StorageError;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use std::io;

use crate::raft::TypeConfig;

/// Serialize a value to MessagePack format
pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, StorageError<TypeConfig>> {
    rmp_serde::to_vec(value).map_err(|e| StorageError::write(&e))
}

/// Deserialize a value from MessagePack format
pub fn deserialize<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
) -> Result<T, StorageError<TypeConfig>> {
    rmp_serde::from_slice(bytes).map_err(|e| StorageError::read(&e))
}

/// Get column family handle with proper error handling
pub fn get_cf_handle<'a>(db: &'a DB, name: &str) -> Result<&'a rocksdb::ColumnFamily, io::Error> {
    db.cf_handle(name)
        .ok_or_else(|| io::Error::other(format!("column family `{}` not found", name)))
}

/// Helper to convert RocksDB errors to IO errors
pub fn rocksdb_err_to_io(e: rocksdb::Error) -> io::Error {
    io::Error::other(e.to_string())
}
