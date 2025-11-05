pub(crate) struct AutoAbort<T>(Option<tokio::task::JoinHandle<T>>);

impl<T> Drop for AutoAbort<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

impl<T> AutoAbort<T> {
    pub fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self(Some(handle))
    }

    pub async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        self.0.take().unwrap().await
    }
}
