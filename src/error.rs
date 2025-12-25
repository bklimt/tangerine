use std::str::Utf8Error;

use bytes::TryGetError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unable to deserialize data")]
    DeserializationError {},

    #[error(transparent)]
    FjallError(#[from] fjall::Error),

    #[error(transparent)]
    TryGetError(#[from] TryGetError),

    #[error(transparent)]
    Utf8Error(#[from] Utf8Error),

    #[error(transparent)]
    InnerError(#[from] Box<dyn std::error::Error + Send + Sync>),
}
