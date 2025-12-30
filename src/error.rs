#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unable to deserialize data")]
    DeserializationError {},

    #[error(transparent)]
    FjallError(#[from] fjall::Error),

    #[error(transparent)]
    TryGetError(#[from] bytes::TryGetError),

    #[error(transparent)]
    Utf8Error(#[from] std::str::Utf8Error),

    #[error(transparent)]
    BrotopufError(#[from] brotopuf::DeserializeError),

    #[error(transparent)]
    InnerError(#[from] Box<dyn std::error::Error + Send + Sync>),
}
