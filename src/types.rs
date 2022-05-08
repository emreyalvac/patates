use crate::types::Error::Database;
use mongodb::bson::document::ValueAccessError;
use mongodb::error::Error as MongoDbError;
use std::result;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Database(MongoDbError),
    MissingField(ValueAccessError),
    UnknownOperation(String),
    SerdeError(serde_json::Error),
    ElasticError(elasticsearch::Error),
    StdError(std::io::Error),
    InvalidOperation,
    ElasticUpdateOperation,
    ElasticInsertOperation,
    ElasticDeleteOperation,
}

impl From<serde_json::Error> for Error {
    fn from(original: serde_json::Error) -> Self {
        Error::SerdeError(original)
    }
}

impl From<ValueAccessError> for Error {
    fn from(original: ValueAccessError) -> Error {
        Error::MissingField(original)
    }
}

impl From<MongoDbError> for Error {
    fn from(original: MongoDbError) -> Self {
        Database(original)
    }
}

impl From<elasticsearch::Error> for Error {
    fn from(original: elasticsearch::Error) -> Self {
        Error::ElasticError(original)
    }
}

impl From<std::io::Error> for Error {
    fn from(original: std::io::Error) -> Self {
        Error::StdError(original)
    }
}