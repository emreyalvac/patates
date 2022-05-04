use mongodb::bson::document::ValueAccessError;
use mongodb::error::Error as MongoDbError;
use std::result;
use crate::types::Error::Database;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Database(MongoDbError),
    MissingField(ValueAccessError),
    UnknownOperation(String),
    SerdeError(serde_json::Error),
    InvalidOperation,
    ElasticError(elasticsearch::Error),
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