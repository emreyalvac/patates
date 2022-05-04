use elasticsearch::{CreateParts, DeleteParts, Elasticsearch, IndexParts, UpdateParts};
use elasticsearch::http::response::Response;
use elasticsearch::indices::{IndicesCreateParts, IndicesExistsParts};
use serde_json::Value;
use crate::types::{Error, Result};

pub struct ElasticImpl {
    pub client: Elasticsearch,
}

impl ElasticImpl {
    pub fn new() -> Result<Self> {
        let client = Elasticsearch::default();

        Ok(Self { client })
    }

    pub async fn check_index_is_exist(&self, index: &String) -> Result<bool> {
        let exists = self.client.indices().exists(IndicesExistsParts::Index(&[index.as_str()])).send().await?;
        return Ok(exists.status_code().is_success());
    }

    pub async fn create_index_if_not_exists(&self, index: String) -> Result<bool> {
        if self.check_index_is_exist(&index).await? {
            return Ok(true);
        } else {
            let res = self.client.indices().create(IndicesCreateParts::Index(index.as_str())).send().await;
            match res {
                Ok(_) => Ok(false),
                Err(err) => {
                    Err(Error::from(err))
                }
            }
        }
    }

    pub async fn update_index_data(&self, index: String, data: Value, id: String) -> Result<Response> {
        let res = self.client.update(UpdateParts::IndexId(index.as_str(), id.as_str())).body(data).send().await;
        match res {
            Ok(res) => Ok(res),
            Err(_) => Err(Error::ElasticUpdateOperation)
        }
    }

    pub async fn insert_index_data(&self, index: String, data: Value, id: String) -> Result<Response> {
        let res = self.client.index(IndexParts::IndexTypeId(index.as_str(), "_create", id.as_str())).body(data).send().await;
        match res {
            Ok(res) => Ok(res),
            Err(_) => Err(Error::ElasticInsertOperation)
        }
    }

    pub async fn delete_index_data(&self, index: String, id: String) -> Result<Response> {
        let res = self.client.delete(DeleteParts::IndexId(index.as_str(), id.as_str())).send().await;
        match res {
            Ok(res) => Ok(res),
            Err(_) => Err(Error::ElasticDeleteOperation)
        }
    }
}