use elasticsearch::{CreateParts, Elasticsearch, IndexParts, UpdateParts};
use elasticsearch::indices::{IndicesCreateParts, IndicesExistsParts};
use serde_json::Value;

pub struct ElasticImpl {
    pub client: Elasticsearch,
}

impl ElasticImpl {
    pub fn new() -> Self {
        let client = Elasticsearch::default();

        Self { client }
    }

    pub async fn check_index_is_exist(&self, indice: &String) -> bool {
        let exists = self.client.indices().exists(IndicesExistsParts::Index(&[indice.as_str()])).send().await.unwrap();
        return exists.status_code().is_success();
    }

    pub async fn create_index_if_not_exists(&self, indice: String) -> bool {
        if self.check_index_is_exist(&indice).await {
            return true;
        } else {
            let res = self.client.indices().create(IndicesCreateParts::Index(indice.as_str())).send().await;
            match res {
                Ok(_) => true,
                Err(err) => {
                    print!("Create indice error {:?}", err);
                    false
                }
            }
        }
    }

    pub async fn update_index_data(&self, index: String, data: Value, id: String) {
        let response = self.client.update(UpdateParts::IndexId(index.as_str(), id.as_str())).body(data).send().await;
        response.unwrap().text().await;
    }

    pub async fn create_index_data(&self, index: String, data: Value, id: String) {
        let response = self.client.index(IndexParts::IndexTypeId(index.as_str(), "_create", id.as_str())).body(data).send().await;
        response.unwrap().text().await;
    }
}