use elasticsearch::Elasticsearch;
use elasticsearch::indices::{IndicesCreateParts, IndicesExistsParts};

pub struct ElasticImpl {
    pub client: Elasticsearch,
}

impl ElasticImpl {
    pub fn new() -> Self {
        let client = Elasticsearch::default();

        Self { client }
    }

    pub async fn check_indices_is_exist(&self, indice: &String) -> bool {
        let exists = self.client.indices().exists(IndicesExistsParts::Index(&[indice.to_lowercase().as_str()])).send().await.unwrap();
        return exists.status_code().is_success();
    }

    pub async fn create_index_if_not_exists(&self, indice: String) -> bool {
        if self.check_indices_is_exist(&indice).await {
            return true;
        } else {
            let res = self.client.indices().create(IndicesCreateParts::Index(indice.to_lowercase().as_str())).send().await;
            match res {
                Ok(_) => true,
                Err(err) => {
                    print!("Create indice error {:?}", err);
                    false
                }
            }
        }
    }
}