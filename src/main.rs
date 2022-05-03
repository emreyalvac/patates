use elasticsearch::indices::IndicesCreateParts;
use crate::database::mongo::{Operation, OpLog};
use futures::StreamExt;
use mongodb::options::ClientOptions;
use mongodb::Client;
use serde_json::json;
use crate::elastic::ElasticImpl;

mod database;
mod elastic;

static POSTS_INDEX: &'static str = "test";

#[tokio::main]
async fn main() -> mongodb::error::Result<()> {
    let options = ClientOptions::default();
    let con = Client::with_options(options).unwrap();
    let mut op_log = OpLog::new(&con).await;
    let elastic = ElasticImpl::new();


    while let Some(item) = op_log.next().await {
        let res = item.unwrap();

        match res {
            Operation::Insert { query, collection } => {
                let mut ser = serde_json::to_value(&query).unwrap();
                let id = query.get_object_id("_id").unwrap().to_string();
                ser.as_object_mut().unwrap().remove("_id");
                elastic.create_index_data(collection, ser, id).await;
            }
            Operation::Update { query, collection, target_document } => {
                let mut ser = serde_json::to_value(&query).unwrap();
                let update_query = json!({"doc": ser});
                let id = target_document.get_object_id("_id").unwrap().to_string();
                elastic.update_index_data(collection, update_query, id).await;
            }
            Operation::Delete { query, collection } => {
                println!("delete {} {}", collection, query)
            }
            _ => {}
        }
    }

    Ok(())
}
