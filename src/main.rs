use elasticsearch::indices::IndicesCreateParts;
use futures::StreamExt;
use mongodb::bson::oid::ObjectId;
use mongodb::options::ClientOptions;
use mongodb::Client;
use serde_json::json;
use crate::elastic::ElasticImpl;
use crate::oplog::{Operation, OpLog};
use crate::types::{Error, Result};

mod types;
mod oplog;
mod elastic;

#[tokio::main]
async fn main() -> Result<()> {
    let options = ClientOptions::default();
    let con = Client::with_options(options)?;
    let mut op_log = OpLog::new(&con).await;
    let elastic = ElasticImpl::new()?;

    while let Some(item) = op_log.next().await {
        let res = item?;

        match res {
            Operation::Insert { query, collection } => {
                let mut ser = serde_json::to_value(&query)?;
                let id = query.get_object_id("_id")?.to_string();
                ser.as_object_mut().unwrap().remove("_id");
                elastic.insert_index_data(collection, ser, id).await?;
            }
            Operation::Update { query, collection, target_document } => {
                let mut ser = serde_json::to_value(&query).unwrap();
                let update_query = json!({"doc": ser});
                let id = target_document.get_object_id("_id")?.to_string();
                elastic.update_index_data(collection, update_query, id).await?;
            }
            Operation::Delete { query, collection } => {
                let id = query.get_object_id("_id")?.to_string();
                elastic.delete_index_data(collection, id).await?;
            }
            _ => {}
        }
    }

    Ok(())
}
