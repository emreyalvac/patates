use crate::elastic::ElasticImpl;
use crate::oplog::{OpLog, Operation};
use crate::types::Result;
use config::Config;
use futures::StreamExt;
use serde_json::json;

mod config;
mod elastic;
mod oplog;
mod types;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::new("patates.toml")?;
    let mut op_log = OpLog::new(config.clone()).await?;
    let elastic = ElasticImpl::new(config.clone())?;

    println!("{config:?}");

    while let Some(item) = op_log.next().await {
        let res = item?;

        match res {
            Operation::Create { collection } => {
                elastic.create_index_if_not_exists(collection).await;
            }
            Operation::Insert { query, collection } => {
                let mut ser = serde_json::to_value(&query)?;
                let id = query.get_object_id("_id")?.to_string();
                ser.as_object_mut().unwrap().remove("_id");
                elastic.insert_index_data(collection, ser, id).await?;
            }
            Operation::Update {
                query,
                collection,
                target_document,
            } => {
                let ser = serde_json::to_value(&query)?;
                let update_query = json!({ "doc": ser });
                let id = target_document.get_object_id("ss")?.to_string();
                elastic
                    .update_index_data(collection, update_query, id)
                    .await?;
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
