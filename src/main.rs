use elasticsearch::indices::IndicesCreateParts;
use crate::database::mongo::{Operation, OpLog};
use futures::StreamExt;
use mongodb::options::ClientOptions;
use mongodb::Client;
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

    // let index_result = elastic.create_index_if_not_exists(POSTS_INDEX.to_string()).await;

    // println!("{index_result:?}");


    while let Some(item) = op_log.next().await {
        let res = item.unwrap();

        match res {
            Operation::Insert { query, collection } => {
                println!("insert {} {}", collection, query)
            }
            Operation::Update { query, collection, target_document } => {
                println!("update {} {}", collection, query)
            }
            _ => {}
        }
    }

    Ok(())
}
