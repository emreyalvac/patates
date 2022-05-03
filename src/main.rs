use elasticsearch::indices::IndicesCreateParts;
use crate::database::mongo::OpLog;
use futures::StreamExt;
use mongodb::options::ClientOptions;
use mongodb::Client;
use crate::elastic::ElasticImpl;

mod database;
mod elastic;

static POSTS_INDEX: &'static str = "AAAAAAA";

#[tokio::main]
async fn main() -> mongodb::error::Result<()> {
    let options = ClientOptions::default();
    let con = Client::with_options(options).unwrap();
    let mut op_log = OpLog::new(&con).await;
    let elastic = ElasticImpl::new();

    let index_result = elastic.create_index_if_not_exists(POSTS_INDEX.to_string()).await;

    println!("{index_result:?}");


    while let Some(item) = op_log.next().await {
        // println!("{:?}", item);
    }

    Ok(())
}
