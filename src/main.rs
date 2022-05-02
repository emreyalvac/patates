use crate::database::mongo::OpLog;
use futures::StreamExt;
use mongodb::options::ClientOptions;
use mongodb::Client;

mod database;

#[tokio::main]
async fn main() -> mongodb::error::Result<()> {
    let options = ClientOptions::default();
    let con = Client::with_options(options).unwrap();
    let mut op_log = OpLog::new(&con).await;

    while let Some(item) = op_log.next().await {
        println!("{:?}", item);
    }

    Ok(())
}
