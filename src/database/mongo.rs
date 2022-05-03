use futures::{ready, Stream};
use mongodb::error::Result;
use mongodb::options::{CursorType, FindOptions};
use mongodb::{bson::Document, Client, Cursor};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};
use mongodb::bson::doc;

const OP_LOG_COLLECTION_NAME: &str = "oplog.rs";

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Operation {
    Insert {},
    Update {},
    Delete {},
    Unknown {},
    NoOp {},
}


impl Operation {
    fn new(doc: Document) -> Result<Operation> {
        let op = doc.get_str("op").unwrap();
        match op {
            "i" => Ok(Operation::Insert {}),
            "u" => Ok(Operation::Update {}),
            "d" => Ok(Operation::Delete {}),
            "n" => Ok(Operation::NoOp {}),
            _ => Ok(Operation::Unknown {}),
        }
    }
}

#[derive(Debug)]
pub struct OpLog {
    pub cursor: Cursor<Document>,
}

impl OpLog {
    pub async fn new(client: &Client) -> OpLog {
        OpLogBuilder::new(client).await
    }
}

#[derive(Debug)]
struct OpLogBuilder {}

impl OpLogBuilder {
    async fn new(client: &Client) -> OpLog {
        let coll = client
            .database("local")
            .collection::<Document>(OP_LOG_COLLECTION_NAME);
        let mut filter_options = FindOptions::default();
        filter_options.cursor_type = Some(CursorType::TailableAwait);
        filter_options.no_cursor_timeout = Some(true);

        let exclude_default_collections = doc! {"ns": {"$ne": "config.system.sessions"}};

        let find = coll.find(exclude_default_collections, filter_options).await;

        OpLog {
            cursor: find.unwrap(),
        }
    }
}

impl Stream for OpLog {
    type Item = Result<Operation>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            let ret = if let Some(res) = ready!(Pin::new(&mut this.cursor).poll_next(cx)) {
                match res {
                    Ok(doc) => match Operation::new(doc) {
                        Ok(res) => Some(Ok(res)).into(),
                        Err(err) => Some(Err(err)).into(),
                    },
                    Err(e) => Some(Err(e.into())).into(),
                }
            } else {
                None.into()
            };

            break ret;
        }
    }
}
