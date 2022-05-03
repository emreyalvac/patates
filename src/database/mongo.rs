use futures::{ready, Stream};
use mongodb::error::Result;
use mongodb::options::{CursorType, FindOptions};
use mongodb::{bson::Document, Client, Cursor};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};
use mongodb::bson::{DateTime, doc};

const OP_LOG_COLLECTION_NAME: &str = "oplog.rs";

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Operation {
    Insert {
        collection: String,
        query: Document,
    },
    Update {
        collection: String,
        query: Document,
        target_document: Document,
    },
    Delete {
        collection: String,
        query: Document,
    },
    NoOp {
        message: String
    },
    Unknown {},
}


impl Operation {
    fn new(doc: Document) -> Result<Operation> {
        let op = doc.get_str("op").unwrap();
        match op {
            "i" => Operation::from_insert(doc),
            "u" => Operation::from_update(doc),
            "d" => Operation::from_delete(doc),
            "n" => Operation::from_noop(doc),
            _ => Ok(Operation::Unknown {}),
        }
    }

    fn from_insert(doc: Document) -> Result<Operation> {
        let coll = doc.get_str("ns").unwrap();
        let query = doc.get_document("o").unwrap();

        Ok(Operation::Insert { collection: coll.to_string(), query: query.to_owned() })
    }

    fn from_update(doc: Document) -> Result<Operation> {
        let coll = doc.get_str("ns").unwrap();
        let o = doc.get_document("o").unwrap();
        let o2 = doc.get_document("o2").unwrap();
        let diff = o.get_document("diff").unwrap();
        let query = diff.get_document("u").unwrap();

        Ok(Operation::Update { collection: coll.to_string(), query: query.to_owned(), target_document: o2.to_owned() })
    }

    fn from_delete(doc: Document) -> Result<Operation> {
        let coll = doc.get_str("ns").unwrap();
        let query = doc.get_document("o").unwrap();

        Ok(Operation::Delete { collection: coll.to_string(), query: query.to_owned() })
    }

    fn from_noop(doc: Document) -> Result<Operation> {
        let o = doc.get_document("o").unwrap();
        let message = o.get_str("msg").unwrap_or("");

        Ok(Operation::NoOp { message: message.to_string() })
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
        let exclude_default_collections = doc! {"ns": {"$nin": ["config.system.sessions", "admin.system.keys"]}};

        let mut filter_options = FindOptions::default();
        filter_options.cursor_type = Some(CursorType::TailableAwait);
        filter_options.no_cursor_timeout = Some(true);

        let count = coll.estimated_document_count(None).await;
        filter_options.skip = Some(count.unwrap());


        let find = coll.find(None, filter_options).await;

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
