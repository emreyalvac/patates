use crate::types::Result;
use futures::{ready, Stream};
use mongodb::bson::doc;
use mongodb::options::{ClientOptions, CursorType, FindOptions};
use mongodb::{bson::Document, Client, Cursor};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};
use crate::Config;

const OP_LOG_COLLECTION_NAME: &str = "oplog.rs";
const LOCAL_DATABASE_NAME: &str = "local";

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Operation {
    Create {
        collection: String,
    },
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
        message: String,
    },
    Unknown {},
}

impl Operation {
    fn new(doc: Document) -> Result<Operation> {
        let _op = doc.get_str("op")?;
        let _coll = doc.get_str("ns")?;

        println!("{doc:?}");

        Ok(Operation::Unknown {})

        /*match op {
            "c" => Operation::from_create(doc),
            "i" => Operation::from_insert(doc),
            "u" => Operation::from_update(doc),
            "d" => Operation::from_delete(doc),
            "n" => Operation::from_noop(doc),
            _ => ,
        }*/
    }

    fn from_create(doc: Document) -> Result<Operation> {
        let coll = doc.get_str("ns")?.to_lowercase();

        Ok(Operation::Create {
            collection: coll,
        })
    }

    fn from_insert(doc: Document) -> Result<Operation> {
        let coll = doc.get_str("ns")?.to_lowercase();
        let query = doc.get_document("o")?;

        Ok(Operation::Insert {
            collection: coll,
            query: query.to_owned(),
        })
    }

    fn from_update(doc: Document) -> Result<Operation> {
        let coll = doc.get_str("ns")?.to_lowercase();
        let o = doc.get_document("o")?;
        let o2 = doc.get_document("o2")?;
        let diff = o.get_document("diff")?;
        let query = diff.get_document("u")?;

        Ok(Operation::Update {
            collection: coll,
            query: query.to_owned(),
            target_document: o2.to_owned(),
        })
    }

    fn from_delete(doc: Document) -> Result<Operation> {
        let coll = doc.get_str("ns")?.to_lowercase();
        let query = doc.get_document("o")?;

        Ok(Operation::Delete {
            collection: coll,
            query: query.to_owned(),
        })
    }

    fn from_noop(doc: Document) -> Result<Operation> {
        let o = doc.get_document("o")?;
        let message = o.get_str("msg")?;

        Ok(Operation::NoOp {
            message: message.to_string(),
        })
    }
}

#[derive(Debug)]
pub struct OpLog {
    pub cursor: Cursor<Document>,
}

impl OpLog {
    pub async fn new(_config: Config) -> Result<OpLog> {
        let options = ClientOptions::default();
        let client = Client::with_options(options)?;
        Ok(OpLogBuilder::new(&client).await)
    }
}

#[derive(Debug)]
struct OpLogBuilder {}

impl OpLogBuilder {
    async fn new(client: &Client) -> OpLog {
        let coll = client
            .database(LOCAL_DATABASE_NAME)
            .collection::<Document>(OP_LOG_COLLECTION_NAME);

        let mut filter_options = FindOptions::default();
        filter_options.cursor_type = Some(CursorType::TailableAwait);
        filter_options.no_cursor_timeout = Some(true);

        let count = coll.estimated_document_count(None).await.unwrap_or(0);
        filter_options.skip = Some(count);

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
