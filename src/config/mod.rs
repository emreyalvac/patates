use std::fs;
use serde::{Serialize, Deserialize};

use crate::types::Result;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub mongo_url: Option<String>,
    pub change_stream_namespaces: Option<Vec<String>>,
    pub replay: Option<bool>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mongo_url: Some("mongodb://127.0.0.1:27017".to_owned()),
            change_stream_namespaces: Some(vec![]),
            replay: Some(false),
        }
    }
}

impl Config {
    pub fn new(path: &str) -> Result<Self> {
        return match fs::read_to_string(path) {
            Ok(data) => {
                let decoded: Self = toml::from_str(data.as_str()).unwrap();
                Ok(decoded)
            }
            Err(_) => {
                println!("patates: Config file (patates.toml) not found. Default values initialized.");

                Ok(Self::default())
            }
        };
    }
}
