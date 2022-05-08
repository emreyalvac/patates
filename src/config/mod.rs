use std::fs;
use serde::{Serialize, Deserialize};

use crate::types::Result;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub mongo_url: Option<String>,
    pub change_stream_namespaces: Option<Vec<String>>,
    pub replay: Option<bool>,
}

impl Config {
    pub fn new(path: &str) -> Result<Self> {
        let data = fs::read_to_string(path)?;

        let decoded: Self = toml::from_str(data.as_str()).unwrap();

        Ok(decoded)
    }
}
