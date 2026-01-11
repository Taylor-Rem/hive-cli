use anyhow::{Result, Context};
use std::env;

pub fn retrieve_from_env(key: &str) -> Result<String> {
    dotenvy::dotenv().ok();
    env::var(key).with_context(|| format!("Missing environment variable: {}", key))
}