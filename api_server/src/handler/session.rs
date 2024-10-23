use axum::{extract::Request, response::Result};
use std::time::{SystemTime, UNIX_EPOCH};

pub async fn create_session(_request: Request) -> Result<String> {
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).unwrap().as_secs() + 300;
    let body = format!(
        r###"<?xml version="1.0" encoding="UTF-8"?>
<CreateSessionOutput>
   <Credentials>
      <AccessKeyId>string</AccessKeyId>
      <Expiration>{since_the_epoch}</Expiration>
      <SecretAccessKey>string</SecretAccessKey>
      <SessionToken>string</SessionToken>
   </Credentials>
</CreateSessionOutput>"###
    );
    Ok(body.into())
}
