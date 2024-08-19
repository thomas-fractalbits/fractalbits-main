#![deny(warnings)]

use api_server::{nss_get_inode, nss_ops, nss_put_inode};
use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{body::Body, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use prost::Message;
use std::net::SocketAddr;
use tokio::net::TcpListener;

/// This is our service handler. It receives a Request, routes on its
/// path, and returns a Future of a Response.
async fn echo(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/put_obj") => {
            // To protect our server, reject requests with bodies larger than
            // 64kbs of data.
            let max = req.body().size_hint().upper().unwrap_or(u64::MAX);
            if max > 1024 * 64 {
                let mut resp = Response::new(full("Body too big"));
                *resp.status_mut() = hyper::StatusCode::PAYLOAD_TOO_LARGE;
                return Ok(resp);
            }

            let key = req.collect().await?.to_bytes();
            let value = key.iter().rev().cloned().collect::<Vec<u8>>();
            nss_put_inode(
                std::str::from_utf8(&key).unwrap().to_string(),
                std::str::from_utf8(&value).unwrap().to_string(),
            )
            .await;
            Ok(Response::new(empty()))
        }

        (&Method::POST, "/get_obj") => {
            // To protect our server, reject requests with bodies larger than
            // 64kbs of data.
            let max = req.body().size_hint().upper().unwrap_or(u64::MAX);
            if max > 1024 * 64 {
                let mut resp = Response::new(full("Body too big"));
                *resp.status_mut() = hyper::StatusCode::PAYLOAD_TOO_LARGE;
                return Ok(resp);
            }

            let key = req.collect().await?.to_bytes();
            let resp = nss_get_inode(std::str::from_utf8(&key).unwrap().to_string())
                .await
                .result;
            if let Some(nss_ops::get_inode_response::Result::Ok(value)) = resp {
                let value_body = value.encode_to_vec();
                Ok(Response::new(full(value_body)))
            } else {
                todo!()
            }
        }

        // Return the 404 Not Found for other routes.
        _ => {
            let mut not_found = Response::new(empty());
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

fn empty() -> BoxBody<Bytes, hyper::Error> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, hyper::Error> {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(io, service_fn(echo))
                .await
            {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}
