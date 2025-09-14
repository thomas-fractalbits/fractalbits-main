use fake::{Fake, StringFaker};
use std::collections::HashMap;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let url = "http://localhost:8080";
    let client = reqwest::Client::new();
    let total = 2 * 1024 * 1024;
    let mut kvs = HashMap::with_capacity(total);

    // Put key&value pairs
    let before = Instant::now();
    // from https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
    const ASCII: &str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!-_.*'()";
    let f = StringFaker::with(Vec::from(ASCII), 4..30);
    for i in 0..total {
        let key: String = f.fake();
        let value: String = f.fake();
        // println!("inserting key={key}, value={value}");
        let _res = client
            .post(format!("{url}/{key}"))
            .body(format!("{value}"))
            .send()
            .await
            .unwrap();
        kvs.insert(key, value);
        if i != 0 && i % 10000 == 0 {
            println!("inserted {i} kv pairs");
        }
    }
    let elapsed = before.elapsed();
    println!(
        "Elapsed time: {:.2?}, tps: {:.4?}",
        elapsed,
        (total as f64) / elapsed.as_secs_f64()
    );

    let before = Instant::now();
    // Get values back
    let mut i = 0;
    for key in kvs.keys() {
        let _res = client.get(format!("{url}/{key}")).send().await.unwrap();
        if i != 0 && i % 10000 == 0 {
            println!("fetched {i} kv pairs");
        }
        i += 1;
    }
    let elapsed = before.elapsed();
    println!(
        "Elapsed time: {:.2?}, tps: {:.4?}",
        elapsed,
        (total as f64) / elapsed.as_secs_f64()
    );
}
