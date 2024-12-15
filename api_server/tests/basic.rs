use aws_sdk_s3::config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config};
use cmd_lib::*;

const DEFAULT_PORT: u16 = 3000;

macro_rules! assert_bytes_eq {
    ($stream:expr, $bytes:expr) => {
        let data = $stream
            .collect()
            .await
            .expect("Error reading data")
            .into_bytes();

        assert_eq!(data.as_ref(), $bytes);
    };
}

#[tokio::test]
async fn test_basic() {
    let s3_client = build_client();
    let bucket: String = "mybucket".into();
    let key = "hello";
    let value = b"42";
    let data = ByteStream::from_static(value);

    start_service();
    s3_client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(data)
        .send()
        .await
        .unwrap();

    let res = s3_client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();

    assert_bytes_eq!(res.body, value);
    stop_service();
}

fn start_service() {
    run_cmd!(cd ..; cargo xtask service restart).unwrap();
}

fn stop_service() {
    run_cmd!(cd ..; cargo xtask service stop).unwrap();
}

fn build_client() -> Client {
    let config = Config::builder()
        .endpoint_url(format!("http://127.0.0.1:{}", DEFAULT_PORT))
        .region(Region::from_static("fractalbits-integ-tests"))
        // .credentials_provider(credentials)
        .behavior_version(BehaviorVersion::v2024_03_28())
        .build();

    Client::from_conf(config)
}
