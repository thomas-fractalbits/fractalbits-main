mod yaml_get;
mod yaml_mixed;
mod yaml_put;

use super::common::*;
use cmd_lib::*;
use {yaml_get::*, yaml_mixed::*, yaml_put::*};

struct WorkloadConfig {
    size_kb: usize,
    put_concurrent_ops: usize,
    get_concurrent_ops: usize,
    mixed_concurrent_ops: usize,
}

const WORKLOAD_CONFIGS: &[WorkloadConfig] = &[
    WorkloadConfig {
        size_kb: 4,
        put_concurrent_ops: 48,
        get_concurrent_ops: 96,
        mixed_concurrent_ops: 72,
    },
    WorkloadConfig {
        size_kb: 64,
        put_concurrent_ops: 12,
        get_concurrent_ops: 12,
        mixed_concurrent_ops: 12,
    },
];

pub fn bootstrap(
    rss_endpoint: String,
    api_server_endpoint: String,
    bench_client_num: usize,
) -> CmdResult {
    install_rpms(&["nmap-ncat"])?;
    download_binaries(&["warp"])?;
    setup_serial_console_password()?;

    let client_ips = get_service_ips("bench-client", bench_client_num);

    let region = get_current_aws_region()?;
    let mut warp_client_ips = String::new();
    for ip in client_ips.iter() {
        warp_client_ips.push_str(&format!("  - {ip}:7761\n"));
    }

    for config in WORKLOAD_CONFIGS {
        create_put_workload_config(
            &warp_client_ips,
            &region,
            &api_server_endpoint,
            "2m",
            config.size_kb,
            config.put_concurrent_ops,
        )?;
        create_get_workload_config(
            &warp_client_ips,
            &region,
            &api_server_endpoint,
            "2m",
            config.size_kb,
            config.get_concurrent_ops,
        )?;
        create_mixed_workload_config(
            &warp_client_ips,
            &region,
            &api_server_endpoint,
            "2m",
            config.size_kb,
            config.mixed_concurrent_ops,
        )?;
    }

    for (role, endpoint, port) in [
        ("rss", rss_endpoint.as_str(), "8088"),
        ("api_server", api_server_endpoint.as_str(), "80"),
    ] {
        info!("Waiting for {role} endpoint {endpoint} to be ready");
        while run_cmd!(nc -z $endpoint $port &>/dev/null).is_err() {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        info!("{role} endpoint can be reached (`nc -z {endpoint} {port}` is ok)");
    }

    create_bench_start_script(&region, &api_server_endpoint)?;

    Ok(())
}

fn create_bench_start_script(region: &str, api_server_ip: &str) -> CmdResult {
    let script_content = format!(
        r##"#!/bin/bash

export AWS_ACCESS_KEY_ID=test_api_key
export AWS_SECRET_ACCESS_KEY=test_api_secret

set -ex
export AWS_DEFAULT_REGION={region}
export AWS_ENDPOINT_URL_S3=http://{api_server_ip}
bench_bucket=warp-benchmark-bucket

if ! aws s3api head-bucket --bucket $bench_bucket &>/dev/null; then
  aws s3api create-bucket --bucket $bench_bucket
  aws s3api wait bucket-exists --bucket $bench_bucket
  aws s3 ls
fi

/opt/fractalbits/bin/warp run /opt/fractalbits/etc/bench_${{WORKLOAD:-put_4k}}.yml
"##
    );
    run_cmd! {
        mkdir -p $BIN_PATH;
        echo $script_content > $BIN_PATH/$BENCH_SERVER_BENCH_START_SCRIPT;
        chmod +x $BIN_PATH/$BENCH_SERVER_BENCH_START_SCRIPT;
    }?;
    Ok(())
}
