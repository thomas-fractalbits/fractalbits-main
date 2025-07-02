use super::common::*;
use cmd_lib::*;

pub fn bootstrap(service_endpoint: String, client_ips: Vec<String>) -> CmdResult {
    download_binaries(&["warp"])?;
    create_workload_config(&service_endpoint, &client_ips)?;
    if service_endpoint == "local-service-endpoint" {
        for client_ip in client_ips {
            run_cmd!(echo "$client_ip $service_endpoint" >>/etc/hosts)?;
        }
    }
    create_bench_start_script()?;
    Ok(())
}

fn create_workload_config(service_endpoint: &str, client_ips: &Vec<String>) -> CmdResult {
    let mut warp_clients_str = String::new();
    for ip in client_ips {
        warp_clients_str.push_str(&format!("  - {}:7761\n", ip));
    }

    let region = get_current_aws_region()?;
    let config_content = format!(
        r##"warp:
  api: v1
  benchmark: put
  warp-client:
{warp_clients_str}
  remote:
    region: {region}
    access-key: test_api_key
    secret-key: test_api_secret
    host: {service_endpoint}
    tls: false
    bucket: warp-benchmark-bucket
  params:
    duration: 1m
    concurrent: 16
    obj:
      size: 4KiB
      rand-size: false
    autoterm:
      enabled: false
      dur: 10s
      pct: 7.5
    no-clear: false
    keep-data: false
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$BENCH_SERVER_WORKLOAD_CONFIG;
        echo $config_content > $ETC_PATH/$BENCH_SERVER_WORKLOAD_CONFIG.orig;
    }?;
    Ok(())
}

fn create_bench_start_script() -> CmdResult {
    let script_content = r##"#!/bin/bash
set -ex

CONF=/opt/fractalbits/etc/bench_workload.yaml
WARP=/opt/fractalbits/bin/warp
region=$(cat $CONF | grep region: | awk '{{print $2}}')
bucket=$(cat $CONF | grep bucket: | awk '{{print $2}}')
host=$(cat $CONF | grep host: | awk '{{print $2}}')
export AWS_DEFAULT_REGION=$region
export AWS_ENDPOINT_URL_S3=http://$host
export AWS_ACCESS_KEY_ID=test_api_key
export AWS_SECRET_ACCESS_KEY=test_api_secret

if ! aws s3api head-bucket --bucket $bucket &>/dev/null; then
  aws s3api create-bucket --bucket $bucket
fi

$WARP run $CONF
"##;
    run_cmd! {
        mkdir -p $BIN_PATH;
        echo $script_content > $BIN_PATH/$BENCH_SERVER_BENCH_START_SCRIPT;
        chmod +x $BIN_PATH/$BENCH_SERVER_BENCH_START_SCRIPT;
    }?;
    Ok(())
}
