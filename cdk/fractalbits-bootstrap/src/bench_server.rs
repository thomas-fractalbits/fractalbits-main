use super::common::*;
use cmd_lib::*;

pub fn bootstrap(service_endpoint: &str, clients_ips: Vec<String>) -> CmdResult {
    download_binaries(&["warp"])?;
    create_workload_config(service_endpoint, clients_ips)?;
    create_systemd_unit_file("bench_server", false)?;
    Ok(())
}

fn create_workload_config(service_endpoint: &str, clients_ips: Vec<String>) -> CmdResult {
    let mut warp_clients_str = String::new();
    for ip in clients_ips {
        warp_clients_str.push_str(&format!("  - {}:7761\n", ip));
    }

    let config_content = format!(
        r##"benchmark: mixed
host: {service_endpoint}
access-key: test_api_key
secret-key: test_api_secret
bucket: warp-benchmark-bucket
warp-client:
{}duration: 10m
obj.size: 4KB
concurrent: 50
autoterm: true
"##,
        warp_clients_str
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$BENCH_SERVER_WORKLOAD_CONFIG;
    }?;
    Ok(())
}
