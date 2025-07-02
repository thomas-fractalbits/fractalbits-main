use crate::*;

pub fn bootstrap(
    bucket_name: &str,
    bss_ip: &str,
    nss_ip: &str,
    rss_ip: &str,
    with_bench_client: bool,
) -> CmdResult {
    install_rpms(&["nmap-ncat"])?;
    download_binaries(&[
        "api_server",
        "warp", // for e2e benchmark testing
    ])?;
    create_config(bucket_name, bss_ip, nss_ip, rss_ip)?;
    for ip in [bss_ip, rss_ip, nss_ip] {
        info!("Waiting for node with ip {ip} to be ready");
        while run_cmd!(nc -z $ip 8088).is_err() {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }

    if with_bench_client {
        run_cmd!(echo "127.0.0.1   local-service-endpoint" >>/etc/hosts)?;
        bench_client::bootstrap()?;
    }

    create_systemd_unit_file("api_server", true)?;
    Ok(())
}

fn create_config(bucket_name: &str, bss_ip: &str, nss_ip: &str, rss_ip: &str) -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let config_content = format!(
        r##"bss_addr = "{bss_ip}:8088"
nss_addr = "{nss_ip}:8088"
rss_addr = "{rss_ip}:8088"
region = "{aws_region}"
port = 80
root_domain = ".localhost"

[s3_cache]
s3_host = "http://s3.{aws_region}.amazonaws.com"
s3_port = 80
s3_region = "{aws_region}"
s3_bucket = "{bucket_name}"
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$API_SERVER_CONFIG
    }?;
    Ok(())
}
