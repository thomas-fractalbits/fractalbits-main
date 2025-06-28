use super::common::*;
use cmd_lib::*;

pub fn bootstrap(bucket_name: &str, bss_ip: &str, nss_ip: &str, rss_ip: &str) -> CmdResult {
    download_binaries(&[
        "api_server",
        "warp", // for e2e benchmark testing
    ])?;
    create_config(bucket_name, bss_ip, nss_ip, rss_ip)?;
    create_systemd_unit_file("api_server", true)?;
    Ok(())
}

fn create_config(bucket_name: &str, bss_ip: &str, nss_ip: &str, rss_ip: &str) -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let config_content = format!(
        r##"bss_addr = "{bss_ip}:9225"
nss_addr = "{nss_ip}:9224"
rss_addr = "{rss_ip}:8888"
region = "{aws_region}"
port = 3000
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
