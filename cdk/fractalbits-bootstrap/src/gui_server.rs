use crate::*;

pub fn bootstrap(
    bucket_name: &str,
    bucket_remote_az: Option<&str>,
    nss_endpoint: &str,
    rss_endpoint: &str,
) -> CmdResult {
    install_rpms(&["amazon-cloudwatch-agent", "nmap-ncat", "perf"])?;
    download_binaries(&["api_server"])?;

    let builds_bucket = format!("s3://fractalbits-builds-{}", get_current_aws_region()?);
    run_cmd!(aws s3 cp --no-progress $builds_bucket/ui $WEB_ROOT --recursive)?;

    let bss_service_name = "bss-server.fractalbits.local";
    info!("Waiting for bss with dns name: {bss_service_name}");
    let bss_ip = loop {
        match run_fun!(dig +short $bss_service_name) {
            Ok(ip) if !ip.is_empty() => break ip,
            _ => std::thread::sleep(std::time::Duration::from_secs(1)),
        }
    };
    for (role, endpoint) in [
        ("bss", bss_ip.as_str()),
        ("rss", rss_endpoint),
        ("nss", nss_endpoint),
    ] {
        info!("Waiting for {role} node with endpoint {endpoint} to be ready");
        while run_cmd!(nc -z $endpoint 8088 &>/dev/null).is_err() {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        info!("{role} node can be reached (`nc -z {endpoint} 8088` is ok)");
    }

    api_server::create_config(
        bucket_name,
        bucket_remote_az,
        &bss_ip,
        nss_endpoint,
        rss_endpoint,
    )?;
    // setup_cloudwatch_agent()?;
    let extra_start_opts = format!("--gui {WEB_ROOT}");
    create_systemd_unit_file_with_extra_opts("api_server", &extra_start_opts, false, true)?;

    Ok(())
}
