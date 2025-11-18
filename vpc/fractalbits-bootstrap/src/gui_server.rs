use crate::*;

pub fn bootstrap(
    bucket: Option<&str>,
    nss_endpoint: &str,
    remote_az: Option<&str>,
    rss_ha_enabled: bool,
) -> CmdResult {
    download_binaries(&["api_server"])?;
    let builds_bucket = get_builds_bucket()?;
    run_cmd!(aws s3 cp --no-progress $builds_bucket/ui $GUI_WEB_ROOT --recursive)?;

    api_server::create_config(bucket, nss_endpoint, remote_az, rss_ha_enabled)?;
    // setup_cloudwatch_agent()?;
    create_systemd_unit_file("gui_server", true)?;

    Ok(())
}
