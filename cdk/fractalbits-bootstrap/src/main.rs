use clap::Parser;
use cmd_lib::*;
use strum::{AsRefStr, EnumString};

#[derive(Parser)]
#[clap(
    name = "fractalbits-bootstrap",
    about = "Bootstrap for cloud ec2 instances"
)]
enum Cmd {
    #[command(name = "api_server")]
    ApiServer,
    #[command(name = "bss_server")]
    BssServer,
    #[command(name = "nss_server")]
    NssServer,
    #[command(name = "root_server")]
    RootServer,
}

const BUILDS_BUCKET: &str = "s3://fractalbits-builds";
const BIN_PATH: &str = "/opt/fractalbits/bin/";
const ETC_PATH: &str = "/opt/fractalbits/etc/";
const NSS_SERVER_CONFIG: &str = "nss_server_cloud_config.toml";
const API_SERVER_CONFIG: &str = "api_server_cloud_config.toml";

#[derive(AsRefStr, EnumString, Copy, Clone)]
#[strum(serialize_all = "snake_case")]
enum ServiceName {
    ApiServer,
    BssServer,
    NssServer,
    RootServer,
}

#[cmd_lib::main]
fn main() -> CmdResult {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_target(false)
        .init();

    match Cmd::parse() {
        Cmd::ApiServer => bootstrap_api_server(),
        Cmd::BssServer => bootstrap_bss_server(),
        Cmd::NssServer => bootstrap_nss_server(),
        Cmd::RootServer => bootstrap_root_server(),
    }
}

fn bootstrap_api_server() -> CmdResult {
    info!("Bootstrapping api_server ...");
    let service = ServiceName::ApiServer;
    download_binary(service.as_ref())?;
    create_api_server_cloud_config()?;
    create_systemd_unit_file(service)?;
    run_cmd! {
        info "Sleep 10s to wait for other ec2 instances";
        sleep 10;
        info "Starting api_server.service";
        systemctl start api_server.service;
    }?;
    Ok(())
}

fn bootstrap_bss_server() -> CmdResult {
    info!("Bootstrapping bss_server ...");
    let service = ServiceName::BssServer;
    download_binary(service.as_ref())?;
    create_systemd_unit_file(service)?;
    run_cmd! {
        info "Starting bss_server.service";
        systemctl start bss_server.service;
    }?;
    Ok(())
}

fn bootstrap_nss_server() -> CmdResult {
    info!("Bootstrapping nss_server ...");

    download_binary("mkfs")?;
    run_cmd! {
        mkdir -p /var/data;
        cd /var/data;
        $BIN_PATH/mkfs;
    }?;

    let service = ServiceName::NssServer;
    download_binary(service.as_ref())?;
    create_nss_server_cloud_config()?;
    create_systemd_unit_file(service)?;
    run_cmd! {
        info "Starting nss_server.service";
        systemctl start nss_server.service;
    }?;
    Ok(())
}

fn bootstrap_root_server() -> CmdResult {
    info!("Bootstrapping root_server ...");

    // root_server requires etcd service running
    download_binary("etcd")?;
    start_etcd_service()?;

    download_binary("rss_admin")?;
    run_cmd!($BIN_PATH/rss_admin api-key init-test)?;

    let service = ServiceName::RootServer;
    download_binary(service.as_ref())?;
    create_systemd_unit_file(service)?;
    run_cmd! {
        info "Starting root_server.service";
        systemctl start root_server.service;
    }?;
    Ok(())
}

fn download_binary(file_name: &str) -> CmdResult {
    run_cmd! {
        info "Downloading $file_name from $BUILDS_BUCKET to $BIN_PATH ...";
        aws s3 cp --no-progress $BUILDS_BUCKET/$file_name $BIN_PATH;
        chmod +x $BIN_PATH/$file_name
    }?;
    Ok(())
}

fn create_systemd_unit_file(service: ServiceName) -> CmdResult {
    let service_name = service.as_ref();
    let (requires, exec_start) = match service {
        ServiceName::ApiServer => (
            "",
            format!("{BIN_PATH}{service_name} -c {ETC_PATH}{API_SERVER_CONFIG}"),
        ),
        ServiceName::NssServer => (
            "",
            format!("{BIN_PATH}{service_name} -c {ETC_PATH}{NSS_SERVER_CONFIG}"),
        ),
        ServiceName::BssServer => ("", format!("{BIN_PATH}{service_name}")),
        ServiceName::RootServer => ("etcd.service", format!("{BIN_PATH}{service_name}")),
    };
    let systemd_unit_content = format!(
        r##"[Unit]
Description={service_name} Service
Requires={requires}
After={requires}

[Service]
LimitNOFILE=1000000
LimitCORE=infinity
WorkingDirectory=/var/data
ExecStart={exec_start}

[Install]
WantedBy=multi-user.target
"##
    );
    let service_file = format!("{service_name}.service");

    run_cmd! {
        mkdir -p /var/data;
        mkdir -p $ETC_PATH;
        echo $systemd_unit_content > ${ETC_PATH}${service_file};
        info "Linking ${ETC_PATH}${service_file} into /etc/systemd/system";
        systemctl link ${ETC_PATH}${service_file} --force --quiet;
    }?;
    Ok(())
}

fn start_etcd_service() -> CmdResult {
    let service_file = format!("{ETC_PATH}etcd.service");
    let service_file_content = format!(
        r##"[Unit]
Description=etcd for root_server

[Install]
WantedBy=default.target

[Service]
Type=simple
ExecStart="{BIN_PATH}etcd"
Restart=always
WorkingDirectory=/var/data
"##
    );

    run_cmd! {
        mkdir -p /var/data;
        mkdir -p $ETC_PATH;
        echo $service_file_content > $service_file;
        info "Linking $service_file into /etc/systemd/system";
        systemctl link $service_file --force --quiet;
        info "Starting etcd.service";
        systemctl start etcd.service;
    }?;

    Ok(())
}

fn create_api_server_cloud_config() -> CmdResult {
    let config_content = r##"bss_addr = "10.0.1.10:9225"
nss_addr = "10.0.1.100:9224"
rss_addr = "10.0.1.254:8888"
region = "us-west-1"
port = 3000
root_domain = ".localhost"

[s3_cache]
s3_host = "http://s3.us-west-1.amazonaws.com"
s3_port = 80
s3_region = "us-west-1"
s3_bucket = "fractalbits-bucket"
"##;
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$API_SERVER_CONFIG
    }?;
    Ok(())
}

fn create_nss_server_cloud_config() -> CmdResult {
    let config_content = r##"[s3_cache]
s3_host = "s3.us-west-1.amazonaws.com"
s3_port = 80
s3_region = "us-west-1"
s3_bucket = "fractalbits-bucket"
"##;
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$NSS_SERVER_CONFIG
    }?;
    Ok(())
}
