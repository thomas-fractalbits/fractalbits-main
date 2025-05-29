use clap::Parser;
use cmd_lib::*;

#[derive(Parser)]
#[clap(
    name = "fractalbits-bootstrap",
    about = "Bootstrap for cloud ec2 instances"
)]
enum Cmd {
    ApiServer,
    BssServer,
    NssServer,
    RootServer,
}

#[cmd_lib::main]
fn main() -> CmdResult {
    match Cmd::parse() {
        Cmd::ApiServer => bootstrap_api_server(),
        Cmd::BssServer => bootstrap_bss_server(),
        Cmd::NssServer => bootstrap_nss_server(),
        Cmd::RootServer => bootstrap_root_server(),
    }
}

fn bootstrap_api_server() -> CmdResult {
    info!("bootstrapping api_server ...");
    Ok(())
}

fn bootstrap_bss_server() -> CmdResult {
    info!("bootstrapping bss_server ...");
    Ok(())
}

fn bootstrap_nss_server() -> CmdResult {
    info!("bootstrapping nss_server ...");
    Ok(())
}

fn bootstrap_root_server() -> CmdResult {
    info!("bootstrapping root_server ...");
    Ok(())
}
