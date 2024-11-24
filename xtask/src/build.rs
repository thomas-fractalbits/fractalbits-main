use cmd_lib::*;

pub enum BuildMode {
    Debug,
    Release,
}

pub fn build_rewrk() -> CmdResult {
    run_cmd! {
        info "Building benchmark tool `rewrk` ...";
        cd ./api_server/benches/rewrk;
        cargo build --release;
    }
}

pub fn build_rewrk_rpc() -> CmdResult {
    run_cmd! {
        info "Building benchmark tool `rewrk_rpc` ...";
        cd ./api_server/benches/rewrk_rpc;
        cargo build --release;
    }
}

pub fn build_bss_nss_server(mode: BuildMode) -> CmdResult {
    let opts = match mode {
        BuildMode::Debug => "",
        BuildMode::Release => "--release=safe",
    };
    run_cmd! {
        info "Building bss and nss server ...";
        zig build $opts;
    }
}

pub fn build_api_server(mode: BuildMode) -> CmdResult {
    let opts = match mode {
        BuildMode::Debug => "",
        BuildMode::Release => "--release",
    };
    run_cmd! {
        info "Building api_server ...";
        cd api_server;
        cargo build $opts;
    }
}
