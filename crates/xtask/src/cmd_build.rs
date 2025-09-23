use crate::*;
use std::path::Path;
use std::sync::OnceLock;
use strum::{AsRefStr, EnumString};

pub static BUILD_INFO: OnceLock<String> = OnceLock::new();
pub const ZIG_REPO_PATH: &str = "core";
pub const UI_REPO_PATH: &str = "ui";

#[derive(Copy, Clone, AsRefStr, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum BuildMode {
    Debug,
    Release,
}

pub fn build_mode(release: bool) -> BuildMode {
    match release {
        true => BuildMode::Release,
        false => BuildMode::Debug,
    }
}

pub fn build_info() -> String {
    let git_branch = run_fun!(git branch --show-current).unwrap();
    let git_rev = run_fun!(git rev-parse --short HEAD).unwrap();
    let build_timestamp = run_fun!(date "+%s").unwrap();
    let dirty = if run_cmd!(git diff-index --quiet HEAD).is_ok() {
        ""
    } else {
        "+"
    };
    format!("{git_branch}:{git_rev}{dirty}, build time: {build_timestamp}")
}

pub fn build_bench_rpc() -> CmdResult {
    let build_info = BUILD_INFO.get().unwrap();
    run_cmd! {
        info "Building benchmark tool `rewrk_rpc` ...";
        cd crates/bench_rpc;
        BUILD_INFO=$build_info cargo build --release;
    }
}

pub fn build_zig_servers(mode: BuildMode) -> CmdResult {
    if !Path::new(ZIG_REPO_PATH).exists() {
        return Ok(());
    }

    let build_info = BUILD_INFO.get().unwrap();
    let opts = match mode {
        BuildMode::Debug => "",
        BuildMode::Release => "--release=safe",
    };
    run_cmd! {
        info "Building zig-based servers ...";
        cd $ZIG_REPO_PATH;
        zig build -p ../$ZIG_DEBUG_OUT -Dbuild_info=$build_info $opts 2>&1;
        info "Building bss and nss server done";
    }
}

pub fn build_rust_servers(mode: BuildMode) -> CmdResult {
    let build_info = BUILD_INFO.get().unwrap();
    match mode {
        BuildMode::Debug => {
            run_cmd! {
                info "Building rust-based servers in debug mode ...";
                BUILD_INFO=$build_info cargo build --workspace
                    --exclude fractalbits-bootstrap
                    --exclude rewrk*;
            }
        }
        BuildMode::Release => {
            run_cmd! {
                info "Building rust-based servers in release mode ...";
                BUILD_INFO=$build_info cargo build --release;
            }
        }
    }
}

pub fn build_ui(region: &str) -> CmdResult {
    if !Path::new(UI_REPO_PATH).exists() {
        return Ok(());
    }

    run_cmd! {
        info "Building ui ...";
        cd $UI_REPO_PATH;
        npm install;
        VITE_AWS_REGION=$region npm run build;
    }
}

pub fn build_all(release: bool) -> CmdResult {
    let build_mode = build_mode(release);
    build_rust_servers(build_mode)?;
    build_zig_servers(build_mode)?;
    if release {
        build_bench_rpc()?;
    }
    build_ui(crate::UI_DEFAULT_REGION)?;
    Ok(())
}

pub fn build_prebuilt(_release: bool) -> CmdResult {
    BUILD_INFO.get_or_init(cmd_build::build_info);
    let build_info = BUILD_INFO.get().unwrap();

    // Build Rust binaries with size optimization flags (release mode)
    run_cmd! {
        info "Building Rust binaries for generic x86_64 (size-optimized release mode)...";
        RUSTFLAGS="-C target-cpu=x86-64 -C opt-level=z -C codegen-units=1 -C strip=symbols"
        BUILD_INFO=$build_info
            cargo build --release
            --workspace --exclude fractalbits-bootstrap --exclude rewrk*;
    }?;

    // Build Zig binaries for portable x86_64
    // Note: Using x86_64_v3 which supports 128-bit atomics and is widely compatible
    if Path::new(ZIG_REPO_PATH).exists() {
        run_cmd! {
            info "Building Zig binaries for x86_64_v3 (release mode)...";
            cd $ZIG_REPO_PATH;
            zig build -p ../$ZIG_RELEASE_OUT
                -Dbuild_info=$build_info
                -Doptimize=ReleaseSafe
                -Dtarget=x86_64-linux-gnu
                -Dcpu=x86_64_v3 2>&1;
            info "Zig build complete";
        }?;
    }

    info!("Copying binaries to prebuilt directory...");
    run_cmd!(mkdir -p prebuilt)?;
    for bin in [
        "nss_role_agent",
        "root_server",
        "rss_admin",
        "zig-out/bin/bss_server",
        "zig-out/bin/nss_server",
    ] {
        run_cmd!(cp -f target/release/$bin prebuilt/)?;
    }

    // Strip debug symbols
    run_cmd! {
        info "Stripping debug symbols...";
        find prebuilt -maxdepth 1 -type f -executable ! -name "*.d"
            -exec strip --strip-all "{}" +;
    }?;

    info!("Prebuilt binaries are ready");
    Ok(())
}
