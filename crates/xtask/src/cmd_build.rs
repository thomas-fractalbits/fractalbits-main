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
    format!("main:{git_branch}-{git_rev}{dirty}, build time: {build_timestamp}")
}

pub fn build_info_for_repo(repo_path: &str) -> String {
    if repo_path == "." || repo_path.is_empty() {
        return build_info();
    }

    // Check if the repo path exists
    if !Path::new(repo_path).exists() {
        return String::new();
    }

    let git_branch = run_fun!(cd $repo_path; git branch --show-current)
        .unwrap_or_else(|_| "unknown".to_string());
    let git_rev = run_fun!(cd $repo_path; git rev-parse --short HEAD)
        .unwrap_or_else(|_| "unknown".to_string());
    let dirty = if run_cmd!(cd $repo_path; git diff-index --quiet HEAD).is_ok() {
        ""
    } else {
        "+"
    };

    // Extract the last component of the repo path for the label
    let repo_name = Path::new(repo_path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(repo_path);

    format!("{repo_name}:{git_branch}-{git_rev}{dirty}")
}

pub fn get_combined_build_info(repo_path: &str) -> String {
    let main_info = BUILD_INFO.get().unwrap();
    if repo_path == "." || repo_path.is_empty() {
        return main_info.clone();
    }

    let repo_info = build_info_for_repo(repo_path);
    if repo_info.is_empty() {
        return main_info.clone();
    }

    // Extract timestamp from main_info
    let timestamp_part = main_info.split(", build time: ").nth(1).unwrap_or("");

    // Extract the main repo part without timestamp
    let main_part = main_info
        .split(", build time: ")
        .next()
        .unwrap_or(main_info);

    // Combine: main:branch-rev/submodule:branch-rev, build time: timestamp
    format!("{main_part}/{repo_info}, build time: {timestamp_part}")
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

    let build_info = get_combined_build_info(ZIG_REPO_PATH);
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
    let release_flag = match mode {
        BuildMode::Release => "--release",
        BuildMode::Debug => "",
    };

    if Path::new("crates/ha").exists() {
        let ha_build_info = get_combined_build_info("crates/ha");
        run_cmd! {
            info "Building nss_role_agent ...";
            BUILD_INFO=$ha_build_info cargo build $release_flag -p nss_role_agent;
        }?;
    }

    if Path::new("crates/root_server").exists() {
        let rs_build_info = get_combined_build_info("crates/root_server");
        run_cmd! {
            info "Building root_server and rss_admin ...";
            BUILD_INFO=$rs_build_info cargo build $release_flag -p root_server -p rss_admin;
        }?;
    }

    match mode {
        BuildMode::Debug => {
            run_cmd! {
                info "Building rust-based servers in debug mode ...";
                BUILD_INFO=$build_info cargo build --workspace
                    --exclude nss_role_agent
                    --exclude root_server
                    --exclude rss_admin
                    --exclude fractalbits-bootstrap
                    --exclude rewrk*;
            }
        }
        BuildMode::Release => {
            run_cmd! {
                info "Building rust-based servers in release mode ...";
                BUILD_INFO=$build_info cargo build --workspace
                    --exclude nss_role_agent
                    --exclude root_server
                    --exclude rss_admin
                    --release;
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
    let build_target = "x86_64-unknown-linux-gnu";
    let build_dir = format!("target/{build_target}/release");
    let ha_build_info = get_combined_build_info("crates/ha");
    let rs_build_info = get_combined_build_info("crates/root_server");
    let rustflags = "-C target-cpu=x86-64-v3 -C opt-level=z -C codegen-units=1 -C strip=symbols";

    run_cmd! {
        info "Building nss_role_agent for x86_64_v3 (size-optimized release mode with zigbuild)...";
        RUSTFLAGS=$rustflags
        BUILD_INFO=$ha_build_info
            cargo zigbuild --release --target $build_target -p nss_role_agent;

        info "Building root_server and rss_admin for x86_64_v3 (size-optimized release mode with zigbuild)...";
        RUSTFLAGS=$rustflags
        BUILD_INFO=$rs_build_info
            cargo zigbuild --release --target $build_target -p root_server -p rss_admin;
    }?;

    let zig_build_info = get_combined_build_info(ZIG_REPO_PATH);
    run_cmd! {
        info "Building Zig binaries for x86_64_v3 (release mode)...";
        cd $ZIG_REPO_PATH;
        zig build -p ../$build_dir/zig-out
            -Dbuild_info=$zig_build_info
            -Doptimize=ReleaseSafe
            -Dtarget=x86_64-linux-gnu
            -Dcpu=x86_64_v3 2>&1;
        info "Zig build complete";
    }?;

    info!("Copying binaries to prebuilt directory...");
    run_cmd!(mkdir -p prebuilt)?;
    for bin in [
        "nss_role_agent",
        "root_server",
        "rss_admin",
        "zig-out/bin/bss_server",
        "zig-out/bin/nss_server",
    ] {
        run_cmd!(cp -f $build_dir/$bin prebuilt/)?;
    }

    run_cmd! {
        info "Stripping debug symbols...";
        find prebuilt -maxdepth 1 -type f -executable ! -name "*.d"
            -exec strip --strip-all "{}" +;
    }?;

    info!("Prebuilt binaries are ready");
    Ok(())
}
