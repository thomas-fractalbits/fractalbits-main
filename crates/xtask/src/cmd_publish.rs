use crate::*;
use cmd_build::{BUILD_INFO, ZIG_REPO_PATH};
use cmd_lib::*;
use std::path::Path;

pub fn run_build_and_strip() -> CmdResult {
    BUILD_INFO.get_or_init(cmd_build::build_info);
    let build_info = BUILD_INFO.get().unwrap();

    // Step 1: Build Rust binaries with size optimization flags (debug mode)
    run_cmd! {
        info "Building Rust binaries for generic x86_64 (size-optimized debug mode)...";
        RUSTFLAGS="-C target-cpu=x86-64 -C opt-level=z -C codegen-units=1" BUILD_INFO=$build_info cargo build --workspace
            --exclude fractalbits-bootstrap
            --exclude rewrk*;
    }?;

    // Step 2: Build Zig binaries for portable x86_64
    // Note: Using x86_64_v3 which supports 128-bit atomics and is widely compatible
    if Path::new(ZIG_REPO_PATH).exists() {
        run_cmd! {
            info "Building Zig binaries for x86_64_v3 (debug mode)...";
            cd $ZIG_REPO_PATH;
            zig build -p ../$ZIG_DEBUG_OUT -Dbuild_info=$build_info
                -Dtarget=x86_64-linux-gnu -Dcpu=x86_64_v3 2>&1;
            info "Zig build complete";
        }?;
    }

    // Step 3: Strip debug symbols from Rust binaries
    run_cmd! {
        info "Stripping debug symbols from Rust binaries...";
        find target/debug -maxdepth 1 -type f -executable ! -name "*.d"
            -exec strip --strip-all "{}" +;
    }?;

    // Step 4: Strip debug symbols from Zig binaries
    if Path::new("target/debug/zig-out/bin").exists() {
        run_cmd! {
            info "Stripping debug symbols from Zig binaries...";
            find target/debug/zig-out/bin -type f -executable
                -exec strip --strip-all "{}" +;
        }?;
    }

    info!("Build and strip complete");
    Ok(())
}

pub fn run_git_add() -> CmdResult {
    info!("Adding pre-built binaries into git");
    for bin in [
        "nss_role_agent",
        "root_server",
        "rss_admin",
        "zig-out/bin/bss_server",
        "zig-out/bin/nss_server",
    ] {
        run_cmd!(git add -f target/debug/$bin)?;
    }

    info!("Pre-built binaries added to git");
    Ok(())
}
