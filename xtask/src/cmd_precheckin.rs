use crate::{ServiceName, TEST_BUCKET_ROOT_BLOB_NAME};
use cmd_lib::*;

pub fn run_cmd_precheckin() -> CmdResult {
    crate::cmd_service::stop_service(ServiceName::All)?;
    crate::cmd_service::start_minio_service()?;

    run_cmd! {
        info "Running cargo unit tests ...";
        cargo test --lib --workspace --exclude rewrk --exclude rewrk_rpc --exclude xtask;
    }?;

    run_cmd! {
        info "Building ...";
        zig build 2>&1;
        mkdir -p data/local/meta_cache;
    }?;

    run_cmd! {
        info "Running zig unit tests ...";
        cd data;
        ../zig-out/bin/mkfs;
        zig build test --summary all 2>&1;
    }?;

    let rand_log = "test_art_random.log";
    run_cmd! {
        info "Running art tests (random) with log $rand_log ...";
        cd data;
        ../zig-out/bin/mkfs;
        ../zig-out/bin/test_art --tests random --size 400000 --ops 1000000 --threads 20 &> $rand_log;
    }
    .map_err(|e| {
        run_cmd!(tail $rand_log).unwrap();
        e
    })?;

    let fat_log = "test_art_fat.log";
    run_cmd! {
        info "Running art tests (fat) with log $fat_log ...";
        cd data;
        ../zig-out/bin/mkfs;
        ../zig-out/bin/test_art --tests fat --ops 1000000 &> $fat_log;
    }
    .map_err(|e| {
        run_cmd!(tail $fat_log).unwrap();
        e
    })?;

    let async_art_log = "test_async_art_rename.log";
    run_cmd! {
        info "Running async art rename tests with log $async_art_log ...";
        cd data;
        ../zig-out/bin/mkfs;
        ../zig-out/bin/fbs --new_tree $TEST_BUCKET_ROOT_BLOB_NAME;
        ../zig-out/bin/test_async_art --prefill 100000 --tests rename --ops 10000 --parallelism 1000 --debug  &> $async_art_log;
    }
    .map_err(|e| {
        run_cmd!(tail $async_art_log).unwrap();
        e
    })?;

    let async_art_log = "test_async_art.log";
    run_cmd! {
        info "Running async art tests with log $async_art_log ...";
        cd data;
        ../zig-out/bin/mkfs;
        ../zig-out/bin/fbs --new_tree $TEST_BUCKET_ROOT_BLOB_NAME;
        ../zig-out/bin/test_async_art -p 20 &> $async_art_log;
        ../zig-out/bin/test_async_art -p 20 &>> $async_art_log;
        ../zig-out/bin/test_async_art -p 20 &>> $async_art_log;
    }
    .map_err(|e| {
        run_cmd!(tail $async_art_log).unwrap();
        e
    })?;

    info!("Precheckin is OK");
    crate::cmd_service::stop_service(ServiceName::Minio)?;
    Ok(())
}
