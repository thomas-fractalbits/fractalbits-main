use super::common::*;
use cmd_lib::*;

pub fn bootstrap(meta_stack_testing: bool, for_bench: bool) -> CmdResult {
    install_rpms(&["nvme-cli", "mdadm"])?;
    // no twp support since experiment done
    format_local_nvme_disks(false)?;
    download_binaries(&["bss_server"])?;

    create_coredump_config()?;

    info!("Creating directories for bss_server");
    run_cmd!(mkdir -p "/data/local/stats")?;
    // Create volume directories for multi-BSS support
    // TODO: only create volumes where this node belongs to
    for volume_id in 0..2 {
        // Data volumes
        run_cmd!(mkdir -p data/local/blobs/data_volume$volume_id)?;
        for i in 0..256 {
            run_cmd!(mkdir -p data/local/blobs/data_volume$volume_id/dir$i)?;
        }

        // Metadata volumes
        run_cmd!(mkdir -p data/local/blobs/metadata_volume$volume_id)?;
        for i in 0..256 {
            run_cmd!(mkdir -p data/local/blobs/metadata_volume$volume_id/dir$i)?;
        }
    }

    create_bss_config()?;
    create_systemd_unit_file("bss", true)?;

    if meta_stack_testing || for_bench {
        download_binaries(&["rewrk_rpc"])?;
        xtask_tools::gen_uuids(1_000_000, "/data/uuids.data")?;
    }

    create_logrotate_for_stats()?;
    create_ddb_register_and_deregister_service("bss-server")?;

    run_cmd! {
        info "Syncing file system changes";
        sync;
    }?;

    Ok(())
}

fn create_bss_config() -> CmdResult {
    let num_threads = run_fun!(nproc)?;
    let config_content = format!(
        r##"working_dir = "/data"
server_port = 8088
num_threads = {num_threads}
log_level = "warn"
use_direct_io = true
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$BSS_SERVER_CONFIG;
    }?;

    Ok(())
}
