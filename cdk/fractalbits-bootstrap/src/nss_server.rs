use super::common::*;
use cmd_lib::*;

pub fn bootstrap(bucket_name: &str, volume_id: &str, num_nvme_disks: u32) -> CmdResult {
    if num_nvme_disks != 0 {
        format_local_nvme_disks(num_nvme_disks)?;
    }

    // Sanitize: convert vol-07451bc901d5e1e09 → vol07451bc901d5e1e09
    let volume_id = &volume_id.replace("-", "");
    let service_name = "nss_server";
    for bin in ["nss_server", "mkfs", "format-ebs"] {
        download_binary(bin)?;
    }
    create_nss_config(bucket_name)?;
    create_ebs_mount_unit(volume_id)?;
    create_ebs_udev_rule(volume_id)?;
    create_systemd_unit_file(service_name)?;
    run_cmd! {
        info "Enabling ${service_name}.service";
        systemctl enable ${service_name}.service;
    }?;
    // Note the nss_server service is not started until EBS formatted from root_server
    Ok(())
}

fn create_nss_config(bucket_name: &str) -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let config_content = format!(
        r##"[s3_cache]
s3_host = "s3.{aws_region}.amazonaws.com"
s3_port = 80
s3_region = "{aws_region}"
s3_bucket = "{bucket_name}"
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$NSS_SERVER_CONFIG
    }?;
    Ok(())
}

fn create_ebs_mount_unit(volume_id: &str) -> CmdResult {
    let content = format!(
        r##"[Unit]
Description=Mount EBS Volume at /data

[Mount]
What=/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_{volume_id}
Where=/data
Type=xfs
Options=defaults,nofail

[Install]
WantedBy=multi-user.target
"##
    );
    run_cmd! {
        echo $content > /etc/systemd/system/data.mount;
    }?;

    Ok(())
}

fn create_ebs_udev_rule(volume_id: &str) -> CmdResult {
    let content = format!(
        r##"KERNEL=="nvme*n*", SUBSYSTEM=="block", ENV{{ID_SERIAL}}=="Amazon_Elastic_Block_Store_{volume_id}_1", TAG+="systemd", ENV{{SYSTEMD_WANTS}}="nss_server.service""##
    );
    run_cmd! {
        echo $content > $ETC_PATH/99-ebs.rules;
        ln -s $ETC_PATH/99-ebs.rules /etc/udev/rules.d/;
    }?;

    Ok(())
}

fn format_local_nvme_disks(num_nvme_disks: u32) -> CmdResult {
    const DATA_CACHE_MNT: &str = "/data2/cache";
    run_cmd!(yum install -y nvme-cli mdadm)?;
    let nvme_disks = run_fun! {
        nvme list
            | grep -v "Amazon Elastic Block Store"
            | awk r##"/nvme[0-9]n[0-9]/ {print $1}"##
            | tr '\n' ' '
    }?;
    // FIXME: somehow cmd_lib's $[] is not working here
    let create_md0_cmd = format!(
        "mdadm --create --verbose /dev/md0 --level=0 --raid-devices={num_nvme_disks} {nvme_disks}"
    );
    run_cmd! {
        info "Creating md0 on $nvme_disks";
        bash -c $create_md0_cmd;

        info "Creating XFS on /dev/md0";
        mkfs.xfs /dev/md0;

        info "Mounting to $DATA_CACHE_MNT";
        mkdir -p $DATA_CACHE_MNT;
        mount /dev/md0 $DATA_CACHE_MNT;

        info "Updating /etc/mdadm/mdadm.conf";
        mkdir -p /etc/mdadm;
        mdadm --detail --scan > /etc/mdadm/mdadm.conf;
    }?;

    let md0_uuid = run_fun!(blkid -s UUID -o value /dev/md0)?;
    run_cmd! {
        info "Updating /etc/fstab (md0 uuid=$md0_uuid)";
        echo "UUID=$md0_uuid $DATA_CACHE_MNT xfs defaults,nofail 0 0" >> /etc/fstab;
    }?;

    Ok(())
}
