use crate::*;

pub fn bootstrap(
    bucket: Option<&str>,
    nss_endpoint: &str,
    rss_endpoint: &str,
    remote_az: Option<&str>,
    for_bench: bool,
) -> CmdResult {
    download_binaries(&["api_server"])?;

    let is_multi_az = remote_az.is_some();
    for (role, endpoint) in [("rss", rss_endpoint), ("nss", nss_endpoint)] {
        info!("Waiting for {role} node {endpoint} to be ready");
        while run_cmd!(nc -z $endpoint 8088 &>/dev/null).is_err() {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        info!("{role} node can be reached (`nc -z {endpoint} 8088` is ok)");
    }

    // For S3 Express multi-az setup, only wait for RSS and NSS
    if is_multi_az {
        for (role, ip) in [("rss", rss_endpoint), ("nss", nss_endpoint)] {
            info!("Waiting for {role} node {ip} to be ready");
            while run_cmd!(nc -z $ip 8088 &>/dev/null).is_err() {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            info!("{role} node can be reached (`nc -z {ip} 8088` is ok)");
        }
    }

    create_config(bucket, nss_endpoint, rss_endpoint, remote_az)?;

    if for_bench {
        // Try to download tools for micro-benchmarking
        download_binaries(&["rewrk_rpc", "test_art"])?;
    }

    configure_ena_interrupts()?;

    // setup_cloudwatch_agent()?;
    create_systemd_unit_file("api_server", true)?;
    create_ddb_register_and_deregister_service("api-server")?;

    Ok(())
}

pub fn create_config(
    bucket: Option<&str>,
    nss_endpoint: &str,
    rss_endpoint: &str,
    remote_az: Option<&str>,
) -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let num_cores = run_fun!(nproc)?;
    let config_content = if let Some(remote_az) = remote_az {
        // S3 Express Multi-AZ configuration
        let local_az = get_current_aws_az_id()?;
        let local_bucket = get_s3_express_bucket_name(&local_az)?;
        let remote_bucket = get_s3_express_bucket_name(remote_az)?;

        format!(
            r##"nss_addr = "{nss_endpoint}:8088"
rss_addr = "{rss_endpoint}:8088"
nss_conn_num = {num_cores}
rss_conn_num = 1
bss_conn_num = {num_cores}
region = "{aws_region}"
port = 80
mgmt_port = 18088
root_domain = ".localhost"
with_metrics = true
http_request_timeout_seconds = 5
rpc_timeout_seconds = 4
allow_missing_or_bad_signature = false

[https]
enabled = false
port = 443
cert_file = "/opt/fractalbits/etc/cert.pem"
key_file = "/opt/fractalbits/etc/key.pem"
force_http1_only = false

[blob_storage]
backend = "s3_express_multi_az"

[blob_storage.s3_express_multi_az]
local_az_host = "http://s3.{aws_region}.amazonaws.com"
local_az_port = 80
remote_az_host = "http://s3.{aws_region}.amazonaws.com"
remote_az_port = 80
s3_region = "{aws_region}"
local_az_bucket = "{local_bucket}"
remote_az_bucket = "{remote_bucket}"
local_az = "{local_az}"
remote_az = "{remote_az}"

[blob_storage.s3_express_multi_az.ratelimit]
enabled = false
put_qps = 7000
get_qps = 10000
delete_qps = 5000

[blob_storage.s3_express_multi_az.retry_config]
enabled = false
max_attempts = 15
initial_backoff_us = 50
max_backoff_us = 500
backoff_multiplier = 1.0
"##
        )
    } else {
        // Hybrid single az configuration
        let bucket_name =
            bucket.ok_or_else(|| std::io::Error::other("Bucket name required for hybrid mode"))?;
        format!(
            r##"nss_addr = "{nss_endpoint}:8088"
rss_addr = "{rss_endpoint}:8088"
nss_conn_num = {num_cores}
rss_conn_num = 1
bss_conn_num = {num_cores}
region = "{aws_region}"
port = 80
mgmt_port = 18088
root_domain = ".localhost"
with_metrics = true
http_request_timeout_seconds = 5
rpc_timeout_seconds = 4
allow_missing_or_bad_signature = false

[https]
enabled = false
port = 443
cert_file = "/opt/fractalbits/etc/cert.pem"
key_file = "/opt/fractalbits/etc/key.pem"
force_http1_only = false

[blob_storage]
backend = "s3_hybrid_single_az"

[blob_storage.s3_hybrid_single_az]
s3_host = "http://s3.{aws_region}.amazonaws.com"
s3_port = 80
s3_region = "{aws_region}"
s3_bucket = "{bucket_name}"

[blob_storage.s3_hybrid_single_az.ratelimit]
enabled = false
put_qps = 7000
get_qps = 10000
delete_qps = 5000

[blob_storage.s3_hybrid_single_az.retry_config]
enabled = true
max_attempts = 8
initial_backoff_us = 15000
max_backoff_us = 2000000
backoff_multiplier = 1.8
"##
        )
    };

    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$API_SERVER_CONFIG
    }?;
    Ok(())
}

fn configure_ena_interrupts() -> CmdResult {
    info!("Configuring ENA interrupt affinity");

    run_cmd! {
        info "Disabled irqbalance";
        systemctl disable --now irqbalance
    }?;

    let iface =
        run_fun!(grep -o r"ens[0-9]*-Tx-Rx" /proc/interrupts | head -1 | sed r"s/-Tx-Rx//")?;
    info!("Detected ENA interface: {}", iface);

    let num_queues = run_fun!(grep "$iface-Tx-Rx-" /proc/interrupts | wc -l)?
        .parse()
        .unwrap_or_else(|_| panic!("failed to get queues number of ENA {iface}"));
    info!("Found {} queues", num_queues);

    let num_cpus = num_cpus()?;
    info!("System has {} CPUs", num_cpus);

    for queue in 0..num_queues {
        let pattern = format!("{}-Tx-Rx-{}", iface, queue);
        let irq = run_fun!(grep $pattern /proc/interrupts | awk -F: r"{print $1}" | tr -d " ")?;

        if !irq.is_empty() {
            let cpu = queue % num_cpus;
            let mask = format!("{:x}", 1u64 << cpu);

            run_cmd! {
                echo $mask > /proc/irq/$irq/smp_affinity;
                info "IRQ ${irq} (${iface}-Tx-Rx-${queue}) -> CPU ${cpu} (mask: 0x${mask})";
            }?;
        }
    }

    let mgmt_irq = run_fun! {
        grep -E "ena-mgmnt|${iface}-mgmnt" /proc/interrupts
            | awk -F: r"{print $1}"
            | tr -d " "
            | head -1
    }?;
    if !mgmt_irq.is_empty() {
        run_cmd! {
            echo 1 > /proc/irq/${mgmt_irq}/smp_affinity;
            info "Management IRQ ${mgmt_irq} -> CPU 0";
        }?;
    }

    info!("Done! Current ENA IRQ affinity:");
    for queue in 0..num_queues {
        let irq = run_fun! {
            grep "${iface}-Tx-Rx-${queue}" /proc/interrupts
                | awk -F: r"{print $1}"
                | tr -d " "
        }?;
        if !irq.is_empty() {
            let affinity_path = format!("/proc/irq/{}/smp_affinity", irq);
            let affinity = run_fun!(cat $affinity_path)?;
            info!("Queue {queue} (IRQ {irq}): affinity mask = 0x{affinity}");
        }
    }

    Ok(())
}
