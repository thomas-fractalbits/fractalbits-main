use bytes::Bytes;
use metrics::{counter, histogram};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, sleep};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::blob_storage::{BlobStorageError, S3ClientWrapper};

/// Configuration for S3 Express session token pre-warming
#[derive(Debug, Clone)]
pub struct SessionPrewarmingConfig {
    /// Enable/disable session pre-warming
    pub enabled: bool,
    /// Interval between pre-warming operations (should be shorter than session lifetime)
    pub prewarming_interval: Duration,
    /// Delay before starting pre-warming after service startup
    pub startup_delay: Duration,
    /// Size of dummy objects for pre-warming (small to minimize overhead)
    pub dummy_object_size: usize,
    /// Timeout for pre-warming operations
    pub operation_timeout: Duration,
}

impl Default for SessionPrewarmingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            // Pre-warm every 4 minutes (before 5-minute session expiration)
            prewarming_interval: Duration::from_secs(240),
            // Wait 5 seconds after startup to begin pre-warming
            startup_delay: Duration::from_secs(5),
            // 1KB dummy objects
            dummy_object_size: 1024,
            // 30-second timeout for pre-warming operations
            operation_timeout: Duration::from_secs(30),
        }
    }
}

/// Session pre-warming service for S3 Express
pub struct SessionPrewarmingService {
    config: SessionPrewarmingConfig,
    client: S3ClientWrapper,
    local_bucket: String,
    remote_bucket: String,
    prewarming_key_prefix: String,
}

impl SessionPrewarmingService {
    pub fn new(
        config: SessionPrewarmingConfig,
        client: S3ClientWrapper,
        local_bucket: String,
        remote_bucket: String,
    ) -> Self {
        let prewarming_key_prefix = format!("__session_prewarming/{}", Uuid::new_v4());

        Self {
            config,
            client,
            local_bucket,
            remote_bucket,
            prewarming_key_prefix,
        }
    }

    /// Start the session pre-warming background task
    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            if !self.config.enabled {
                info!("Session pre-warming is disabled");
                return;
            }

            info!(
                "Starting session pre-warming service - interval: {}s, startup_delay: {}s",
                self.config.prewarming_interval.as_secs(),
                self.config.startup_delay.as_secs()
            );

            // Wait for initial startup delay
            sleep(self.config.startup_delay).await;

            // Perform initial pre-warming
            self.perform_startup_prewarming().await;

            // Start periodic pre-warming
            let mut interval = interval(self.config.prewarming_interval);
            loop {
                interval.tick().await;
                self.perform_periodic_prewarming().await;
            }
        })
    }

    /// Perform startup pre-warming to establish initial sessions
    async fn perform_startup_prewarming(&self) {
        info!("Performing startup session pre-warming");

        let start = std::time::Instant::now();
        let local_result = self.prewarm_bucket(&self.local_bucket, "startup").await;
        let remote_result = self.prewarm_bucket(&self.remote_bucket, "startup").await;

        let duration = start.elapsed();

        // Record metrics
        histogram!("s3_session_prewarming_duration_ms", "type" => "startup")
            .record(duration.as_millis() as f64);

        match (local_result, remote_result) {
            (Ok(()), Ok(())) => {
                info!(
                    "Startup session pre-warming completed successfully in {:?}",
                    duration
                );
                counter!("s3_session_prewarming_total", "type" => "startup", "result" => "success")
                    .increment(1);
            }
            _ => {
                warn!(
                    "Startup session pre-warming completed with errors in {:?}",
                    duration
                );
                counter!("s3_session_prewarming_total", "type" => "startup", "result" => "partial_failure").increment(1);
            }
        }
    }

    /// Perform periodic pre-warming to maintain active sessions
    async fn perform_periodic_prewarming(&self) {
        debug!("Performing periodic session pre-warming");

        let start = std::time::Instant::now();

        // Run pre-warming for both buckets concurrently
        let (local_result, remote_result) = tokio::join!(
            self.prewarm_bucket(&self.local_bucket, "periodic"),
            self.prewarm_bucket(&self.remote_bucket, "periodic")
        );

        let duration = start.elapsed();

        // Record metrics
        histogram!("s3_session_prewarming_duration_ms", "type" => "periodic")
            .record(duration.as_millis() as f64);

        match (local_result, remote_result) {
            (Ok(()), Ok(())) => {
                debug!(
                    "Periodic session pre-warming completed successfully in {:?}",
                    duration
                );
                counter!("s3_session_prewarming_total", "type" => "periodic", "result" => "success").increment(1);
            }
            (Err(e), Ok(())) => {
                warn!("Local bucket pre-warming failed: {}", e);
                counter!("s3_session_prewarming_total", "type" => "periodic", "result" => "local_failure").increment(1);
            }
            (Ok(()), Err(e)) => {
                warn!("Remote bucket pre-warming failed: {}", e);
                counter!("s3_session_prewarming_total", "type" => "periodic", "result" => "remote_failure").increment(1);
            }
            (Err(e1), Err(e2)) => {
                error!(
                    "Both bucket pre-warming failed - local: {}, remote: {}",
                    e1, e2
                );
                counter!("s3_session_prewarming_total", "type" => "periodic", "result" => "both_failure").increment(1);
            }
        }
    }

    /// Pre-warm a specific bucket by performing a lightweight operation
    async fn prewarm_bucket(
        &self,
        bucket: &str,
        operation_type: &str,
    ) -> Result<(), BlobStorageError> {
        let key = format!(
            "{}/{}_{}.tmp",
            self.prewarming_key_prefix,
            operation_type,
            Uuid::new_v4()
        );
        let dummy_data = Bytes::from(vec![0u8; self.config.dummy_object_size]);

        let start = std::time::Instant::now();

        // Perform PUT operation to trigger session authentication
        let put_result = tokio::time::timeout(self.config.operation_timeout, async {
            self.client
                .put_object()
                .await
                .bucket(bucket)
                .key(&key)
                .body(dummy_data.into())
                .send()
                .await
        })
        .await;

        let put_duration = start.elapsed();

        match put_result {
            Ok(Ok(_)) => {
                debug!(
                    "Pre-warming PUT succeeded for bucket {} in {:?}",
                    bucket, put_duration
                );

                // Immediately clean up the dummy object
                let delete_start = std::time::Instant::now();
                let delete_result = tokio::time::timeout(self.config.operation_timeout, async {
                    self.client
                        .delete_object()
                        .await
                        .bucket(bucket)
                        .key(&key)
                        .send()
                        .await
                })
                .await;

                let delete_duration = delete_start.elapsed();

                match delete_result {
                    Ok(Ok(_)) => {
                        debug!(
                            "Pre-warming cleanup succeeded for bucket {} in {:?}",
                            bucket, delete_duration
                        );
                    }
                    Ok(Err(e)) => {
                        warn!(
                            "Pre-warming cleanup failed for bucket {}: {} (non-critical)",
                            bucket, e
                        );
                    }
                    Err(_) => {
                        warn!(
                            "Pre-warming cleanup timed out for bucket {} (non-critical)",
                            bucket
                        );
                    }
                }

                // Record successful pre-warming metrics
                histogram!("s3_session_prewarming_operation_duration_ms", "bucket_type" => if bucket.contains(&self.local_bucket) { "local" } else { "remote" }, "operation" => "put")
                    .record(put_duration.as_millis() as f64);

                counter!("s3_session_prewarming_operations_total", "bucket_type" => if bucket.contains(&self.local_bucket) { "local" } else { "remote" }, "operation" => "put", "result" => "success")
                    .increment(1);

                Ok(())
            }
            Ok(Err(e)) => {
                warn!("Pre-warming PUT failed for bucket {}: {}", bucket, e);
                counter!("s3_session_prewarming_operations_total", "bucket_type" => if bucket.contains(&self.local_bucket) { "local" } else { "remote" }, "operation" => "put", "result" => "failure")
                    .increment(1);
                Err(BlobStorageError::S3(e.to_string()))
            }
            Err(_) => {
                warn!("Pre-warming PUT timed out for bucket {}", bucket);
                counter!("s3_session_prewarming_operations_total", "bucket_type" => if bucket.contains(&self.local_bucket) { "local" } else { "remote" }, "operation" => "put", "result" => "timeout")
                    .increment(1);
                Err(BlobStorageError::S3(
                    "Pre-warming operation timed out".to_string(),
                ))
            }
        }
    }
}
