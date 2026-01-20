//! S3 client creation and bucket management.

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use tracing::{info, warn};

use crate::config::Config;

/// Create and configure the S3 client (returns None if credentials are not available)
pub async fn create_s3_client(config: &Config) -> Option<Client> {
    let (access_key, secret_key) = match (&config.aws_access_key, &config.aws_secret_key) {
        (Some(ak), Some(sk)) => (ak.clone(), sk.clone()),
        _ => {
            info!("AWS credentials not configured, S3 client not created");
            return None;
        }
    };

    let region_provider = RegionProviderChain::first_try(Region::new(config.aws_region.clone()))
        .or_else(Region::new("us-west-2"));

    let credentials_provider = aws_sdk_s3::config::Credentials::new(
        &access_key,
        &secret_key,
        None,
        None,
        "custom",
    );

    let shared_config = aws_config::defaults(BehaviorVersion::v2026_01_12())
        .region(region_provider)
        .credentials_provider(credentials_provider)
        .load()
        .await;

    Some(Client::new(&shared_config))
}

/// Create S3 bucket if it doesn't exist
pub async fn create_bucket_if_not_exists(client: &Client, bucket_name: &str, region: &str) {
    match client.head_bucket().bucket(bucket_name).send().await {
        Ok(_) => info!(bucket = bucket_name, "Bucket already exists"),
        Err(_) => {
            // us-east-1 is the default region and doesn't use location constraint
            let create_bucket_config = if region == "us-east-1" {
                None
            } else {
                Some(
                    aws_sdk_s3::types::CreateBucketConfiguration::builder()
                        .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::from(
                            region,
                        ))
                        .build(),
                )
            };

            let mut request = client.create_bucket().bucket(bucket_name);
            if let Some(config) = create_bucket_config {
                request = request.create_bucket_configuration(config);
            }

            match request.send().await {
                Ok(_) => info!(bucket = bucket_name, region, "Bucket created"),
                Err(e) => warn!(
                    bucket = bucket_name,
                    error = %e,
                    "Failed to create bucket (may already exist or require manual creation)"
                ),
            }
        }
    }
}
