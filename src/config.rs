use std::env;
use std::path::PathBuf;

/// Application configuration loaded from environment variables
pub struct Config {
    pub aws_access_key: String,
    pub aws_secret_key: String,
    pub aws_region: String,
    pub market_symbol: String,
    pub bucket_name: String,
    pub database_path: PathBuf,
    pub archive_dir: PathBuf,
    pub batch_interval_secs: u64,
    pub home_server_name: Option<String>,
    pub log_retention_days: u64,
}

impl Config {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let aws_access_key = env::var("AWS_ACCESS_KEY").expect("AWS_ACCESS_KEY must be set");
        let aws_secret_key = env::var("AWS_SECRET_KEY").expect("AWS_SECRET_KEY must be set");
        let aws_region = env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());
        let market_symbol = env::var("MARKET_SYMBOL").unwrap_or_else(|_| "bnbusdt".to_string());
        let home_server_name = env::var("HOME_SERVER_NAME").ok();

        let curr_dir = env::current_dir().expect("Failed to get current directory");
        let base_path = curr_dir.join("orderbookdata");
        let bucket_name = format!("binance-spot-{}", market_symbol.to_lowercase());
        let database_path = base_path.join(format!("snapshots-binance-spot-{}.db", market_symbol.to_lowercase()));
        let archive_dir = base_path.join(format!("archive-{}", market_symbol.to_lowercase()));

        let batch_interval_secs = env::var("BATCH_INTERVAL")
            .unwrap_or_else(|_| "5".to_string())
            .parse::<u64>()
            .unwrap_or(5);

        let log_retention_days = env::var("LOG_RETENTION_DAYS")
            .unwrap_or_else(|_| "1".to_string())
            .parse::<u64>()
            .unwrap_or(1);

        // Ensure directories exist
        std::fs::create_dir_all(&base_path).expect("Failed to create base data directory");
        std::fs::create_dir_all(&archive_dir).expect("Failed to create archive directory");

        Self {
            aws_access_key,
            aws_secret_key,
            aws_region,
            market_symbol,
            bucket_name,
            database_path,
            archive_dir,
            batch_interval_secs,
            home_server_name,
            log_retention_days,
        }
    }
}
