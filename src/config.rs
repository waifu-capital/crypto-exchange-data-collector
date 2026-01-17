use std::env;
use std::path::PathBuf;

/// Application configuration loaded from environment variables
pub struct Config {
    pub aws_access_key: String,
    pub aws_secret_key: String,
    pub aws_region: String,
    pub exchange: String,
    pub market_symbol: String,
    pub feeds: Vec<String>,
    pub bucket_name: String,
    pub database_path: PathBuf,
    pub archive_dir: PathBuf,
    pub batch_interval_secs: u64,
    pub home_server_name: Option<String>,
    pub log_retention_days: u64,
    pub metrics_port: u16,
    // WebSocket connection settings
    pub ws_message_timeout_secs: u64,
    pub ws_initial_retry_delay_secs: u64,
    pub ws_max_retry_delay_secs: u64,
    // Archive settings
    pub archive_interval_secs: u64,
}

impl Config {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let aws_access_key = env::var("AWS_ACCESS_KEY").expect("AWS_ACCESS_KEY must be set");
        let aws_secret_key = env::var("AWS_SECRET_KEY").expect("AWS_SECRET_KEY must be set");
        let aws_region = env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());

        // Exchange configuration (defaults to binance for backward compatibility)
        let exchange = env::var("EXCHANGE").unwrap_or_else(|_| "binance".to_string());
        let market_symbol = env::var("MARKET_SYMBOL").unwrap_or_else(|_| "btcusdt".to_string());

        // Feed types: comma-separated list (e.g., "orderbook,trades")
        let feeds = env::var("FEEDS")
            .unwrap_or_else(|_| "orderbook".to_string())
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect();

        let home_server_name = env::var("HOME_SERVER_NAME").ok();

        let curr_dir = env::current_dir().expect("Failed to get current directory");
        let base_path = curr_dir.join("orderbookdata");

        // Single bucket for all data (hierarchical prefixes used in S3 keys)
        let bucket_name = env::var("BUCKET_NAME")
            .unwrap_or_else(|_| "crypto-market-data".to_string());

        // Local paths still use exchange/symbol for organization
        let exchange_lower = exchange.to_lowercase();
        let symbol_lower = market_symbol.to_lowercase();
        let database_path = base_path.join(format!(
            "snapshots-{}-spot-{}.db",
            exchange_lower, symbol_lower
        ));
        let archive_dir = base_path.join(format!("archive-{}-{}", exchange_lower, symbol_lower));

        let batch_interval_secs = env::var("BATCH_INTERVAL")
            .unwrap_or_else(|_| "5".to_string())
            .parse::<u64>()
            .unwrap_or(5);

        let log_retention_days = env::var("LOG_RETENTION_DAYS")
            .unwrap_or_else(|_| "1".to_string())
            .parse::<u64>()
            .unwrap_or(1);

        let metrics_port = env::var("METRICS_PORT")
            .unwrap_or_else(|_| "9090".to_string())
            .parse::<u16>()
            .unwrap_or(9090);

        let ws_message_timeout_secs = env::var("WS_MESSAGE_TIMEOUT_SECS")
            .unwrap_or_else(|_| "30".to_string())
            .parse::<u64>()
            .unwrap_or(30);

        let ws_initial_retry_delay_secs = env::var("WS_INITIAL_RETRY_DELAY_SECS")
            .unwrap_or_else(|_| "1".to_string())
            .parse::<u64>()
            .unwrap_or(1);

        let ws_max_retry_delay_secs = env::var("WS_MAX_RETRY_DELAY_SECS")
            .unwrap_or_else(|_| "60".to_string())
            .parse::<u64>()
            .unwrap_or(60);

        let archive_interval_secs = env::var("ARCHIVE_INTERVAL_SECS")
            .unwrap_or_else(|_| "3600".to_string())
            .parse::<u64>()
            .unwrap_or(3600);

        // Ensure directories exist
        std::fs::create_dir_all(&base_path).expect("Failed to create base data directory");
        std::fs::create_dir_all(&archive_dir).expect("Failed to create archive directory");

        Self {
            aws_access_key,
            aws_secret_key,
            aws_region,
            exchange,
            market_symbol,
            feeds,
            bucket_name,
            database_path,
            archive_dir,
            batch_interval_secs,
            home_server_name,
            log_retention_days,
            metrics_port,
            ws_message_timeout_secs,
            ws_initial_retry_delay_secs,
            ws_max_retry_delay_secs,
            archive_interval_secs,
        }
    }
}
