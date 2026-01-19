use serde::Deserialize;
use std::env;
use std::path::PathBuf;

/// Storage mode for Parquet files
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageMode {
    /// Upload to S3 only (delete local after upload) - default behavior
    #[default]
    S3,
    /// Persist to local disk only (no S3)
    Local,
    /// Both: upload to S3 AND keep a local copy
    Both,
}

impl<'de> Deserialize<'de> for StorageMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.to_lowercase().as_str() {
            "s3" => Ok(StorageMode::S3),
            "local" => Ok(StorageMode::Local),
            "both" => Ok(StorageMode::Both),
            _ => Err(serde::de::Error::custom(format!(
                "Invalid storage mode '{}'. Expected: s3, local, or both",
                s
            ))),
        }
    }
}

/// TOML config file structure
#[derive(Debug, Deserialize)]
pub struct ConfigFile {
    pub collector: CollectorConfig,
    #[serde(default)]
    pub aws: AwsConfig,
    #[serde(default)]
    pub database: DatabaseConfig,
    #[serde(default)]
    pub archive: ArchiveConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub websocket: WebSocketConfig,
    pub markets: Vec<MarketConfig>,
}

#[derive(Debug, Deserialize, Default)]
pub struct CollectorConfig {
    // Note: Per-market feeds are now in MarketConfig, not here
    #[serde(default = "default_batch_interval")]
    pub batch_interval_secs: u64,
    #[serde(default = "default_archive_interval")]
    pub archive_interval_secs: u64,
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,
    #[serde(default = "default_log_retention")]
    pub log_retention_days: u64,
}

#[derive(Debug, Deserialize)]
pub struct AwsConfig {
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default = "default_bucket")]
    pub bucket: String,
    pub home_server_name: Option<String>,
}

impl Default for AwsConfig {
    fn default() -> Self {
        Self {
            region: default_region(),
            bucket: default_bucket(),
            home_server_name: None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    #[serde(default = "default_db_path")]
    pub path: String,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            path: default_db_path(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ArchiveConfig {
    #[serde(default = "default_archive_dir")]
    pub dir: String,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            dir: default_archive_dir(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    /// Storage mode: "s3" (default), "local", or "both"
    #[serde(default)]
    pub mode: StorageMode,
    /// Directory for persistent local Parquet storage (hierarchical structure)
    /// Only used when mode is "local" or "both"
    #[serde(default = "default_local_storage_path")]
    pub local_path: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            mode: StorageMode::default(),
            local_path: default_local_storage_path(),
        }
    }
}

fn default_local_storage_path() -> String {
    "data/parquet".to_string()
}

#[derive(Debug, Deserialize, Default)]
pub struct WebSocketConfig {
    #[serde(default = "default_message_timeout")]
    pub message_timeout_secs: u64,
    #[serde(default = "default_initial_retry")]
    pub initial_retry_delay_secs: u64,
    #[serde(default = "default_max_retry")]
    pub max_retry_delay_secs: u64,
}

#[derive(Debug, Deserialize)]
pub struct MarketConfig {
    pub exchange: String,
    pub symbols: Vec<String>,
    /// Feeds to collect for this market (defaults to ["orderbook", "trades"])
    #[serde(default = "default_market_feeds")]
    pub feeds: Vec<String>,
    /// Optional WebSocket base URL override (exchange-specific)
    /// Currently only used for Binance to support different endpoints:
    /// - wss://stream.binance.com:9443/ws (international, default)
    /// - wss://data-stream.binance.vision/ws (market data, may bypass geo-restrictions)
    /// - wss://stream.binance.us:9443/ws (US, different trading pairs)
    pub base_url: Option<String>,
    /// Optional data timeout override for low-volume pairs (seconds)
    /// If no trades/orderbook updates are received for 3x this value, the connection is
    /// considered stale and will reconnect. Useful for low-volume pairs like XRP-USDT.
    /// Default: uses global websocket.message_timeout_secs
    pub data_timeout_secs: Option<u64>,
}

fn default_market_feeds() -> Vec<String> {
    vec!["orderbook".to_string(), "trades".to_string()]
}

// Default value functions
fn default_batch_interval() -> u64 {
    5
}
fn default_archive_interval() -> u64 {
    3600
}
fn default_metrics_port() -> u16 {
    9090
}
fn default_log_retention() -> u64 {
    1
}
fn default_region() -> String {
    "us-west-2".to_string()
}
fn default_bucket() -> String {
    "crypto-exchange-data-collector".to_string()
}
fn default_db_path() -> String {
    "data/collector.db".to_string()
}
fn default_archive_dir() -> String {
    "data/archive".to_string()
}
fn default_message_timeout() -> u64 {
    30
}
fn default_initial_retry() -> u64 {
    1
}
fn default_max_retry() -> u64 {
    60
}

/// A single exchange+symbol pair for data collection
#[derive(Debug, Clone)]
pub struct MarketPair {
    pub exchange: String,
    pub symbol: String,
    /// Feeds to collect for this market pair
    pub feeds: Vec<String>,
    /// Optional WebSocket base URL override (exchange-specific)
    pub base_url: Option<String>,
    /// Optional data timeout override for low-volume pairs (seconds)
    pub data_timeout_secs: Option<u64>,
}

/// Application configuration
pub struct Config {
    // Storage settings
    pub storage_mode: StorageMode,
    pub local_storage_path: PathBuf,
    // AWS credentials (from env vars for security)
    // Optional when storage_mode is Local
    pub aws_access_key: Option<String>,
    pub aws_secret_key: Option<String>,
    pub aws_region: String,
    pub bucket_name: String,
    pub home_server_name: Option<String>,
    // Coinbase API credentials (optional, from env vars for security)
    // Required for level2 (orderbook) channel, not needed for matches (trades)
    pub coinbase_api_key: Option<String>,
    pub coinbase_api_secret: Option<String>,
    // Markets to collect (each MarketPair has its own feeds)
    pub market_pairs: Vec<MarketPair>,
    // Paths
    pub database_path: PathBuf,
    pub archive_dir: PathBuf,
    // Intervals
    pub batch_interval_secs: u64,
    pub archive_interval_secs: u64,
    // WebSocket settings
    pub ws_message_timeout_secs: u64,
    pub ws_initial_retry_delay_secs: u64,
    pub ws_max_retry_delay_secs: u64,
    // Other
    pub log_retention_days: u64,
    pub metrics_port: u16,
}

impl Config {
    /// Load configuration from TOML file
    pub fn from_toml() -> Self {
        let config = Self::load_from_toml();
        config.validate();
        config
    }

    fn load_from_toml() -> Self {
        // Config file path (defaults to config.toml in current dir)
        let config_path = env::var("CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());

        let content = std::fs::read_to_string(&config_path).unwrap_or_else(|e| {
            panic!(
                "Failed to read config file '{}': {}. Create config.toml or set CONFIG_PATH.",
                config_path, e
            )
        });

        let file: ConfigFile = toml::from_str(&content).unwrap_or_else(|e| {
            panic!("Failed to parse config file '{}': {}", config_path, e)
        });

        let storage_mode = file.storage.mode;

        // AWS credentials from environment (not in TOML for security)
        // Required for S3 and Both modes, optional for Local mode
        let (aws_access_key, aws_secret_key) = match storage_mode {
            StorageMode::Local => {
                // AWS credentials optional for local-only storage
                (env::var("AWS_ACCESS_KEY").ok(), env::var("AWS_SECRET_KEY").ok())
            }
            StorageMode::S3 | StorageMode::Both => {
                // AWS credentials required for S3 storage
                let key = env::var("AWS_ACCESS_KEY")
                    .expect("AWS_ACCESS_KEY must be set when storage.mode is 's3' or 'both'");
                let secret = env::var("AWS_SECRET_KEY")
                    .expect("AWS_SECRET_KEY must be set when storage.mode is 's3' or 'both'");
                (Some(key), Some(secret))
            }
        };

        // Coinbase API credentials from environment (optional)
        // Required for level2 (orderbook) channel authentication
        let coinbase_api_key = env::var("COINBASE_API_KEY").ok();
        if coinbase_api_key.is_some() {
            tracing::info!("COINBASE_API_KEY found in environment");
        } else {
            tracing::debug!("COINBASE_API_KEY not set");
        }
        // Load secret from file if COINBASE_API_SECRET_FILE is set, otherwise try COINBASE_API_SECRET
        let coinbase_api_secret = if let Ok(path) = env::var("COINBASE_API_SECRET_FILE") {
            tracing::info!(path = %path, "COINBASE_API_SECRET_FILE env var found, reading file");
            match std::fs::read_to_string(&path) {
                Ok(content) => {
                    // Normalize: trim, fix line endings, ensure trailing newline (required by some PEM parsers)
                    let mut content = content.trim().replace("\r\n", "\n").to_string();
                    if !content.ends_with('\n') {
                        content.push('\n');
                    }
                    tracing::info!(path = %path, len = content.len(), "Loaded COINBASE_API_SECRET from file");
                    Some(content)
                }
                Err(e) => {
                    tracing::error!(path = %path, error = %e, "Failed to read COINBASE_API_SECRET_FILE");
                    None
                }
            }
        } else if let Ok(secret) = env::var("COINBASE_API_SECRET") {
            tracing::info!(len = secret.len(), "COINBASE_API_SECRET env var found (not file)");
            Some(secret.replace("\\n", "\n"))
        } else {
            tracing::debug!("Neither COINBASE_API_SECRET_FILE nor COINBASE_API_SECRET set");
            None
        };

        // Flatten markets into pairs (each symbol gets the market's feeds, base_url, and timeout)
        let market_pairs: Vec<MarketPair> = file
            .markets
            .iter()
            .flat_map(|m| {
                let feeds = m.feeds.clone();
                let base_url = m.base_url.clone();
                let data_timeout_secs = m.data_timeout_secs;
                m.symbols.iter().map(move |s| MarketPair {
                    exchange: m.exchange.to_lowercase(),
                    symbol: s.clone(),
                    feeds: feeds.clone(),
                    base_url: base_url.clone(),
                    data_timeout_secs,
                })
            })
            .collect();

        if market_pairs.is_empty() {
            panic!("Configuration error: No markets configured. Add [[markets]] sections to config.toml");
        }

        // Resolve paths relative to current directory
        let curr_dir = env::current_dir().expect("Failed to get current directory");
        let database_path = curr_dir.join(&file.database.path);
        let archive_dir = curr_dir.join(&file.archive.dir);
        let local_storage_path = curr_dir.join(&file.storage.local_path);

        // Ensure directories exist
        if let Some(parent) = database_path.parent() {
            std::fs::create_dir_all(parent).expect("Failed to create database directory");
        }
        std::fs::create_dir_all(&archive_dir).expect("Failed to create archive directory");

        // Create local storage directory if using local or both mode
        if matches!(storage_mode, StorageMode::Local | StorageMode::Both) {
            std::fs::create_dir_all(&local_storage_path)
                .expect("Failed to create local storage directory");
        }

        Self {
            storage_mode,
            local_storage_path,
            aws_access_key,
            aws_secret_key,
            aws_region: file.aws.region,
            bucket_name: file.aws.bucket,
            home_server_name: file.aws.home_server_name,
            coinbase_api_key,
            coinbase_api_secret,
            market_pairs,
            database_path,
            archive_dir,
            batch_interval_secs: file.collector.batch_interval_secs,
            archive_interval_secs: file.collector.archive_interval_secs,
            ws_message_timeout_secs: file.websocket.message_timeout_secs,
            ws_initial_retry_delay_secs: file.websocket.initial_retry_delay_secs,
            ws_max_retry_delay_secs: file.websocket.max_retry_delay_secs,
            log_retention_days: file.collector.log_retention_days,
            metrics_port: file.collector.metrics_port,
        }
    }

    /// Validate configuration for conflicting or invalid values
    fn validate(&self) {
        // Check for duplicate market pairs
        let mut seen = std::collections::HashSet::new();
        for pair in &self.market_pairs {
            let key = format!("{}:{}", pair.exchange, pair.symbol.to_lowercase());
            if !seen.insert(key.clone()) {
                panic!("Configuration error: Duplicate market pair: {}", key);
            }
        }

        // Validate exchange names
        let supported = crate::exchanges::supported_exchanges();
        for pair in &self.market_pairs {
            if !supported.contains(&pair.exchange.as_str()) {
                panic!(
                    "Configuration error: Unknown exchange '{}'. Supported: {:?}",
                    pair.exchange, supported
                );
            }
        }

        // WS timeout should be > batch interval to avoid false reconnects
        if self.ws_message_timeout_secs < self.batch_interval_secs {
            panic!(
                "Configuration error: websocket.message_timeout_secs ({}) must be >= collector.batch_interval_secs ({}) to avoid false timeouts",
                self.ws_message_timeout_secs, self.batch_interval_secs
            );
        }

        // Archive interval should be reasonable (1 min to 24 hours)
        if self.archive_interval_secs < 60 {
            panic!(
                "Configuration error: collector.archive_interval_secs ({}) must be at least 60 seconds",
                self.archive_interval_secs
            );
        }
        if self.archive_interval_secs > 86400 {
            panic!(
                "Configuration error: collector.archive_interval_secs ({}) must be at most 86400 seconds (24 hours)",
                self.archive_interval_secs
            );
        }

        // Retry delay should be < message timeout
        if self.ws_initial_retry_delay_secs >= self.ws_message_timeout_secs {
            panic!(
                "Configuration error: websocket.initial_retry_delay_secs ({}) must be < websocket.message_timeout_secs ({})",
                self.ws_initial_retry_delay_secs, self.ws_message_timeout_secs
            );
        }

        // Max retry delay should be >= initial delay
        if self.ws_max_retry_delay_secs < self.ws_initial_retry_delay_secs {
            panic!(
                "Configuration error: websocket.max_retry_delay_secs ({}) must be >= websocket.initial_retry_delay_secs ({})",
                self.ws_max_retry_delay_secs, self.ws_initial_retry_delay_secs
            );
        }
    }
}
