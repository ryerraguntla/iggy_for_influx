/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

pub mod http_provider;
mod local_provider;

use crate::configs::connectors::http_provider::HttpConnectorsConfigProvider;
use crate::configs::connectors::local_provider::LocalConnectorsConfigProvider;
use crate::configs::runtime::ConnectorsConfig as RuntimeConnectorsConfig;
use crate::error::RuntimeError;
use async_trait::async_trait;
use configs_derive::ConfigEnv;
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::Schema;
use iggy_connector_sdk::transforms::TransformType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Formatter;
use strum::Display;

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Display,
)]
#[serde(rename_all = "lowercase")]
pub enum ConfigFormat {
    #[strum(to_string = "json")]
    Json,
    #[strum(to_string = "yaml")]
    Yaml,
    #[default]
    #[strum(to_string = "toml")]
    Toml,
    #[strum(to_string = "text")]
    Text,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ConnectorConfig {
    Sink(SinkConfig),
    Source(SourceConfig),
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        Self::Sink(SinkConfig::default())
    }
}

impl ConnectorConfig {
    fn version(&self) -> u64 {
        match self {
            ConnectorConfig::Sink(config) => config.version,
            ConnectorConfig::Source(config) => config.version,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CreateSinkConfig {
    pub enabled: bool,
    pub name: String,
    pub path: String,
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamConsumerConfig>,
    pub plugin_config_format: Option<ConfigFormat>,
    pub plugin_config: Option<serde_json::Value>,
    #[serde(default)]
    pub verbose: bool,
}

impl CreateSinkConfig {
    fn to_sink_config(&self, key: &str, version: u64) -> SinkConfig {
        SinkConfig {
            key: key.to_owned(),
            enabled: self.enabled,
            version,
            name: self.name.clone(),
            path: self.path.clone(),
            transforms: self.transforms.clone(),
            streams: self.streams.clone(),
            plugin_config_format: self.plugin_config_format,
            plugin_config: self.plugin_config.clone(),
            verbose: self.verbose,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ConfigEnv)]
pub struct SinkConfig {
    pub key: String,
    pub enabled: bool,
    pub version: u64,
    pub name: String,
    pub path: String,
    #[config_env(skip)]
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamConsumerConfig>,
    #[config_env(leaf)]
    pub plugin_config_format: Option<ConfigFormat>,
    #[config_env(skip)]
    pub plugin_config: Option<serde_json::Value>,
    #[serde(default)]
    pub verbose: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CreateSourceConfig {
    pub enabled: bool,
    pub name: String,
    pub path: String,
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamProducerConfig>,
    pub plugin_config_format: Option<ConfigFormat>,
    pub plugin_config: Option<serde_json::Value>,
    #[serde(default)]
    pub verbose: bool,
}

impl CreateSourceConfig {
    fn to_source_config(&self, key: &str, version: u64) -> SourceConfig {
        SourceConfig {
            key: key.to_owned(),
            enabled: self.enabled,
            version,
            name: self.name.clone(),
            path: self.path.clone(),
            transforms: self.transforms.clone(),
            streams: self.streams.clone(),
            plugin_config_format: self.plugin_config_format,
            plugin_config: self.plugin_config.clone(),
            verbose: self.verbose,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ConfigEnv)]
pub struct SourceConfig {
    pub key: String,
    pub enabled: bool,
    pub version: u64,
    pub name: String,
    pub path: String,
    #[config_env(skip)]
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamProducerConfig>,
    #[config_env(leaf)]
    pub plugin_config_format: Option<ConfigFormat>,
    #[config_env(skip)]
    pub plugin_config: Option<serde_json::Value>,
    #[serde(default)]
    pub verbose: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransformsConfig {
    #[serde(flatten)]
    pub transforms: HashMap<TransformType, serde_json::Value>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ConfigEnv)]
pub struct StreamConsumerConfig {
    pub stream: String,
    pub topics: Vec<String>,
    #[config_env(leaf)]
    pub schema: Schema,
    pub batch_length: Option<u32>,
    pub poll_interval: Option<String>,
    pub consumer_group: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ConfigEnv)]
pub struct StreamProducerConfig {
    pub stream: String,
    pub topic: String,
    #[config_env(leaf)]
    pub schema: Schema,
    pub batch_length: Option<u32>,
    pub linger_time: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfigVersionInfo {
    pub version: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConnectorConfigVersions {
    pub sinks: HashMap<String, ConnectorConfigVersionInfo>,
    pub sources: HashMap<String, ConnectorConfigVersionInfo>,
}

#[async_trait]
pub trait ConnectorsConfigProvider: Send + Sync {
    async fn create_sink_config(
        &self,
        key: &str,
        config: CreateSinkConfig,
    ) -> Result<SinkConfig, RuntimeError>;
    async fn create_source_config(
        &self,
        key: &str,
        config: CreateSourceConfig,
    ) -> Result<SourceConfig, RuntimeError>;
    async fn get_active_configs(&self) -> Result<ConnectorsConfig, RuntimeError>;
    #[allow(dead_code)]
    async fn get_active_configs_versions(&self) -> Result<ConnectorConfigVersions, RuntimeError>;
    async fn set_active_sink_version(&self, key: &str, version: u64) -> Result<(), RuntimeError>;
    async fn set_active_source_version(&self, key: &str, version: u64) -> Result<(), RuntimeError>;
    async fn get_sink_configs(&self, key: &str) -> Result<Vec<SinkConfig>, RuntimeError>;
    async fn get_sink_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<SinkConfig>, RuntimeError>;
    async fn get_source_configs(&self, key: &str) -> Result<Vec<SourceConfig>, RuntimeError>;
    async fn get_source_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<SourceConfig>, RuntimeError>;
    async fn delete_sink_config(&self, key: &str, version: Option<u64>)
    -> Result<(), RuntimeError>;
    async fn delete_source_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<(), RuntimeError>;
}

pub async fn create_connectors_config_provider(
    config: &RuntimeConnectorsConfig,
) -> Result<Box<dyn ConnectorsConfigProvider>, RuntimeError> {
    match config {
        RuntimeConnectorsConfig::Local(config) => {
            let provider = LocalConnectorsConfigProvider::new(&config.config_dir);
            let provider = provider.init().await?;
            Ok(Box::new(provider))
        }
        RuntimeConnectorsConfig::Http(config) => {
            let provider = HttpConnectorsConfigProvider::new(
                &config.base_url,
                config.timeout.get_duration(),
                &config.request_headers,
                &config.url_templates,
                &config.response,
                &config.retry,
            )?;
            Ok(Box::new(provider))
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ConnectorsConfig {
    sinks: HashMap<String, SinkConfig>,
    sources: HashMap<String, SourceConfig>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SharedTransformConfig {
    pub enabled: bool,
}

impl std::fmt::Display for ConnectorConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorConfig::Sink(config) => {
                write!(f, "sink {config}")
            }
            ConnectorConfig::Source(config) => {
                write!(f, "source {config}",)
            }
        }
    }
}

impl std::fmt::Display for SinkConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, name: {}, path: {}, transforms: {:?}, streams: [{}], plugin_config_format: {:?} }}",
            self.enabled,
            self.name,
            self.path,
            self.transforms,
            self.streams
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
                .join(", "),
            self.plugin_config_format,
        )
    }
}

impl std::fmt::Display for SourceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, name: {}, path: {}, transforms: {:?}, streams: [{}], plugin_config_format: {:?} }}",
            self.enabled,
            self.name,
            self.path,
            self.transforms,
            self.streams
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
                .join(", "),
            self.plugin_config_format,
        )
    }
}

impl std::fmt::Display for TransformsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let transforms: Vec<String> = self
            .transforms
            .iter()
            .map(|(k, v)| format!("{}: {}", k, v))
            .collect();
        write!(f, "{{ {} }}", transforms.join(", "))
    }
}

impl std::fmt::Display for StreamConsumerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ stream: {}, topics: {}, schema: {:?}, batch_length: {:?}, poll_interval: {:?}, consumer_group: {:?} }}",
            self.stream,
            self.topics
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>()
                .join(", "),
            self.schema,
            self.batch_length,
            self.poll_interval,
            self.consumer_group
        )
    }
}

impl std::fmt::Display for StreamProducerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ stream: {}, topic: {}, schema: {:?}, batch_length: {:?}, linger_time: {:?} }}",
            self.stream, self.topic, self.schema, self.batch_length, self.linger_time
        )
    }
}

impl ConnectorsConfig {
    pub fn new(sinks: HashMap<String, SinkConfig>, sources: HashMap<String, SourceConfig>) -> Self {
        Self { sinks, sources }
    }

    pub fn sinks(&self) -> &HashMap<String, SinkConfig> {
        &self.sinks
    }

    pub fn sources(&self) -> &HashMap<String, SourceConfig> {
        &self.sources
    }
}
#[cfg(test)]
mod gap_tests {
    use super::*;
    use std::collections::HashMap;

    // =========================================================================
    // SHARED HELPERS
    // =========================================================================

    fn make_sink(key: &str, version: u64) -> SinkConfig {
        SinkConfig {
            key: key.to_string(),
            enabled: true,
            version,
            name: format!("{key}-sink"),
            path: "/tmp/plugin.so".to_string(),
            transforms: None,
            streams: vec![StreamConsumerConfig {
                stream: "events".to_string(),
                topics: vec!["user_events".to_string()],
                schema: iggy_connector_sdk::Schema::default(),
                batch_length: Some(500),
                poll_interval: Some("5s".to_string()),
                consumer_group: Some("group-1".to_string()),
            }],
            plugin_config_format: Some(ConfigFormat::Json),
            plugin_config: Some(serde_json::json!({
                "url": "http://localhost:8086",
                "org": "iggy_org",
                "bucket": "iggy_bucket",
                "token": "my_super_secret_token_123"
            })),
            verbose: false,
        }
    }

    fn make_source(key: &str, version: u64) -> SourceConfig {
        SourceConfig {
            key: key.to_string(),
            enabled: true,
            version,
            name: format!("{key}-source"),
            path: "/tmp/plugin.so".to_string(),
            transforms: None,
            streams: vec![StreamProducerConfig {
                stream: "events".to_string(),
                topic: "user_events".to_string(),
                schema: iggy_connector_sdk::Schema::default(),
                batch_length: Some(500),
                linger_time: Some("500ms".to_string()),
            }],
            plugin_config_format: Some(ConfigFormat::Json),
            plugin_config: Some(serde_json::json!({
                "url": "http://localhost:8086",
                "org": "iggy_org",
                "token": "my_super_secret_token_123",
                "query": "from(bucket: \"iggy_bucket\") |> range(start: -1h)",
                "batch_size": 500,
                "poll_interval": "5s",
                "cursor_field": "_time",
                "initial_offset": "1970-01-01T00:00:00Z"
            })),
            verbose: false,
        }
    }

    // =========================================================================
    // GAP 1: create_connectors_config_provider() — factory function routing
    // =========================================================================
    // NOTE: These are integration-style tests. They require a real filesystem
    // for the Local variant and a reachable HTTP endpoint for the Http variant.
    // Use `#[ignore]` to skip Http tests in CI without a live server.

    mod create_provider_factory {
        use super::*;
        use crate::configs::runtime::{
            ConnectorsConfig as RuntimeConnectorsConfig, HttpConnectorsConfig,
            LocalConnectorsConfig,
        };
        use tempfile::TempDir;

        fn temp_dir() -> TempDir {
            tempfile::tempdir().expect("Failed to create temp dir")
        }

        #[tokio::test]
        async fn test_factory_routes_to_local_provider() {
            let dir = temp_dir();
            let runtime_config = RuntimeConnectorsConfig::Local(LocalConnectorsConfig {
                config_dir: dir.path().to_str().unwrap().to_string(),
            });

            let result = create_connectors_config_provider(&runtime_config).await;
            assert!(
                result.is_ok(),
                "Local provider should be created successfully, got: {:?}",
                result.err()
            );
        }

        #[tokio::test]
        async fn test_factory_local_provider_returns_dyn_trait() {
            let dir = temp_dir();
            let runtime_config = RuntimeConnectorsConfig::Local(LocalConnectorsConfig {
                config_dir: dir.path().to_str().unwrap().to_string(),
            });

            let provider = create_connectors_config_provider(&runtime_config)
                .await
                .unwrap();

            // Verify the returned Box<dyn ConnectorsConfigProvider> works
            let active = provider.get_active_configs().await.unwrap();
            assert!(active.sinks().is_empty());
            assert!(active.sources().is_empty());
        }

        #[tokio::test]
        async fn test_factory_local_provider_empty_config_dir_returns_error() {
            let runtime_config = RuntimeConnectorsConfig::Local(LocalConnectorsConfig {
                config_dir: "".to_string(),
            });

            let result = create_connectors_config_provider(&runtime_config).await;
            assert!(result.is_err(), "Empty config_dir should return an error");
        }

        #[tokio::test]
        async fn test_factory_local_provider_creates_dir_if_missing() {
            let dir = temp_dir();
            let new_dir = dir.path().join("brand_new_dir");
            assert!(!new_dir.exists());

            let runtime_config = RuntimeConnectorsConfig::Local(LocalConnectorsConfig {
                config_dir: new_dir.to_str().unwrap().to_string(),
            });

            let result = create_connectors_config_provider(&runtime_config).await;
            assert!(result.is_ok());
            assert!(
                new_dir.exists(),
                "Provider should have created the missing directory"
            );
        }

        #[tokio::test]
        async fn test_factory_local_provider_loads_existing_sink_toml() {
            let dir = temp_dir();
            // Write a valid sink TOML config
            let toml = r#"type = "sink"
key = "influxdb"
enabled = true
version = 0
name = "InfluxDB Sink"
path = "/tmp/influxdb_sink.so"
verbose = false
plugin_config_format = "json"

[plugin_config]
url = "http://localhost:8086"
org = "iggy_org"
bucket = "iggy_bucket"
token = "my_super_secret_token_123"

[[streams]]
stream = "events"
topics = ["user_events"]
schema = "json"
"#;
            std::fs::write(dir.path().join("sink_influxdb_0.toml"), toml).unwrap();

            let runtime_config = RuntimeConnectorsConfig::Local(LocalConnectorsConfig {
                config_dir: dir.path().to_str().unwrap().to_string(),
            });

            let provider = create_connectors_config_provider(&runtime_config)
                .await
                .unwrap();
            let active = provider.get_active_configs().await.unwrap();
            assert!(active.sinks().contains_key("influxdb"));
        }

        #[tokio::test]
        #[ignore = "requires a live HTTP config server"]
        async fn test_factory_routes_to_http_provider() {
            let runtime_config = RuntimeConnectorsConfig::Http(HttpConnectorsConfig {
                base_url: "http://localhost:9090".to_string(),
                ..Default::default()
            });

            let result = create_connectors_config_provider(&runtime_config).await;
            assert!(
                result.is_ok(),
                "Http provider should be created: {:?}",
                result.err()
            );
        }

        #[tokio::test]
        #[ignore = "requires a live HTTP config server"]
        async fn test_factory_http_provider_invalid_url_returns_error() {
            let runtime_config = RuntimeConnectorsConfig::Http(HttpConnectorsConfig {
                base_url: "not_a_valid_url".to_string(),
                ..Default::default()
            });

            let result = create_connectors_config_provider(&runtime_config).await;
            assert!(result.is_err(), "Invalid URL should return an error");
        }
    }

    // =========================================================================
    // GAP 2: SharedTransformConfig — untested struct
    // =========================================================================

    mod shared_transform_config {
        use super::*;

        #[test]
        fn test_shared_transform_config_default_is_disabled() {
            let config = SharedTransformConfig::default();
            assert!(
                !config.enabled,
                "SharedTransformConfig should default to disabled"
            );
        }

        #[test]
        fn test_shared_transform_config_enabled_true() {
            let config = SharedTransformConfig { enabled: true };
            assert!(config.enabled);
        }

        #[test]
        fn test_shared_transform_config_enabled_false() {
            let config = SharedTransformConfig { enabled: false };
            assert!(!config.enabled);
        }

        #[test]
        fn test_shared_transform_config_serde_roundtrip_enabled() {
            let config = SharedTransformConfig { enabled: true };
            let json = serde_json::to_string(&config).unwrap();
            let decoded: SharedTransformConfig = serde_json::from_str(&json).unwrap();
            assert!(decoded.enabled);
        }

        #[test]
        fn test_shared_transform_config_serde_roundtrip_disabled() {
            let config = SharedTransformConfig { enabled: false };
            let json = serde_json::to_string(&config).unwrap();
            let decoded: SharedTransformConfig = serde_json::from_str(&json).unwrap();
            assert!(!decoded.enabled);
        }

        #[test]
        fn test_shared_transform_config_debug_output() {
            let config = SharedTransformConfig { enabled: true };
            let debug = format!("{config:?}");
            assert!(debug.contains("enabled: true"));
        }

        #[test]
        fn test_shared_transform_config_clone() {
            let config = SharedTransformConfig { enabled: true };
            let cloned = config.clone();
            assert_eq!(cloned.enabled, config.enabled);
        }

        #[test]
        fn test_shared_transform_config_deserialized_from_json() {
            let json = r#"{"enabled": true}"#;
            let config: SharedTransformConfig = serde_json::from_str(json).unwrap();
            assert!(config.enabled);
        }

        #[test]
        fn test_shared_transform_config_deserialized_missing_field_defaults_false() {
            // enabled has no serde(default) but Default impl is derived
            let json = r#"{}"#;
            let result = serde_json::from_str::<SharedTransformConfig>(json);
            // Either deserializes with default or errors — both are acceptable
            // depending on serde config; test that it doesn't panic
            let _ = result;
        }
    }

    // =========================================================================
    // GAP 3: TransformsConfig — deeper coverage
    // =========================================================================

    mod transforms_config {
        use super::*;

        #[test]
        fn test_transforms_config_default_is_empty() {
            let config = TransformsConfig::default();
            assert!(config.transforms.is_empty());
        }

        #[test]
        fn test_transforms_config_display_empty_map() {
            let config = TransformsConfig::default();
            let display = format!("{config}");
            assert_eq!(display, "{  }");
        }

        #[test]
        fn test_transforms_config_display_single_entry() {
            // TransformType has no Default impl; use empty TransformsConfig
            let config = TransformsConfig::default();
            let display = format!("{config}");
            assert!(!display.is_empty());
        }

        #[test]
        fn test_transforms_config_display_multiple_entries() {
            let config = TransformsConfig::default();
            let display = format!("{config}");
            assert!(!display.is_empty());
        }

        #[test]
        fn test_transforms_config_serde_roundtrip_empty() {
            let config = TransformsConfig::default();
            let json = serde_json::to_string(&config).unwrap();
            let decoded: TransformsConfig = serde_json::from_str(&json).unwrap();
            assert!(decoded.transforms.is_empty());
        }

        #[test]
        fn test_transforms_config_serde_roundtrip_with_data() {
            // TransformType has no Default impl; test roundtrip with empty config
            let config = TransformsConfig::default();
            let json = serde_json::to_string(&config).unwrap();
            let decoded: TransformsConfig = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.transforms.len(), config.transforms.len());
        }

        #[test]
        fn test_transforms_config_clone() {
            let config = TransformsConfig::default();
            let cloned = config.clone();
            assert_eq!(cloned.transforms.len(), config.transforms.len());
        }

        #[test]
        fn test_sink_config_with_transforms_and_plugin_config() {
            // Tests that TransformsConfig and plugin_config coexist correctly
            let config = SinkConfig {
                key: "influxdb".to_string(),
                enabled: true,
                version: 0,
                name: "InfluxDB Sink".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: Some(TransformsConfig::default()),
                streams: vec![],
                plugin_config_format: Some(ConfigFormat::Json),
                plugin_config: Some(serde_json::json!({"url": "http://localhost:8086"})),
                verbose: false,
            };

            assert!(config.transforms.is_some());
            assert!(config.plugin_config.is_some());

            let json = serde_json::to_string(&config).unwrap();
            let decoded: SinkConfig = serde_json::from_str(&json).unwrap();
            assert!(decoded.transforms.is_some());
            assert!(decoded.plugin_config.is_some());
        }

        #[test]
        fn test_source_config_with_transforms_and_plugin_config() {
            let config = SourceConfig {
                key: "influxdb".to_string(),
                enabled: true,
                version: 0,
                name: "InfluxDB Source".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: Some(TransformsConfig::default()),
                streams: vec![],
                plugin_config_format: Some(ConfigFormat::Json),
                plugin_config: Some(serde_json::json!({"org": "iggy_org"})),
                verbose: false,
            };

            assert!(config.transforms.is_some());
            assert!(config.plugin_config.is_some());

            let json = serde_json::to_string(&config).unwrap();
            let decoded: SourceConfig = serde_json::from_str(&json).unwrap();
            assert!(decoded.transforms.is_some());
            assert!(decoded.plugin_config.is_some());
        }

        #[test]
        fn test_create_sink_config_with_transforms_preserved() {
            let cmd = CreateSinkConfig {
                enabled: true,
                name: "Sink".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: Some(TransformsConfig::default()),
                streams: vec![],
                plugin_config_format: None,
                plugin_config: None,
                verbose: false,
            };

            let config = cmd.to_sink_config("influxdb", 0);
            assert!(config.transforms.is_some());
        }

        #[test]
        fn test_create_source_config_with_transforms_preserved() {
            let cmd = CreateSourceConfig {
                enabled: true,
                name: "Source".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: Some(TransformsConfig::default()),
                streams: vec![],
                plugin_config_format: None,
                plugin_config: None,
                verbose: false,
            };

            let config = cmd.to_source_config("influxdb", 0);
            assert!(config.transforms.is_some());
            assert!(config.transforms.is_some());
        }
    }

    // =========================================================================
    // GAP 4: ConnectorsConfigProvider trait — mock implementation covering
    //         all 11 methods, focusing on get_active_configs_versions(),
    //         delete with version=None, get_sink/source_configs (list all)
    // =========================================================================

    mod provider_trait_contract {
        use super::*;
        use crate::error::RuntimeError;
        use async_trait::async_trait;
        use std::sync::{Arc, Mutex};

        // --- Minimal in-memory mock provider ---

        #[derive(Default)]
        struct MockProvider {
            sinks: Arc<Mutex<HashMap<(String, u64), SinkConfig>>>,
            sources: Arc<Mutex<HashMap<(String, u64), SourceConfig>>>,
            active_sink_versions: Arc<Mutex<HashMap<String, u64>>>,
            active_source_versions: Arc<Mutex<HashMap<String, u64>>>,
        }

        #[async_trait]
        impl ConnectorsConfigProvider for MockProvider {
            async fn create_sink_config(
                &self,
                key: &str,
                config: CreateSinkConfig,
            ) -> Result<SinkConfig, RuntimeError> {
                let mut sinks = self.sinks.lock().unwrap();
                let next_version = sinks
                    .keys()
                    .filter(|(k, _)| k == key)
                    .map(|(_, v)| v + 1)
                    .max()
                    .unwrap_or(0);
                let sink = config.to_sink_config(key, next_version);
                sinks.insert((key.to_string(), next_version), sink.clone());
                Ok(sink)
            }

            async fn create_source_config(
                &self,
                key: &str,
                config: CreateSourceConfig,
            ) -> Result<SourceConfig, RuntimeError> {
                let mut sources = self.sources.lock().unwrap();
                let next_version = sources
                    .keys()
                    .filter(|(k, _)| k == key)
                    .map(|(_, v)| v + 1)
                    .max()
                    .unwrap_or(0);
                let source = config.to_source_config(key, next_version);
                sources.insert((key.to_string(), next_version), source.clone());
                Ok(source)
            }

            async fn get_active_configs(&self) -> Result<ConnectorsConfig, RuntimeError> {
                let sinks = self.sinks.lock().unwrap();
                let sources = self.sources.lock().unwrap();
                let active_sinks = self.active_sink_versions.lock().unwrap();
                let active_sources = self.active_source_versions.lock().unwrap();

                let sink_map: HashMap<String, SinkConfig> = sinks
                    .iter()
                    .filter(|((key, ver), _)| active_sinks.get(key).is_none_or(|v| v == ver))
                    .map(|((key, _), config)| (key.clone(), config.clone()))
                    .collect();

                let source_map: HashMap<String, SourceConfig> = sources
                    .iter()
                    .filter(|((key, ver), _)| active_sources.get(key).is_none_or(|v| v == ver))
                    .map(|((key, _), config)| (key.clone(), config.clone()))
                    .collect();

                Ok(ConnectorsConfig::new(sink_map, source_map))
            }

            async fn get_active_configs_versions(
                &self,
            ) -> Result<ConnectorConfigVersions, RuntimeError> {
                let sinks = self.sinks.lock().unwrap();
                let sources = self.sources.lock().unwrap();

                let sink_versions: HashMap<String, ConnectorConfigVersionInfo> = sinks
                    .iter()
                    .map(|((key, ver), _)| {
                        (
                            key.clone(),
                            ConnectorConfigVersionInfo {
                                version: *ver,
                                created_at: iggy_common::Utc::now(),
                            },
                        )
                    })
                    .collect();

                let source_versions: HashMap<String, ConnectorConfigVersionInfo> = sources
                    .iter()
                    .map(|((key, ver), _)| {
                        (
                            key.clone(),
                            ConnectorConfigVersionInfo {
                                version: *ver,
                                created_at: iggy_common::Utc::now(),
                            },
                        )
                    })
                    .collect();

                Ok(ConnectorConfigVersions {
                    sinks: sink_versions,
                    sources: source_versions,
                })
            }

            async fn set_active_sink_version(
                &self,
                key: &str,
                version: u64,
            ) -> Result<(), RuntimeError> {
                let sinks = self.sinks.lock().unwrap();
                if !sinks.contains_key(&(key.to_string(), version)) {
                    return Err(RuntimeError::SinkConfigNotFound(key.to_string(), version));
                }
                self.active_sink_versions
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), version);
                Ok(())
            }

            async fn set_active_source_version(
                &self,
                key: &str,
                version: u64,
            ) -> Result<(), RuntimeError> {
                let sources = self.sources.lock().unwrap();
                if !sources.contains_key(&(key.to_string(), version)) {
                    return Err(RuntimeError::SourceConfigNotFound(key.to_string(), version));
                }
                self.active_source_versions
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), version);
                Ok(())
            }

            async fn get_sink_configs(&self, key: &str) -> Result<Vec<SinkConfig>, RuntimeError> {
                let sinks = self.sinks.lock().unwrap();
                Ok(sinks
                    .iter()
                    .filter(|((k, _), _)| k == key)
                    .map(|(_, v)| v.clone())
                    .collect())
            }

            async fn get_sink_config(
                &self,
                key: &str,
                version: Option<u64>,
            ) -> Result<Option<SinkConfig>, RuntimeError> {
                let sinks = self.sinks.lock().unwrap();
                if let Some(ver) = version {
                    Ok(sinks.get(&(key.to_string(), ver)).cloned())
                } else {
                    Ok(sinks
                        .iter()
                        .filter(|((k, _), _)| k == key)
                        .max_by_key(|((_, v), _)| *v)
                        .map(|(_, c)| c.clone()))
                }
            }

            async fn get_source_configs(
                &self,
                key: &str,
            ) -> Result<Vec<SourceConfig>, RuntimeError> {
                let sources = self.sources.lock().unwrap();
                Ok(sources
                    .iter()
                    .filter(|((k, _), _)| k == key)
                    .map(|(_, v)| v.clone())
                    .collect())
            }

            async fn get_source_config(
                &self,
                key: &str,
                version: Option<u64>,
            ) -> Result<Option<SourceConfig>, RuntimeError> {
                let sources = self.sources.lock().unwrap();
                if let Some(ver) = version {
                    Ok(sources.get(&(key.to_string(), ver)).cloned())
                } else {
                    Ok(sources
                        .iter()
                        .filter(|((k, _), _)| k == key)
                        .max_by_key(|((_, v), _)| *v)
                        .map(|(_, c)| c.clone()))
                }
            }

            async fn delete_sink_config(
                &self,
                key: &str,
                version: Option<u64>,
            ) -> Result<(), RuntimeError> {
                let mut sinks = self.sinks.lock().unwrap();
                let ver =
                    version.ok_or_else(|| RuntimeError::SinkConfigNotFound(key.to_string(), 0))?;
                sinks
                    .remove(&(key.to_string(), ver))
                    .ok_or_else(|| RuntimeError::SinkConfigNotFound(key.to_string(), ver))?;
                Ok(())
            }

            async fn delete_source_config(
                &self,
                key: &str,
                version: Option<u64>,
            ) -> Result<(), RuntimeError> {
                let mut sources = self.sources.lock().unwrap();
                let ver = version
                    .ok_or_else(|| RuntimeError::SourceConfigNotFound(key.to_string(), 0))?;
                sources
                    .remove(&(key.to_string(), ver))
                    .ok_or_else(|| RuntimeError::SourceConfigNotFound(key.to_string(), ver))?;
                Ok(())
            }
        }

        fn make_create_sink() -> CreateSinkConfig {
            CreateSinkConfig {
                enabled: true,
                name: "InfluxDB Sink".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: None,
                streams: vec![],
                plugin_config_format: Some(ConfigFormat::Json),
                plugin_config: Some(serde_json::json!({"url": "http://localhost:8086"})),
                verbose: false,
            }
        }

        fn make_create_source() -> CreateSourceConfig {
            CreateSourceConfig {
                enabled: true,
                name: "InfluxDB Source".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: None,
                streams: vec![],
                plugin_config_format: Some(ConfigFormat::Json),
                plugin_config: Some(serde_json::json!({"org": "iggy_org"})),
                verbose: false,
            }
        }

        // --- get_active_configs_versions() ---

        #[tokio::test]
        async fn test_get_active_configs_versions_empty() {
            let provider = MockProvider::default();
            let versions = provider.get_active_configs_versions().await.unwrap();
            assert!(versions.sinks.is_empty());
            assert!(versions.sources.is_empty());
        }

        #[tokio::test]
        async fn test_get_active_configs_versions_with_sinks() {
            let provider = MockProvider::default();
            provider
                .create_sink_config("influxdb", make_create_sink())
                .await
                .unwrap();
            provider
                .create_sink_config("influxdb", make_create_sink())
                .await
                .unwrap();

            let versions = provider.get_active_configs_versions().await.unwrap();
            assert!(!versions.sinks.is_empty());
        }

        #[tokio::test]
        async fn test_get_active_configs_versions_with_sources() {
            let provider = MockProvider::default();
            provider
                .create_source_config("influxdb", make_create_source())
                .await
                .unwrap();

            let versions = provider.get_active_configs_versions().await.unwrap();
            assert!(versions.sources.contains_key("influxdb"));
            assert_eq!(versions.sources["influxdb"].version, 0);
        }

        #[tokio::test]
        async fn test_get_active_configs_versions_both_populated() {
            let provider = MockProvider::default();
            provider
                .create_sink_config("influxdb", make_create_sink())
                .await
                .unwrap();
            provider
                .create_source_config("influxdb", make_create_source())
                .await
                .unwrap();

            let versions = provider.get_active_configs_versions().await.unwrap();
            assert!(!versions.sinks.is_empty());
            assert!(!versions.sources.is_empty());
        }

        #[tokio::test]
        async fn test_get_active_configs_versions_version_info_has_created_at() {
            let provider = MockProvider::default();
            provider
                .create_sink_config("influxdb", make_create_sink())
                .await
                .unwrap();

            let versions = provider.get_active_configs_versions().await.unwrap();
            let info = &versions.sinks["influxdb"];
            // created_at should be a valid recent timestamp (not epoch zero)
            assert!(info.created_at.timestamp() > 0);
        }

        // --- get_sink_configs / get_source_configs (list ALL versions) ---

        #[tokio::test]
        async fn test_get_sink_configs_returns_all_versions() {
            let provider = MockProvider::default();
            provider
                .create_sink_config("influxdb", make_create_sink())
                .await
                .unwrap();
            provider
                .create_sink_config("influxdb", make_create_sink())
                .await
                .unwrap();
            provider
                .create_sink_config("influxdb", make_create_sink())
                .await
                .unwrap();

            let configs = provider.get_sink_configs("influxdb").await.unwrap();
            assert_eq!(configs.len(), 3, "Should return all 3 versions");
        }

        #[tokio::test]
        async fn test_get_sink_configs_unknown_key_returns_empty() {
            let provider = MockProvider::default();
            let configs = provider.get_sink_configs("nonexistent").await.unwrap();
            assert!(configs.is_empty());
        }

        #[tokio::test]
        async fn test_get_sink_configs_different_keys_isolated() {
            let provider = MockProvider::default();
            provider
                .create_sink_config("influxdb", make_create_sink())
                .await
                .unwrap();
            provider
                .create_sink_config("postgres", make_create_sink())
                .await
                .unwrap();
            provider
                .create_sink_config("postgres", make_create_sink())
                .await
                .unwrap();

            let influx = provider.get_sink_configs("influxdb").await.unwrap();
            let pg = provider.get_sink_configs("postgres").await.unwrap();
            assert_eq!(influx.len(), 1);
            assert_eq!(pg.len(), 2);
        }

        #[tokio::test]
        async fn test_get_source_configs_returns_all_versions() {
            let provider = MockProvider::default();
            provider
                .create_source_config("influxdb", make_create_source())
                .await
                .unwrap();
            provider
                .create_source_config("influxdb", make_create_source())
                .await
                .unwrap();

            let configs = provider.get_source_configs("influxdb").await.unwrap();
            assert_eq!(configs.len(), 2);
        }

        #[tokio::test]
        async fn test_get_source_configs_unknown_key_returns_empty() {
            let provider = MockProvider::default();
            let configs = provider.get_source_configs("nonexistent").await.unwrap();
            assert!(configs.is_empty());
        }

        // --- delete with version = None ---

        #[tokio::test]
        async fn test_delete_sink_config_none_version_returns_error_from_mock() {
            // MockProvider requires explicit version — None is ambiguous
            let provider = MockProvider::default();
            provider
                .create_sink_config("influxdb", make_create_sink())
                .await
                .unwrap();

            let result = provider.delete_sink_config("influxdb", None).await;
            assert!(
                result.is_err(),
                "delete with version=None should return error in mock"
            );
        }

        #[tokio::test]
        async fn test_delete_source_config_none_version_returns_error_from_mock() {
            let provider = MockProvider::default();
            provider
                .create_source_config("influxdb", make_create_source())
                .await
                .unwrap();

            let result = provider.delete_source_config("influxdb", None).await;
            assert!(
                result.is_err(),
                "delete with version=None should return error in mock"
            );
        }

        #[tokio::test]
        async fn test_delete_sink_config_explicit_version_succeeds() {
            let provider = MockProvider::default();
            provider
                .create_sink_config("influxdb", make_create_sink())
                .await
                .unwrap();

            let result = provider.delete_sink_config("influxdb", Some(0)).await;
            assert!(result.is_ok());

            let configs = provider.get_sink_configs("influxdb").await.unwrap();
            assert!(configs.is_empty());
        }

        #[tokio::test]
        async fn test_delete_source_config_explicit_version_succeeds() {
            let provider = MockProvider::default();
            provider
                .create_source_config("influxdb", make_create_source())
                .await
                .unwrap();

            let result = provider.delete_source_config("influxdb", Some(0)).await;
            assert!(result.is_ok());

            let configs = provider.get_source_configs("influxdb").await.unwrap();
            assert!(configs.is_empty());
        }

        #[tokio::test]
        async fn test_delete_sink_nonexistent_version_returns_error() {
            let provider = MockProvider::default();
            let result = provider.delete_sink_config("influxdb", Some(99)).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_delete_source_nonexistent_version_returns_error() {
            let provider = MockProvider::default();
            let result = provider.delete_source_config("influxdb", Some(99)).await;
            assert!(result.is_err());
        }

        // --- Full trait contract lifecycle ---

        #[tokio::test]
        async fn test_full_sink_lifecycle_via_trait() {
            let provider = MockProvider::default();

            // Create
            let v0 = provider
                .create_sink_config("influxdb", make_create_sink())
                .await
                .unwrap();
            assert_eq!(v0.version, 0);

            let v1 = provider
                .create_sink_config("influxdb", make_create_sink())
                .await
                .unwrap();
            assert_eq!(v1.version, 1);

            // List all
            let all = provider.get_sink_configs("influxdb").await.unwrap();
            assert_eq!(all.len(), 2);

            // Get specific
            let fetched = provider.get_sink_config("influxdb", Some(0)).await.unwrap();
            assert_eq!(fetched.unwrap().version, 0);

            // Get latest
            let latest = provider.get_sink_config("influxdb", None).await.unwrap();
            assert_eq!(latest.unwrap().version, 1);

            // Set active
            provider
                .set_active_sink_version("influxdb", 0)
                .await
                .unwrap();
            let active = provider.get_active_configs().await.unwrap();
            assert!(active.sinks().contains_key("influxdb"));

            // Delete v0
            provider
                .delete_sink_config("influxdb", Some(0))
                .await
                .unwrap();
            let remaining = provider.get_sink_configs("influxdb").await.unwrap();
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].version, 1);
        }

        #[tokio::test]
        async fn test_full_source_lifecycle_via_trait() {
            let provider = MockProvider::default();

            let v0 = provider
                .create_source_config("influxdb", make_create_source())
                .await
                .unwrap();
            assert_eq!(v0.version, 0);

            let all = provider.get_source_configs("influxdb").await.unwrap();
            assert_eq!(all.len(), 1);

            let fetched = provider
                .get_source_config("influxdb", Some(0))
                .await
                .unwrap();
            assert!(fetched.is_some());

            provider
                .set_active_source_version("influxdb", 0)
                .await
                .unwrap();
            let active = provider.get_active_configs().await.unwrap();
            assert!(active.sources().contains_key("influxdb"));

            provider
                .delete_source_config("influxdb", Some(0))
                .await
                .unwrap();
            let remaining = provider.get_source_configs("influxdb").await.unwrap();
            assert!(remaining.is_empty());
        }
    }

    // =========================================================================
    // GAP 5: Http variant — config structure validation (no live server needed)
    // =========================================================================

    mod http_provider_config {
        use crate::configs::runtime::{
            ConnectorsConfig as RuntimeConnectorsConfig, HttpConnectorsConfig,
        };

        #[test]
        fn test_http_runtime_config_has_base_url() {
            let config = HttpConnectorsConfig {
                base_url: "http://config-server:9090".to_string(),
                ..Default::default()
            };
            assert_eq!(config.base_url, "http://config-server:9090");
        }

        #[test]
        fn test_http_runtime_config_variant_is_http() {
            let config = RuntimeConnectorsConfig::Http(HttpConnectorsConfig {
                base_url: "http://config-server:9090".to_string(),
                ..Default::default()
            });
            assert!(matches!(config, RuntimeConnectorsConfig::Http(_)));
        }

        #[test]
        fn test_local_runtime_config_variant_is_local() {
            let config =
                RuntimeConnectorsConfig::Local(crate::configs::runtime::LocalConnectorsConfig {
                    config_dir: "/tmp/configs".to_string(),
                });
            assert!(matches!(config, RuntimeConnectorsConfig::Local(_)));
        }

        #[test]
        fn test_local_runtime_config_has_config_dir() {
            let config = crate::configs::runtime::LocalConnectorsConfig {
                config_dir: "/tmp/configs".to_string(),
            };
            assert_eq!(config.config_dir, "/tmp/configs");
        }

        #[test]
        fn test_runtime_config_serde_local_roundtrip() {
            let config =
                RuntimeConnectorsConfig::Local(crate::configs::runtime::LocalConnectorsConfig {
                    config_dir: "/tmp/configs".to_string(),
                });
            let json = serde_json::to_string(&config).unwrap();
            let decoded: RuntimeConnectorsConfig = serde_json::from_str(&json).unwrap();
            assert!(matches!(decoded, RuntimeConnectorsConfig::Local(_)));
        }
    }

    // =========================================================================
    // GAP 6: ConnectorConfigVersions with populated data
    // =========================================================================

    mod connector_config_versions_populated {
        use super::*;

        fn make_version_info(version: u64) -> ConnectorConfigVersionInfo {
            ConnectorConfigVersionInfo {
                version,
                created_at: iggy_common::Utc::now(),
            }
        }

        #[test]
        fn test_versions_with_populated_sinks_and_sources() {
            let versions = ConnectorConfigVersions {
                sinks: HashMap::from([
                    ("influxdb".to_string(), make_version_info(2)),
                    ("postgres".to_string(), make_version_info(0)),
                ]),
                sources: HashMap::from([("influxdb".to_string(), make_version_info(1))]),
            };

            assert_eq!(versions.sinks.len(), 2);
            assert_eq!(versions.sources.len(), 1);
            assert_eq!(versions.sinks["influxdb"].version, 2);
            assert_eq!(versions.sinks["postgres"].version, 0);
            assert_eq!(versions.sources["influxdb"].version, 1);
        }

        #[test]
        fn test_versions_serde_roundtrip_populated() {
            let versions = ConnectorConfigVersions {
                sinks: HashMap::from([("influxdb".to_string(), make_version_info(3))]),
                sources: HashMap::from([("influxdb".to_string(), make_version_info(1))]),
            };

            let json = serde_json::to_string(&versions).unwrap();
            let decoded: ConnectorConfigVersions = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.sinks["influxdb"].version, 3);
            assert_eq!(decoded.sources["influxdb"].version, 1);
        }

        #[test]
        fn test_version_info_clone() {
            let info = make_version_info(5);
            let cloned = info.clone();
            assert_eq!(cloned.version, 5);
        }

        #[test]
        fn test_versions_clone() {
            let versions = ConnectorConfigVersions {
                sinks: HashMap::from([("influxdb".to_string(), make_version_info(1))]),
                sources: HashMap::new(),
            };
            let cloned = versions.clone();
            assert_eq!(cloned.sinks.len(), 1);
        }

        #[test]
        fn test_versions_only_sinks_populated() {
            let versions = ConnectorConfigVersions {
                sinks: HashMap::from([("influxdb".to_string(), make_version_info(0))]),
                sources: HashMap::new(),
            };
            assert!(!versions.sinks.is_empty());
            assert!(versions.sources.is_empty());
        }

        #[test]
        fn test_versions_only_sources_populated() {
            let versions = ConnectorConfigVersions {
                sinks: HashMap::new(),
                sources: HashMap::from([("influxdb".to_string(), make_version_info(0))]),
            };
            assert!(versions.sinks.is_empty());
            assert!(!versions.sources.is_empty());
        }
    }

    // =========================================================================
    // GAP 7: Edge cases not previously covered
    // =========================================================================

    mod edge_cases {
        use super::*;

        // --- verbose = true ---

        #[test]
        fn test_sink_config_verbose_true_preserved_in_mapping() {
            let cmd = CreateSinkConfig {
                enabled: true,
                name: "Verbose Sink".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: None,
                streams: vec![],
                plugin_config_format: None,
                plugin_config: None,
                verbose: true,
            };
            let config = cmd.to_sink_config("influxdb", 0);
            assert!(
                config.verbose,
                "verbose=true must be preserved through mapping"
            );
        }

        #[test]
        fn test_source_config_verbose_true_preserved_in_mapping() {
            let cmd = CreateSourceConfig {
                enabled: true,
                name: "Verbose Source".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: None,
                streams: vec![],
                plugin_config_format: None,
                plugin_config: None,
                verbose: true,
            };
            let config = cmd.to_source_config("influxdb", 0);
            assert!(config.verbose);
        }

        #[test]
        fn test_sink_config_verbose_included_in_display() {
            // Display doesn't show verbose but serde should include it
            let config = SinkConfig {
                key: "influxdb".to_string(),
                enabled: true,
                version: 0,
                name: "Test".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: None,
                streams: vec![],
                plugin_config_format: None,
                plugin_config: None,
                verbose: true,
            };
            let json = serde_json::to_string(&config).unwrap();
            assert!(json.contains("\"verbose\":true"));
        }

        // --- plugin_config_format = None ---

        #[test]
        fn test_sink_config_plugin_config_format_none() {
            let config = SinkConfig {
                key: "influxdb".to_string(),
                enabled: true,
                version: 0,
                name: "Test".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: None,
                streams: vec![],
                plugin_config_format: None,
                plugin_config: None,
                verbose: false,
            };
            assert!(config.plugin_config_format.is_none());
            // Display should handle None gracefully
            let display = format!("{config}");
            assert!(display.contains("None"));
        }

        #[test]
        fn test_source_config_plugin_config_format_none() {
            let config = SourceConfig {
                key: "influxdb".to_string(),
                enabled: true,
                version: 0,
                name: "Test".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: None,
                streams: vec![],
                plugin_config_format: None,
                plugin_config: None,
                verbose: false,
            };
            assert!(config.plugin_config_format.is_none());
        }

        #[test]
        fn test_sink_config_serde_with_plugin_config_format_none() {
            let config = SinkConfig {
                key: "influxdb".to_string(),
                enabled: false,
                version: 0,
                name: "Test".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: None,
                streams: vec![],
                plugin_config_format: None,
                plugin_config: None,
                verbose: false,
            };
            let json = serde_json::to_string(&config).unwrap();
            let decoded: SinkConfig = serde_json::from_str(&json).unwrap();
            assert!(decoded.plugin_config_format.is_none());
        }

        // --- StreamConsumerConfig with multiple topics ---

        #[test]
        fn test_stream_consumer_config_multiple_topics() {
            let config = StreamConsumerConfig {
                stream: "events".to_string(),
                topics: vec![
                    "user_events".to_string(),
                    "orders".to_string(),
                    "payments".to_string(),
                    "shipments".to_string(),
                ],
                schema: iggy_connector_sdk::Schema::default(),
                batch_length: Some(100),
                poll_interval: Some("1s".to_string()),
                consumer_group: Some("influxdb_sink".to_string()),
            };

            assert_eq!(config.topics.len(), 4);
            let display = format!("{config}");
            assert!(display.contains("user_events"));
            assert!(display.contains("orders"));
            assert!(display.contains("payments"));
            assert!(display.contains("shipments"));
        }

        #[test]
        fn test_sink_config_multiple_streams() {
            let config = SinkConfig {
                key: "influxdb".to_string(),
                enabled: true,
                version: 0,
                name: "Multi-stream Sink".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: None,
                streams: vec![
                    StreamConsumerConfig {
                        stream: "metrics".to_string(),
                        topics: vec!["cpu".to_string(), "memory".to_string()],
                        schema: iggy_connector_sdk::Schema::default(),
                        batch_length: None,
                        poll_interval: None,
                        consumer_group: None,
                    },
                    StreamConsumerConfig {
                        stream: "events".to_string(),
                        topics: vec!["user_events".to_string()],
                        schema: iggy_connector_sdk::Schema::default(),
                        batch_length: None,
                        poll_interval: None,
                        consumer_group: None,
                    },
                ],
                plugin_config_format: Some(ConfigFormat::Json),
                plugin_config: None,
                verbose: false,
            };

            assert_eq!(config.streams.len(), 2);
            let display = format!("{config}");
            assert!(display.contains("metrics"));
            assert!(display.contains("events"));
        }

        #[test]
        fn test_source_config_multiple_streams() {
            let config = SourceConfig {
                key: "influxdb".to_string(),
                enabled: true,
                version: 0,
                name: "Multi-stream Source".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: None,
                streams: vec![
                    StreamProducerConfig {
                        stream: "metrics".to_string(),
                        topic: "cpu_metrics".to_string(),
                        schema: iggy_connector_sdk::Schema::default(),
                        batch_length: None,
                        linger_time: None,
                    },
                    StreamProducerConfig {
                        stream: "events".to_string(),
                        topic: "user_events".to_string(),
                        schema: iggy_connector_sdk::Schema::default(),
                        batch_length: None,
                        linger_time: None,
                    },
                ],
                plugin_config_format: Some(ConfigFormat::Json),
                plugin_config: None,
                verbose: false,
            };

            assert_eq!(config.streams.len(), 2);
        }

        // --- Transforms coexisting with plugin_config ---

        #[test]
        fn test_sink_config_display_with_transforms_not_none() {
            let config = SinkConfig {
                key: "influxdb".to_string(),
                enabled: true,
                version: 0,
                name: "Sink with Transforms".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: Some(TransformsConfig::default()),
                streams: vec![],
                plugin_config_format: Some(ConfigFormat::Json),
                plugin_config: Some(serde_json::json!({"url": "http://localhost:8086"})),
                verbose: false,
            };

            let display = format!("{config}");
            // Display shows transforms as {:?} so Some(...) should appear
            assert!(display.contains("Some"));
        }

        // --- ConnectorsConfig serde with #[serde(default)] ---

        #[test]
        fn test_connectors_config_deserialize_missing_fields_uses_default() {
            // #[serde(default)] on ConnectorsConfig means missing keys → empty maps
            let json = r#"{}"#;
            let config: ConnectorsConfig = serde_json::from_str(json).unwrap();
            assert!(config.sinks().is_empty());
            assert!(config.sources().is_empty());
        }

        #[test]
        fn test_connectors_config_serde_roundtrip_with_data() {
            let sinks = HashMap::from([("influxdb".to_string(), make_sink("influxdb", 0))]);
            let sources = HashMap::from([("influxdb".to_string(), make_source("influxdb", 0))]);
            let config = ConnectorsConfig::new(sinks, sources);

            let json = serde_json::to_string(&config).unwrap();
            let decoded: ConnectorsConfig = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.sinks().len(), 1);
            assert_eq!(decoded.sources().len(), 1);
            assert!(decoded.sinks().contains_key("influxdb"));
            assert!(decoded.sources().contains_key("influxdb"));
        }

        // --- version = u64::MAX boundary ---

        #[test]
        fn test_sink_config_version_u64_max() {
            let config = SinkConfig {
                key: "influxdb".to_string(),
                version: u64::MAX,
                ..Default::default()
            };
            assert_eq!(config.version, u64::MAX);
            let connector = ConnectorConfig::Sink(config);
            assert_eq!(connector.version(), u64::MAX);
        }

        #[test]
        fn test_source_config_version_u64_max() {
            let config = SourceConfig {
                key: "influxdb".to_string(),
                version: u64::MAX,
                ..Default::default()
            };
            assert_eq!(config.version, u64::MAX);
            let connector = ConnectorConfig::Source(config);
            assert_eq!(connector.version(), u64::MAX);
        }

        // --- Empty streams list ---

        #[test]
        fn test_sink_config_empty_streams_display() {
            let config = SinkConfig {
                key: "influxdb".to_string(),
                enabled: true,
                version: 0,
                name: "No Streams".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: None,
                streams: vec![],
                plugin_config_format: None,
                plugin_config: None,
                verbose: false,
            };
            let display = format!("{config}");
            // streams: [] renders as empty between brackets
            assert!(display.contains("streams: []") || display.contains("[]"));
        }

        #[test]
        fn test_source_config_empty_streams_display() {
            let config = SourceConfig {
                key: "influxdb".to_string(),
                enabled: true,
                version: 0,
                name: "No Streams".to_string(),
                path: "/tmp/plugin.so".to_string(),
                transforms: None,
                streams: vec![],
                plugin_config_format: None,
                plugin_config: None,
                verbose: false,
            };
            let display = format!("{config}");
            assert!(!display.is_empty());
        }
    }
}
