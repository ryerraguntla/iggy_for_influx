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

use crate::configs::connectors::{
    ConnectorConfig, ConnectorConfigVersionInfo, ConnectorConfigVersions, ConnectorsConfig,
    ConnectorsConfigProvider, CreateSinkConfig, CreateSourceConfig, SinkConfig, SourceConfig,
};
use crate::error::RuntimeError;
use ::configs::{ConfigProvider, FileConfigProvider, TypedEnvProvider};
use async_trait::async_trait;
use dashmap::DashMap;
use figment::value::Dict;
use figment::{Metadata, Profile, Provider};
use iggy_common::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tracing::{debug, info, warn};

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
struct ConnectorId {
    key: String,
    version: u64,
}

impl ConnectorId {
    fn to_filename_key(&self) -> String {
        format!("{}_{}", self.key, self.version)
    }
}

impl From<&ConnectorConfig> for ConnectorId {
    fn from(value: &ConnectorConfig) -> Self {
        match value {
            ConnectorConfig::Sink(config) => ConnectorId {
                key: config.key.clone(),
                version: config.version,
            },
            ConnectorConfig::Source(config) => ConnectorId {
                key: config.key.clone(),
                version: config.version,
            },
        }
    }
}

#[derive(Clone)]
struct SinkConfigFile {
    config: SinkConfig,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
    path: String,
}

#[derive(Clone)]
struct SourceConfigFile {
    config: SourceConfig,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
    path: String,
}

#[derive(Default)]
struct ImportedConfigurations {
    sinks: DashMap<ConnectorId, SinkConfigFile>,
    sources: DashMap<ConnectorId, SourceConfigFile>,
}

impl ImportedConfigurations {
    fn sinks(&self) -> &DashMap<ConnectorId, SinkConfigFile> {
        &self.sinks
    }

    fn sinks_grouped_by_key(&self) -> HashMap<String, Vec<SinkConfigFile>> {
        let mut grouped: HashMap<String, Vec<SinkConfigFile>> = HashMap::new();
        for entry in self.sinks.iter() {
            let key = entry.key().key.clone();
            grouped.entry(key).or_default().push(entry.value().clone());
        }
        grouped
    }

    fn sources(&self) -> &DashMap<ConnectorId, SourceConfigFile> {
        &self.sources
    }

    fn sources_grouped_by_key(&self) -> HashMap<String, Vec<SourceConfigFile>> {
        let mut grouped: HashMap<String, Vec<SourceConfigFile>> = HashMap::new();
        for entry in self.sources.iter() {
            let key = entry.key().key.clone();
            grouped.entry(key).or_default().push(entry.value().clone());
        }
        grouped
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct ActiveConfigVersions {
    #[serde(default)]
    sinks: HashMap<String, u64>,
    #[serde(default)]
    sources: HashMap<String, u64>,
}

pub trait ProviderState {}

pub struct Created {}

impl ProviderState for Created {}

#[derive(Default)]
pub struct Initialized {
    connectors_config: ImportedConfigurations,
}

impl ProviderState for Initialized {}

pub struct LocalConnectorsConfigProvider<S: ProviderState> {
    config_dir: String,
    state: S,
}

impl LocalConnectorsConfigProvider<Created> {
    pub fn new(config_dir: &str) -> Self {
        Self {
            config_dir: config_dir.to_owned(),
            state: Created {},
        }
    }

    pub async fn init(&self) -> Result<LocalConnectorsConfigProvider<Initialized>, RuntimeError> {
        if self.config_dir.is_empty() {
            return Err(RuntimeError::InvalidConfiguration(
                "Connectors configuration directory not provided".to_string(),
            ));
        }
        if !std::fs::exists(&self.config_dir)? {
            warn!(
                "Connectors configuration directory does not exist: {}",
                self.config_dir
            );
            std::fs::create_dir_all(&self.config_dir)?;
            return Ok(LocalConnectorsConfigProvider {
                config_dir: self.config_dir.clone(),
                state: Initialized::default(),
            });
        }

        let sinks: DashMap<ConnectorId, SinkConfigFile> = DashMap::new();
        let sources: DashMap<ConnectorId, SourceConfigFile> = DashMap::new();
        let cwd = match std::env::current_dir() {
            Ok(path) => path.display().to_string(),
            Err(_) => "unknown".to_string(),
        };
        info!(
            "Loading connectors configuration from: {}, current directory: {cwd}",
            self.config_dir
        );
        let entries = std::fs::read_dir(&self.config_dir)?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                if path.extension().and_then(|ext| ext.to_str()) != Some("toml") {
                    debug!("Skipping non-TOML file: {}", path.display());
                    continue;
                }

                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    if file_name.starts_with('.') {
                        debug!("Skipping hidden file: {}", path.display());
                        continue;
                    }
                    let file_name_lower = file_name.to_lowercase();
                    if file_name_lower == "cargo.toml" {
                        debug!("Skipping Cargo.toml: {}", path.display());
                        continue;
                    }
                }

                info!("Loading connector configuration from: {}", path.display());
                let base_config = Self::read_base_config(&path)?;
                debug!("Loaded base configuration: {:?}", base_config);
                let path = path
                    .to_str()
                    .ok_or_else(|| {
                        RuntimeError::InvalidConfiguration(format!(
                            "Non-UTF8 connector config path: {}",
                            path.display()
                        ))
                    })?
                    .to_string();
                let connector_config: ConnectorConfig =
                    Self::create_file_config_provider(path.clone(), &base_config)
                        .load_config()
                        .await
                        .map_err(|e| {
                            RuntimeError::InvalidConfiguration(format!(
                                "Failed to load connector configuration from '{path}': {e}"
                            ))
                        })?;

                let metadata = entry.metadata()?;
                let created_at: DateTime<Utc> = metadata
                    .created()
                    .or_else(|_| metadata.modified())
                    .map(Into::into)
                    .unwrap_or_else(|_| {
                        warn!(
                            "Could not read created or modified time for '{path}', using current time",
                        );
                        Utc::now()
                    });
                let connector_id: ConnectorId = (&connector_config).into();
                let version = connector_config.version();

                match connector_config {
                    ConnectorConfig::Sink(mut sink_config) => {
                        Self::apply_plugin_config_env_overrides(
                            &mut sink_config.plugin_config,
                            &base_config,
                        );
                        sinks.insert(
                            connector_id,
                            SinkConfigFile {
                                config: sink_config,
                                created_at,
                                path,
                            },
                        );
                    }
                    ConnectorConfig::Source(mut source_config) => {
                        Self::apply_plugin_config_env_overrides(
                            &mut source_config.plugin_config,
                            &base_config,
                        );
                        sources.insert(
                            connector_id,
                            SourceConfigFile {
                                config: source_config,
                                created_at,
                                path,
                            },
                        );
                    }
                }

                info!(
                    "Loaded connector configuration with key: {}, version: {}, created at {}",
                    base_config.key(),
                    version,
                    created_at.to_rfc3339()
                );
            }
        }
        Ok(LocalConnectorsConfigProvider {
            config_dir: self.config_dir.clone(),
            state: Initialized {
                connectors_config: ImportedConfigurations { sinks, sources },
            },
        })
    }
}

impl LocalConnectorsConfigProvider<Initialized> {
    fn active_versions_file_path(&self) -> String {
        format!("{}/.active_versions.toml", self.config_dir)
    }

    fn load_active_versions(&self) -> ActiveConfigVersions {
        let path = self.active_versions_file_path();
        if !Path::new(&path).exists() {
            return ActiveConfigVersions::default();
        }

        match std::fs::read(&path) {
            Ok(data) => toml::from_slice(&data).unwrap_or_else(|err| {
                warn!(
                    "Failed to parse active versions file '{}': {}",
                    path,
                    err.message()
                );
                ActiveConfigVersions::default()
            }),
            Err(err) => {
                warn!("Failed to read active versions file '{}': {}", path, err);
                ActiveConfigVersions::default()
            }
        }
    }

    fn save_active_versions(&self, versions: &ActiveConfigVersions) -> Result<(), RuntimeError> {
        let path = self.active_versions_file_path();
        let content = toml::to_string(versions).map_err(|err| {
            RuntimeError::InvalidConfiguration(format!(
                "Failed to serialize active versions: {}",
                err
            ))
        })?;
        std::fs::write(&path, content)?;
        Ok(())
    }
}

impl<S: ProviderState> LocalConnectorsConfigProvider<S> {
    fn create_file_config_provider(
        path: String,
        base_config: &BaseConnectorConfig,
    ) -> FileConfigProvider<ConnectorEnvProvider> {
        FileConfigProvider::new(
            path,
            ConnectorEnvProvider::with_connector_base_config(base_config),
            false,
            None,
        )
    }

    fn read_base_config(path: &Path) -> Result<BaseConnectorConfig, RuntimeError> {
        let config_data = std::fs::read(path)?;
        toml::from_slice(&config_data).map_err(|err| {
            RuntimeError::InvalidConfiguration(format!(
                "parsing TOML file '{}' raised an error: {}",
                path.display(),
                err.message()
            ))
        })
    }

    fn apply_plugin_config_env_overrides(
        plugin_config: &mut Option<serde_json::Value>,
        base_config: &BaseConnectorConfig,
    ) {
        let connector_type = base_config.connector_type().to_uppercase();
        let key = base_config.key().to_uppercase();
        let prefix = format!("IGGY_CONNECTORS_{connector_type}_{key}_PLUGIN_CONFIG_");

        for (env_key, env_value) in std::env::vars() {
            let env_key_upper = env_key.to_uppercase();
            if !env_key_upper.starts_with(&prefix) {
                continue;
            }

            let field_path = &env_key_upper[prefix.len()..];
            let field_name = field_path.to_lowercase();
            let parsed_value = ::configs::parse_env_value_to_json(&env_value);

            let config = plugin_config.get_or_insert_with(|| serde_json::json!({}));
            if let serde_json::Value::Object(map) = config {
                map.insert(field_name, parsed_value);
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum BaseConnectorConfig {
    Sink { key: String },
    Source { key: String },
}

impl BaseConnectorConfig {
    fn key(&self) -> &str {
        match self {
            BaseConnectorConfig::Sink { key, .. } => key,
            BaseConnectorConfig::Source { key, .. } => key,
        }
    }

    fn connector_type(&self) -> &str {
        match self {
            BaseConnectorConfig::Sink { .. } => "sink",
            BaseConnectorConfig::Source { .. } => "source",
        }
    }
}

#[async_trait]
impl ConnectorsConfigProvider for LocalConnectorsConfigProvider<Initialized> {
    async fn create_sink_config(
        &self,
        key: &str,
        cmd: CreateSinkConfig,
    ) -> Result<SinkConfig, RuntimeError> {
        let sinks = self.state.connectors_config.sinks();
        let next_version = sinks
            .iter()
            .filter(|entry| entry.key().key == key)
            .max_by_key(|entry| entry.config.version)
            .map(|entry| entry.config.version + 1)
            .unwrap_or(0);

        let config = cmd.to_sink_config(key, next_version);
        let connector_config = ConnectorConfig::Sink(config.clone());
        let connector_id: ConnectorId = (&connector_config).into();

        let path = format!(
            "{}/sink_{}.toml",
            self.config_dir,
            connector_id.to_filename_key()
        );
        std::fs::write(&path, toml::to_string(&connector_config).unwrap())?;
        sinks.insert(
            connector_id,
            SinkConfigFile {
                config: config.clone(),
                created_at: Utc::now(),
                path: path.clone(),
            },
        );

        Ok(config)
    }

    async fn create_source_config(
        &self,
        key: &str,
        cmd: CreateSourceConfig,
    ) -> Result<SourceConfig, RuntimeError> {
        let sources = &self.state.connectors_config.sources;
        let next_version = sources
            .iter()
            .filter(|entry| entry.key().key == key)
            .max_by_key(|entry| entry.config.version)
            .map(|entry| entry.config.version + 1)
            .unwrap_or(0);

        let config = cmd.to_source_config(key, next_version);
        let connector_config = ConnectorConfig::Source(config.clone());
        let connector_id: ConnectorId = (&connector_config).into();

        let path = format!(
            "{}/source_{}.toml",
            self.config_dir,
            connector_id.to_filename_key()
        );
        std::fs::write(&path, toml::to_string(&connector_config).unwrap())?;
        sources.insert(
            connector_id,
            SourceConfigFile {
                config: config.clone(),
                created_at: Utc::now(),
                path: path.clone(),
            },
        );

        Ok(config)
    }

    async fn get_active_configs(&self) -> Result<ConnectorsConfig, RuntimeError> {
        let all_configs = &self.state.connectors_config;
        let active_versions = self.load_active_versions();

        let sinks = all_configs
            .sinks_grouped_by_key()
            .iter()
            .filter_map(|(key, config_files)| {
                if config_files.is_empty() {
                    return None;
                }
                let active_config = if let Some(&version) = active_versions.sinks.get(key) {
                    config_files
                        .iter()
                        .find(|c| c.config.version == version)
                        .cloned()
                } else {
                    config_files
                        .iter()
                        .max_by_key(|c| c.config.version)
                        .cloned()
                };
                active_config.map(|config_file| (key.clone(), config_file.config.clone()))
            })
            .collect();

        let sources = all_configs
            .sources_grouped_by_key()
            .iter()
            .filter_map(|(key, config_files)| {
                if config_files.is_empty() {
                    return None;
                }
                let active_config = if let Some(&version) = active_versions.sources.get(key) {
                    config_files
                        .iter()
                        .find(|c| c.config.version == version)
                        .cloned()
                } else {
                    config_files
                        .iter()
                        .max_by_key(|c| c.config.version)
                        .cloned()
                };
                active_config.map(|config_file| (key.clone(), config_file.config.clone()))
            })
            .collect();

        Ok(ConnectorsConfig::new(sinks, sources))
    }

    async fn get_active_configs_versions(&self) -> Result<ConnectorConfigVersions, RuntimeError> {
        let all_configs = &self.state.connectors_config;
        let active_versions = self.load_active_versions();

        let sinks = all_configs
            .sinks_grouped_by_key()
            .iter()
            .filter_map(|(key, config_files)| {
                if config_files.is_empty() {
                    return None;
                }
                let latest_version = config_files
                    .iter()
                    .map(|c| c.config.version)
                    .max()
                    .expect("At least one config version must exist");
                let active_version = active_versions
                    .sinks
                    .get(key)
                    .copied()
                    .unwrap_or(latest_version);

                config_files
                    .iter()
                    .find(|config_file| config_file.config.version == active_version)
                    .map(|config_file| ConnectorConfigVersionInfo {
                        version: config_file.config.version,
                        created_at: config_file.created_at,
                    })
                    .map(|config| (key.clone(), config))
            })
            .collect();

        let sources = all_configs
            .sources_grouped_by_key()
            .iter()
            .filter_map(|(key, config_files)| {
                if config_files.is_empty() {
                    return None;
                }
                let latest_version = config_files
                    .iter()
                    .map(|c| c.config.version)
                    .max()
                    .expect("At least one config version must exist");
                let active_version = active_versions
                    .sources
                    .get(key)
                    .copied()
                    .unwrap_or(latest_version);

                config_files
                    .iter()
                    .find(|config_file| config_file.config.version == active_version)
                    .map(|config_file| ConnectorConfigVersionInfo {
                        version: config_file.config.version,
                        created_at: config_file.created_at,
                    })
                    .map(|config| (key.clone(), config))
            })
            .collect();

        Ok(ConnectorConfigVersions { sinks, sources })
    }

    async fn set_active_sink_version(&self, key: &str, version: u64) -> Result<(), RuntimeError> {
        let connector_id = ConnectorId {
            key: key.to_owned(),
            version,
        };
        if self
            .state
            .connectors_config
            .sinks()
            .get(&connector_id)
            .is_none()
        {
            return Err(RuntimeError::SinkConfigNotFound(key.to_owned(), version));
        }

        let mut active_versions = self.load_active_versions();
        active_versions.sinks.insert(key.to_owned(), version);
        self.save_active_versions(&active_versions)
    }

    async fn set_active_source_version(&self, key: &str, version: u64) -> Result<(), RuntimeError> {
        let connector_id = ConnectorId {
            key: key.to_owned(),
            version,
        };
        if self
            .state
            .connectors_config
            .sources()
            .get(&connector_id)
            .is_none()
        {
            return Err(RuntimeError::SourceConfigNotFound(key.to_owned(), version));
        }

        let mut active_versions = self.load_active_versions();
        active_versions.sources.insert(key.to_owned(), version);
        self.save_active_versions(&active_versions)
    }

    async fn get_sink_configs(&self, key: &str) -> Result<Vec<SinkConfig>, RuntimeError> {
        Ok(self
            .state
            .connectors_config
            .sinks_grouped_by_key()
            .get(key)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|config_file| config_file.config)
            .collect())
    }

    async fn get_sink_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<SinkConfig>, RuntimeError> {
        if let Some(version) = version {
            let connector_id = ConnectorId {
                key: key.to_owned(),
                version,
            };
            Ok(self
                .state
                .connectors_config
                .sinks()
                .get(&connector_id)
                .map(|entry| entry.config.clone()))
        } else {
            Ok(self
                .get_sink_configs(key)
                .await?
                .into_iter()
                .max_by_key(|config| config.version))
        }
    }

    async fn get_source_configs(&self, key: &str) -> Result<Vec<SourceConfig>, RuntimeError> {
        Ok(self
            .state
            .connectors_config
            .sources_grouped_by_key()
            .get(key)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|config_file| config_file.config)
            .collect())
    }

    async fn get_source_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<SourceConfig>, RuntimeError> {
        if let Some(version) = version {
            let connector_id = ConnectorId {
                key: key.to_owned(),
                version,
            };
            Ok(self
                .state
                .connectors_config
                .sources()
                .get(&connector_id)
                .map(|entry| entry.config.clone()))
        } else {
            Ok(self
                .get_source_configs(key)
                .await?
                .into_iter()
                .max_by_key(|config| config.version))
        }
    }

    async fn delete_sink_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<(), RuntimeError> {
        debug!("Deleting sink config: {}@{:?}", &key, &version);
        let sinks = self.state.connectors_config.sinks();
        let active_versions = self.load_active_versions();

        let version_to_delete = version
            .or(active_versions.sinks.get(key).copied())
            .ok_or_else(|| RuntimeError::SinkConfigNotFound(key.to_owned(), 0))?;

        let connector_id = ConnectorId {
            key: key.to_owned(),
            version: version_to_delete,
        };

        let config_file = {
            sinks
                .get(&connector_id)
                .ok_or_else(|| RuntimeError::SinkConfigNotFound(key.to_owned(), version_to_delete))?
                .value()
                .clone()
        };

        std::fs::remove_file(&config_file.path)?;
        sinks.remove(&connector_id);

        let mut active_versions = self.load_active_versions();
        let remaining_versions: Vec<u64> = sinks
            .iter()
            .filter(|entry| entry.key().key == key)
            .map(|entry| entry.key().version)
            .collect();

        if remaining_versions.is_empty() {
            active_versions.sinks.remove(key);
        } else if Some(version_to_delete) == active_versions.sinks.get(key).copied() {
            let latest_version = remaining_versions
                .into_iter()
                .max()
                .expect("At least one version must exist");
            active_versions.sinks.insert(key.to_owned(), latest_version);
        }

        self.save_active_versions(&active_versions)?;
        debug!("Deleted sink configuration: {}@{:?}", &key, &version);
        Ok(())
    }

    async fn delete_source_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<(), RuntimeError> {
        debug!("Deleting source config: {}@{:?}", &key, &version);
        let sources = self.state.connectors_config.sources();
        let active_versions = self.load_active_versions();

        let version_to_delete = version
            .or(active_versions.sources.get(key).copied())
            .ok_or_else(|| RuntimeError::SourceConfigNotFound(key.to_owned(), 0))?;

        let connector_id = ConnectorId {
            key: key.to_owned(),
            version: version_to_delete,
        };

        let config_file = {
            sources
                .get(&connector_id)
                .ok_or_else(|| {
                    RuntimeError::SourceConfigNotFound(key.to_owned(), version_to_delete)
                })?
                .value()
                .clone()
        };

        std::fs::remove_file(&config_file.path)?;
        sources.remove(&connector_id);

        let mut active_versions = self.load_active_versions();
        let remaining_versions: Vec<u64> = sources
            .iter()
            .filter(|entry| entry.key().key == key)
            .map(|entry| entry.key().version)
            .collect();

        if remaining_versions.is_empty() {
            active_versions.sources.remove(key);
        } else if Some(version_to_delete) == active_versions.sources.get(key).copied() {
            let latest_version = remaining_versions
                .into_iter()
                .max()
                .expect("At least one version must exist");
            active_versions
                .sources
                .insert(key.to_owned(), latest_version);
        }

        self.save_active_versions(&active_versions)?;
        debug!("Deleted source configuration: {}@{:?}", &key, &version);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ConnectorEnvProvider {
    Sink {
        connector_name: String,
        provider: TypedEnvProvider<SinkConfig>,
    },
    Source {
        connector_name: String,
        provider: TypedEnvProvider<SourceConfig>,
    },
}

impl ConnectorEnvProvider {
    fn with_connector_base_config(base_config: &BaseConnectorConfig) -> Self {
        let connector_type = base_config.connector_type().to_uppercase();
        let key = base_config.key().to_uppercase();
        let prefix = format!("IGGY_CONNECTORS_{}_{}_", connector_type, key);
        let connector_name = base_config.key().to_owned();

        match base_config {
            BaseConnectorConfig::Sink { .. } => Self::Sink {
                connector_name,
                provider: TypedEnvProvider::with_runtime_prefix(&prefix, &[]),
            },
            BaseConnectorConfig::Source { .. } => Self::Source {
                connector_name,
                provider: TypedEnvProvider::with_runtime_prefix(&prefix, &[]),
            },
        }
    }
}

impl Provider for ConnectorEnvProvider {
    fn metadata(&self) -> Metadata {
        let name = match self {
            Self::Sink { connector_name, .. } => connector_name,
            Self::Source { connector_name, .. } => connector_name,
        };
        Metadata::named(format!("iggy-connectors-{}-config", name))
    }

    fn data(&self) -> Result<figment::value::Map<Profile, Dict>, figment::Error> {
        match self {
            Self::Sink { provider, .. } => provider
                .deserialize_with_runtime_prefix()
                .map_err(|e| figment::Error::from(format!("Failed to deserialize env vars: {e}"))),
            Self::Source { provider, .. } => provider
                .deserialize_with_runtime_prefix()
                .map_err(|e| figment::Error::from(format!("Failed to deserialize env vars: {e}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::connectors::{
        ConfigFormat, CreateSinkConfig, CreateSourceConfig, StreamConsumerConfig,
        StreamProducerConfig,
    };
    use iggy_connector_sdk::Schema;
    use tempfile::TempDir;

    // -------------------------------------------------------------------------
    // HELPERS
    // -------------------------------------------------------------------------

    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temp dir")
    }

    fn make_sink_toml(key: &str) -> String {
        format!(
            r#"type = "sink"
key = "{key}"
enabled = true
version = 0
name = "Test Sink"
path = "/tmp/test_plugin.so"
verbose = false

[[streams]]
stream = "events"
topics = ["user_events"]
schema = "json"
"#
        )
    }

    fn make_source_toml(key: &str) -> String {
        format!(
            r#"type = "source"
key = "{key}"
enabled = true
version = 0
name = "Test Source"
path = "/tmp/test_plugin.so"
verbose = false

[[streams]]
stream = "events"
topic = "user_events"
schema = "json"
"#
        )
    }

    fn make_create_sink_config(name: &str) -> CreateSinkConfig {
        CreateSinkConfig {
            enabled: true,
            name: name.to_string(),
            path: "/tmp/plugin.so".to_string(),
            transforms: None,
            streams: vec![StreamConsumerConfig {
                stream: "events".to_string(),
                topics: vec!["user_events".to_string()],
                schema: Schema::default(),
                batch_length: None,
                poll_interval: None,
                consumer_group: None,
            }],
            plugin_config_format: Some(ConfigFormat::Json),
            plugin_config: None,
            verbose: false,
        }
    }

    fn make_create_source_config(name: &str) -> CreateSourceConfig {
        CreateSourceConfig {
            enabled: true,
            name: name.to_string(),
            path: "/tmp/plugin.so".to_string(),
            transforms: None,
            streams: vec![StreamProducerConfig {
                stream: "events".to_string(),
                topic: "user_events".to_string(),
                schema: Schema::default(),
                batch_length: None,
                linger_time: None,
            }],
            plugin_config_format: Some(ConfigFormat::Json),
            plugin_config: None,
            verbose: false,
        }
    }

    // -------------------------------------------------------------------------
    // INIT TESTS
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_init_with_empty_config_dir_returns_error() {
        let provider = LocalConnectorsConfigProvider::new("");
        let result = provider.init().await;
        assert!(result.is_err());
        match result.err().unwrap() {
            RuntimeError::InvalidConfiguration(msg) => {
                assert!(msg.contains("not provided"));
            }
            e => panic!("Unexpected error: {e:?}"),
        }
    }

    #[tokio::test]
    async fn test_init_creates_missing_directory() {
        let dir = temp_dir();
        let new_path = dir.path().join("new_config_dir");
        let provider = LocalConnectorsConfigProvider::new(new_path.to_str().unwrap());
        let result = provider.init().await;
        assert!(result.is_ok());
        assert!(new_path.exists());
    }

    #[tokio::test]
    async fn test_init_loads_sink_toml_from_directory() {
        let dir = temp_dir();
        std::fs::write(
            dir.path().join("sink_influxdb_0.toml"),
            make_sink_toml("influxdb"),
        )
        .unwrap();

        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();
        let configs = initialized.get_sink_configs("influxdb").await.unwrap();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].key, "influxdb");
    }

    #[tokio::test]
    async fn test_init_loads_source_toml_from_directory() {
        let dir = temp_dir();
        std::fs::write(
            dir.path().join("source_influxdb_0.toml"),
            make_source_toml("influxdb"),
        )
        .unwrap();

        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();
        let configs = initialized.get_source_configs("influxdb").await.unwrap();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].key, "influxdb");
    }

    #[tokio::test]
    async fn test_init_skips_non_toml_files() {
        let dir = temp_dir();
        std::fs::write(dir.path().join("not_a_config.json"), r#"{"key": "test"}"#).unwrap();
        std::fs::write(dir.path().join("readme.txt"), "some text").unwrap();

        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();
        let active = initialized.get_active_configs().await.unwrap();
        assert!(active.sinks().is_empty());
        assert!(active.sources().is_empty());
    }

    #[tokio::test]
    async fn test_init_skips_hidden_files() {
        let dir = temp_dir();
        std::fs::write(dir.path().join(".hidden.toml"), make_sink_toml("hidden")).unwrap();

        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();
        let configs = initialized.get_sink_configs("hidden").await.unwrap();
        assert!(configs.is_empty());
    }

    #[tokio::test]
    async fn test_init_skips_cargo_toml() {
        let dir = temp_dir();
        std::fs::write(dir.path().join("Cargo.toml"), "[package]\nname = \"test\"").unwrap();

        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let result = provider.init().await;
        assert!(result.is_ok());
    }

    // -------------------------------------------------------------------------
    // CREATE SINK CONFIG TESTS
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_create_sink_config_persisted_to_disk() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        let cmd = make_create_sink_config("InfluxDB Sink");
        initialized
            .create_sink_config("influxdb", cmd)
            .await
            .unwrap();

        let toml_path = dir.path().join("sink_influxdb_0.toml");
        assert!(toml_path.exists(), "TOML file should be written to disk");
    }

    #[tokio::test]
    async fn test_create_sink_config_version_increments() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        let v0 = initialized
            .create_sink_config("influxdb", make_create_sink_config("Sink v0"))
            .await
            .unwrap();
        let v1 = initialized
            .create_sink_config("influxdb", make_create_sink_config("Sink v1"))
            .await
            .unwrap();

        assert_eq!(v0.version, 0);
        assert_eq!(v1.version, 1);
    }

    #[tokio::test]
    async fn test_create_multiple_sink_connectors_different_keys() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        initialized
            .create_sink_config("influxdb", make_create_sink_config("InfluxDB"))
            .await
            .unwrap();
        initialized
            .create_sink_config("postgres", make_create_sink_config("Postgres"))
            .await
            .unwrap();

        let influx_configs = initialized.get_sink_configs("influxdb").await.unwrap();
        let pg_configs = initialized.get_sink_configs("postgres").await.unwrap();
        assert_eq!(influx_configs.len(), 1);
        assert_eq!(pg_configs.len(), 1);
    }

    // -------------------------------------------------------------------------
    // CREATE SOURCE CONFIG TESTS
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_create_source_config_persisted_to_disk() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        initialized
            .create_source_config("influxdb", make_create_source_config("InfluxDB Source"))
            .await
            .unwrap();

        let toml_path = dir.path().join("source_influxdb_0.toml");
        assert!(toml_path.exists());
    }

    #[tokio::test]
    async fn test_create_source_config_version_increments() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        let v0 = initialized
            .create_source_config("influxdb", make_create_source_config("Source v0"))
            .await
            .unwrap();
        let v1 = initialized
            .create_source_config("influxdb", make_create_source_config("Source v1"))
            .await
            .unwrap();

        assert_eq!(v0.version, 0);
        assert_eq!(v1.version, 1);
    }

    // -------------------------------------------------------------------------
    // GET ACTIVE CONFIGS TESTS
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_get_active_configs_returns_latest_version_by_default() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        initialized
            .create_sink_config("influxdb", make_create_sink_config("v0"))
            .await
            .unwrap();
        initialized
            .create_sink_config("influxdb", make_create_sink_config("v1"))
            .await
            .unwrap();

        let active = initialized.get_active_configs().await.unwrap();
        let sink = active.sinks().get("influxdb").unwrap();
        assert_eq!(sink.version, 1, "Should return latest version by default");
    }

    #[tokio::test]
    async fn test_get_active_configs_empty_when_no_configs_loaded() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();
        let active = initialized.get_active_configs().await.unwrap();
        assert!(active.sinks().is_empty());
        assert!(active.sources().is_empty());
    }

    #[tokio::test]
    async fn test_get_active_configs_respects_pinned_version() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        initialized
            .create_sink_config("influxdb", make_create_sink_config("v0"))
            .await
            .unwrap();
        initialized
            .create_sink_config("influxdb", make_create_sink_config("v1"))
            .await
            .unwrap();

        // Pin to version 0
        initialized
            .set_active_sink_version("influxdb", 0)
            .await
            .unwrap();

        let active = initialized.get_active_configs().await.unwrap();
        let sink = active.sinks().get("influxdb").unwrap();
        assert_eq!(sink.version, 0, "Should return pinned version 0");
    }

    // -------------------------------------------------------------------------
    // GET SINK / SOURCE CONFIG TESTS
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_get_sink_config_by_version() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        initialized
            .create_sink_config("influxdb", make_create_sink_config("v0"))
            .await
            .unwrap();
        initialized
            .create_sink_config("influxdb", make_create_sink_config("v1"))
            .await
            .unwrap();

        let config = initialized
            .get_sink_config("influxdb", Some(0))
            .await
            .unwrap();
        assert!(config.is_some());
        assert_eq!(config.unwrap().version, 0);
    }

    #[tokio::test]
    async fn test_get_sink_config_none_version_returns_latest() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        initialized
            .create_sink_config("influxdb", make_create_sink_config("v0"))
            .await
            .unwrap();
        initialized
            .create_sink_config("influxdb", make_create_sink_config("v1"))
            .await
            .unwrap();

        let config = initialized.get_sink_config("influxdb", None).await.unwrap();
        assert_eq!(config.unwrap().version, 1);
    }

    #[tokio::test]
    async fn test_get_sink_config_unknown_key_returns_none() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        let result = initialized
            .get_sink_config("nonexistent", None)
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_source_config_by_version() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        initialized
            .create_source_config("influxdb", make_create_source_config("v0"))
            .await
            .unwrap();

        let config = initialized
            .get_source_config("influxdb", Some(0))
            .await
            .unwrap();
        assert!(config.is_some());
        assert_eq!(config.unwrap().version, 0);
    }

    // -------------------------------------------------------------------------
    // SET ACTIVE VERSION TESTS
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_set_active_sink_version_valid() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        initialized
            .create_sink_config("influxdb", make_create_sink_config("v0"))
            .await
            .unwrap();
        initialized
            .create_sink_config("influxdb", make_create_sink_config("v1"))
            .await
            .unwrap();

        let result = initialized.set_active_sink_version("influxdb", 0).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_set_active_sink_version_nonexistent_returns_error() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        let result = initialized.set_active_sink_version("influxdb", 99).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_set_active_source_version_valid() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        initialized
            .create_source_config("influxdb", make_create_source_config("v0"))
            .await
            .unwrap();

        let result = initialized.set_active_source_version("influxdb", 0).await;
        assert!(result.is_ok());
    }

    // -------------------------------------------------------------------------
    // DELETE SINK / SOURCE CONFIG TESTS
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_delete_sink_config_removes_file_from_disk() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        initialized
            .create_sink_config("influxdb", make_create_sink_config("v0"))
            .await
            .unwrap();

        let toml_path = dir.path().join("sink_influxdb_0.toml");
        assert!(toml_path.exists());

        initialized
            .delete_sink_config("influxdb", Some(0))
            .await
            .unwrap();
        assert!(!toml_path.exists(), "File should be deleted from disk");
    }

    #[tokio::test]
    async fn test_delete_sink_config_no_longer_returned() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        initialized
            .create_sink_config("influxdb", make_create_sink_config("v0"))
            .await
            .unwrap();
        initialized
            .delete_sink_config("influxdb", Some(0))
            .await
            .unwrap();

        let configs = initialized.get_sink_configs("influxdb").await.unwrap();
        assert!(configs.is_empty());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_sink_config_returns_error() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        let result = initialized.delete_sink_config("nonexistent", Some(0)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_source_config_removes_from_disk() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        initialized
            .create_source_config("influxdb", make_create_source_config("v0"))
            .await
            .unwrap();

        initialized
            .delete_source_config("influxdb", Some(0))
            .await
            .unwrap();

        let configs = initialized.get_source_configs("influxdb").await.unwrap();
        assert!(configs.is_empty());
    }

    #[tokio::test]
    async fn test_delete_one_version_keeps_others() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        initialized
            .create_sink_config("influxdb", make_create_sink_config("v0"))
            .await
            .unwrap();
        initialized
            .create_sink_config("influxdb", make_create_sink_config("v1"))
            .await
            .unwrap();

        initialized
            .delete_sink_config("influxdb", Some(0))
            .await
            .unwrap();

        let configs = initialized.get_sink_configs("influxdb").await.unwrap();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].version, 1);
    }

    // -------------------------------------------------------------------------
    // ACTIVE VERSIONS PERSISTENCE TESTS
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_active_versions_file_created_after_set() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        initialized
            .create_sink_config("influxdb", make_create_sink_config("v0"))
            .await
            .unwrap();
        initialized
            .set_active_sink_version("influxdb", 0)
            .await
            .unwrap();

        let versions_file = dir.path().join(".active_versions.toml");
        assert!(
            versions_file.exists(),
            ".active_versions.toml should be created"
        );
    }

    #[tokio::test]
    async fn test_active_versions_default_when_file_missing() {
        let dir = temp_dir();
        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();

        // No .active_versions.toml written — should default gracefully
        let active = initialized.get_active_configs().await.unwrap();
        assert!(active.sinks().is_empty());
    }

    // -------------------------------------------------------------------------
    // ENV OVERRIDE TESTS
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_plugin_config_overridden_by_env_var() {
        let dir = temp_dir();

        // Write a sink config with plugin_config
        let toml = r#"type = "sink"
key = "influxdb"
enabled = true
version = 0
name = "InfluxDB Sink"
path = "/tmp/plugin.so"
verbose = false
plugin_config_format = "json"

[plugin_config]
url = "http://localhost:8086"
org = "iggy_org"

[[streams]]
stream = "events"
topics = ["user_events"]
schema = "json"
"#;
        std::fs::write(dir.path().join("sink_influxdb_0.toml"), toml).unwrap();

        // Set env override
        // SAFETY: test-only, single-threaded
        unsafe {
            std::env::set_var(
                "IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_URL",
                "http://override:8086",
            );
        }

        let provider = LocalConnectorsConfigProvider::new(dir.path().to_str().unwrap());
        let initialized = provider.init().await.unwrap();
        let config = initialized
            .get_sink_config("influxdb", Some(0))
            .await
            .unwrap()
            .unwrap();

        if let Some(plugin_config) = &config.plugin_config {
            assert_eq!(plugin_config["url"], "http://override:8086");
        }

        // Cleanup
        // SAFETY: test-only cleanup
        unsafe {
            std::env::remove_var("IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_URL");
        }
    }

    // -------------------------------------------------------------------------
    // CONNECTOR ID TESTS
    // -------------------------------------------------------------------------

    #[test]
    fn test_connector_id_filename_key_format() {
        let id = ConnectorId {
            key: "influxdb".to_string(),
            version: 3,
        };
        assert_eq!(id.to_filename_key(), "influxdb_3");
    }

    #[test]
    fn test_connector_id_from_sink_config() {
        let config = ConnectorConfig::Sink(SinkConfig {
            key: "influxdb".to_string(),
            version: 2,
            ..Default::default()
        });
        let id: ConnectorId = (&config).into();
        assert_eq!(id.key, "influxdb");
        assert_eq!(id.version, 2);
    }

    #[test]
    fn test_connector_id_from_source_config() {
        let config = ConnectorConfig::Source(SourceConfig {
            key: "influxdb".to_string(),
            version: 5,
            ..Default::default()
        });
        let id: ConnectorId = (&config).into();
        assert_eq!(id.key, "influxdb");
        assert_eq!(id.version, 5);
    }

    // -------------------------------------------------------------------------
    // BASE CONNECTOR CONFIG TESTS
    // -------------------------------------------------------------------------

    #[test]
    fn test_base_connector_config_key_sink() {
        let config = BaseConnectorConfig::Sink {
            key: "influxdb".to_string(),
        };
        assert_eq!(config.key(), "influxdb");
        assert_eq!(config.connector_type(), "sink");
    }

    #[test]
    fn test_base_connector_config_key_source() {
        let config = BaseConnectorConfig::Source {
            key: "influxdb".to_string(),
        };
        assert_eq!(config.key(), "influxdb");
        assert_eq!(config.connector_type(), "source");
    }
}
