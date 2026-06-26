// Project:   dfe-transform-vector
// File:      src/config/kafka_defaults.rs
// Purpose:   Load librdkafka defaults from central config or scalo fallback
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Central librdkafka defaults loader.
//!
//! Attempts to load profiles from `$DFE_CONFIG_DIR/shared/librdkafka.yaml`.
//! Falls back to `scalo::kafka_config` coded-in constants if the
//! file is not present or cannot be parsed.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;

use scalo::kafka_config;
use tracing::{debug, warn};

/// Parsed librdkafka profiles from the central config file.
#[derive(Debug, Default)]
struct CentralProfiles {
    consumer: HashMap<String, HashMap<String, String>>,
    producer: HashMap<String, HashMap<String, String>>,
}

/// Cached central profiles — loaded once at first access.
static CENTRAL_PROFILES: OnceLock<Option<CentralProfiles>> = OnceLock::new();

/// Resolve the path to the central librdkafka config file.
fn central_config_path() -> Option<PathBuf> {
    std::env::var("DFE_CONFIG_DIR")
        .ok()
        .map(|dir| PathBuf::from(dir).join("shared").join("librdkafka.yaml"))
}

/// Load and parse the central librdkafka.yaml file.
fn load_central_profiles() -> Option<CentralProfiles> {
    let path = central_config_path()?;

    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            debug!(path = %path.display(), error = %e, "Central librdkafka config not found, using scalo defaults");
            return None;
        }
    };

    let raw: serde_yaml_ng::Value = match serde_yaml_ng::from_str(&content) {
        Ok(v) => v,
        Err(e) => {
            warn!(path = %path.display(), error = %e, "Failed to parse central librdkafka config, using scalo defaults");
            return None;
        }
    };

    let mut profiles = CentralProfiles::default();

    if let Some(consumer) = raw.get("consumer").and_then(|v| v.as_mapping()) {
        for (name, settings) in consumer {
            if let (Some(name), Some(settings)) = (name.as_str(), settings.as_mapping()) {
                let map = parse_profile_map(settings);
                if !map.is_empty() {
                    profiles.consumer.insert(name.to_string(), map);
                }
            }
        }
    }

    if let Some(producer) = raw.get("producer").and_then(|v| v.as_mapping()) {
        for (name, settings) in producer {
            if let (Some(name), Some(settings)) = (name.as_str(), settings.as_mapping()) {
                let map = parse_profile_map(settings);
                if !map.is_empty() {
                    profiles.producer.insert(name.to_string(), map);
                }
            }
        }
    }

    debug!(
        consumer_profiles = profiles.consumer.len(),
        producer_profiles = profiles.producer.len(),
        path = %path.display(),
        "Loaded central librdkafka profiles"
    );

    Some(profiles)
}

/// Parse a YAML mapping of string key-value pairs into a HashMap.
fn parse_profile_map(mapping: &serde_yaml_ng::Mapping) -> HashMap<String, String> {
    let mut map = HashMap::with_capacity(mapping.len());
    for (k, v) in mapping {
        if let Some(key) = k.as_str() {
            let value = match v {
                serde_yaml_ng::Value::String(s) => s.clone(),
                serde_yaml_ng::Value::Bool(b) => b.to_string(),
                serde_yaml_ng::Value::Number(n) => n.to_string(),
                _ => continue,
            };
            map.insert(key.to_string(), value);
        }
    }
    map
}

/// Get the cached central profiles, loading on first access.
fn get_central() -> &'static Option<CentralProfiles> {
    CENTRAL_PROFILES.get_or_init(load_central_profiles)
}

/// Convert a scalo const profile to a HashMap.
fn profile_to_map(profile: &[(&str, &str)]) -> HashMap<String, String> {
    profile
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

/// Get consumer profile defaults.
///
/// Tries the central config file first (`$DFE_CONFIG_DIR/shared/librdkafka.yaml`),
/// falls back to `scalo::kafka_config` constants.
#[must_use]
pub fn consumer_profile(profile_name: &str) -> HashMap<String, String> {
    if let Some(Some(central)) = Some(get_central())
        && let Some(profile) = central.consumer.get(profile_name)
    {
        return profile.clone();
    }

    // Fallback to scalo constants
    match profile_name {
        "production" => profile_to_map(kafka_config::CONSUMER_PRODUCTION),
        "devtest" => profile_to_map(kafka_config::CONSUMER_DEVTEST),
        "low_latency" => profile_to_map(kafka_config::CONSUMER_LOW_LATENCY),
        _ => {
            warn!(
                profile = profile_name,
                "Unknown consumer profile, using production"
            );
            profile_to_map(kafka_config::CONSUMER_PRODUCTION)
        }
    }
}

/// Get producer profile defaults.
///
/// Tries the central config file first (`$DFE_CONFIG_DIR/shared/librdkafka.yaml`),
/// falls back to `scalo::kafka_config` constants.
#[must_use]
pub fn producer_profile(profile_name: &str) -> HashMap<String, String> {
    if let Some(Some(central)) = Some(get_central())
        && let Some(profile) = central.producer.get(profile_name)
    {
        return profile.clone();
    }

    // Fallback to scalo constants
    match profile_name {
        "production" => profile_to_map(kafka_config::PRODUCER_PRODUCTION),
        "exactly_once" => profile_to_map(kafka_config::PRODUCER_EXACTLY_ONCE),
        "low_latency" => profile_to_map(kafka_config::PRODUCER_LOW_LATENCY),
        "devtest" => profile_to_map(kafka_config::PRODUCER_DEVTEST),
        _ => {
            warn!(
                profile = profile_name,
                "Unknown producer profile, using production"
            );
            profile_to_map(kafka_config::PRODUCER_PRODUCTION)
        }
    }
}

/// Merge a base profile with user overrides.
///
/// This is the full 3-layer merge for a single component:
/// 1. Central config (or scalo fallback) baseline
/// 2. Service-specific overrides (only if not already set by user)
/// 3. User config YAML `librdkafka_options` (highest priority)
#[must_use]
pub fn merge_layers(
    base: &HashMap<String, String>,
    service_overrides: &[(&str, &str)],
    user_overrides: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut merged = base.clone();

    // Service overrides — only if not already set by user
    for (k, v) in service_overrides {
        if !user_overrides.contains_key(*k) {
            merged.insert((*k).to_string(), (*v).to_string());
        }
    }

    // User overrides — highest priority
    for (k, v) in user_overrides {
        merged.insert(k.clone(), v.clone());
    }

    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallback_consumer_production() {
        // No DFE_CONFIG_DIR set — should return scalo constants
        let profile = consumer_profile("production");
        assert_eq!(
            profile["partition.assignment.strategy"],
            "cooperative-sticky"
        );
        assert_eq!(profile["fetch.min.bytes"], "1048576");
        assert_eq!(profile["enable.auto.commit"], "false");
        assert_eq!(profile.len(), kafka_config::CONSUMER_PRODUCTION.len());
    }

    #[test]
    fn fallback_producer_production() {
        let profile = producer_profile("production");
        assert_eq!(profile["linger.ms"], "100");
        assert_eq!(profile["compression.type"], "zstd");
        assert_eq!(profile.len(), kafka_config::PRODUCER_PRODUCTION.len());
    }

    #[test]
    fn unknown_profile_falls_back_to_production() {
        let profile = consumer_profile("nonexistent");
        assert_eq!(
            profile["partition.assignment.strategy"],
            "cooperative-sticky"
        );
    }

    #[test]
    fn merge_layers_user_wins() {
        let base = profile_to_map(kafka_config::PRODUCER_PRODUCTION);
        let service = &[("queue.buffering.max.kbytes", "262144")];
        let mut user = HashMap::new();
        user.insert("linger.ms".to_string(), "50".to_string());

        let merged = merge_layers(&base, service, &user);

        // User override wins
        assert_eq!(merged["linger.ms"], "50");
        // Service override applied
        assert_eq!(merged["queue.buffering.max.kbytes"], "262144");
        // Base retained
        assert_eq!(merged["compression.type"], "zstd");
    }

    #[test]
    fn merge_layers_user_overrides_service() {
        let base = profile_to_map(kafka_config::PRODUCER_PRODUCTION);
        let service = &[("queue.buffering.max.kbytes", "262144")];
        let mut user = HashMap::new();
        user.insert(
            "queue.buffering.max.kbytes".to_string(),
            "524288".to_string(),
        );

        let merged = merge_layers(&base, service, &user);

        // User override beats service override
        assert_eq!(merged["queue.buffering.max.kbytes"], "524288");
    }

    #[test]
    fn parse_profile_map_handles_types() {
        let mut mapping = serde_yaml_ng::Mapping::new();
        mapping.insert(
            serde_yaml_ng::Value::String("key1".into()),
            serde_yaml_ng::Value::String("value1".into()),
        );
        mapping.insert(
            serde_yaml_ng::Value::String("key2".into()),
            serde_yaml_ng::Value::Bool(true),
        );
        mapping.insert(
            serde_yaml_ng::Value::String("key3".into()),
            serde_yaml_ng::Value::Number(42.into()),
        );

        let map = parse_profile_map(&mapping);
        assert_eq!(map["key1"], "value1");
        assert_eq!(map["key2"], "true");
        assert_eq!(map["key3"], "42");
    }
}
