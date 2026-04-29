// Project:   dfe-transform-vector
// File:      src/main.rs
// Purpose:   CLI entry point and orchestrator
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! CLI entry point for dfe-transform-vector.
//!
//! Uses hyperi-rustlib CLI module for standard arguments and subcommands.
//! Implements the [`DfeApp`] trait for the standard DFE service lifecycle.

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use clap::{Parser, Subcommand};
use hyperi_rustlib::cli::{CliError, CommonArgs, DfeApp, StandardCommand, VersionInfo};
use hyperi_rustlib::deployment::{generate_chart, generate_compose_fragment};
use hyperi_rustlib::logger::security::{self, SecurityEvent, SecurityOutcome};
use hyperi_rustlib::version_check::{VersionCheck, VersionCheckConfig};
use tracing::{debug, error, info};

use dfe_transform_vector::config::Config;
use dfe_transform_vector::config::assembler;
use dfe_transform_vector::config::reload::{ReloadTrigger, run_reload_loop};
use dfe_transform_vector::config::validate::{check_vector_version, vector_validate};
use dfe_transform_vector::deployment;
use dfe_transform_vector::health::serve_health;
use dfe_transform_vector::metrics::{WrapperMetrics, serve_metrics};

/// Git commit hash for build info metric.
const COMMIT: &str = match option_env!("GIT_COMMIT") {
    Some(c) => c,
    None => "unknown",
};
use dfe_transform_vector::vector::lifecycle::State;
use dfe_transform_vector::vector::{BackoffConfig, Lifecycle, run_lifecycle};

/// dfe-transform-vector: Kafka-to-Kafka transform pipelines powered by Vector.dev.
#[derive(Parser, Debug)]
#[command(name = "dfe-transform-vector")]
#[command(version, about, long_about = None)]
struct App {
    /// Standard CLI arguments (config, log-level, log-format, metrics-addr, verbose, quiet).
    #[command(flatten)]
    common: CommonArgs,

    /// Subcommand (defaults to `run` if omitted).
    #[command(subcommand)]
    command: Option<AppCommand>,
}

/// Application subcommands.
///
/// Standard commands (`run`, `version`, `config-check`) delegate to the
/// rustlib CLI lifecycle. Deployment commands generate artefacts from the
/// [`DeploymentContract`](dfe_transform_vector::deployment::contract).
#[derive(Subcommand, Clone, Debug)]
enum AppCommand {
    /// Start the service (default if no subcommand given).
    Run,

    /// Print version information and exit.
    Version,

    /// Validate configuration and exit.
    #[command(name = "config-check")]
    ConfigCheck,

    /// Assemble Vector config directory and exit.
    #[command(name = "assemble")]
    Assemble {
        /// Output directory for assembled Vector config.
        #[arg(default_value = assembler::DEFAULT_CONFIG_DIR)]
        dir: Option<String>,
    },

    /// Generate Dockerfile to stdout.
    #[command(name = "emit-dockerfile")]
    EmitDockerfile,

    /// Generate Helm chart to the given directory.
    #[command(name = "emit-chart")]
    EmitChart {
        /// Output directory for the chart.
        dir: String,
    },

    /// Generate Docker Compose fragment to stdout.
    #[command(name = "emit-compose")]
    EmitCompose,

    /// Print deployment contract as JSON to stdout.
    #[command(name = "emit-contract")]
    EmitContract,
}

impl DfeApp for App {
    type Config = Config;

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "dfe-transform-vector"
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn env_prefix(&self) -> &str {
        "DFE_TRANSFORM"
    }

    fn version_info(&self) -> VersionInfo {
        VersionInfo::new("dfe-transform-vector", env!("CARGO_PKG_VERSION"))
    }

    fn common_args(&self) -> &CommonArgs {
        &self.common
    }

    fn command(&self) -> Option<&StandardCommand> {
        match &self.command {
            Some(AppCommand::Version) => {
                static VERSION: StandardCommand = StandardCommand::Version;
                Some(&VERSION)
            }
            Some(AppCommand::ConfigCheck) => {
                static CONFIG_CHECK: StandardCommand = StandardCommand::ConfigCheck;
                Some(&CONFIG_CHECK)
            }
            // Run (explicit or default), Assemble, and deployment commands
            _ => None,
        }
    }

    fn load_config(&self, path: Option<&str>) -> Result<Config, CliError> {
        let config =
            Config::load(path).map_err(|e| CliError::Config(format!("failed to load: {e}")))?;
        config
            .validate()
            .map_err(|e| CliError::Config(format!("validation failed: {e}")))?;
        Ok(config)
    }

    async fn run_service(
        &self,
        config: Config,
        _runtime: hyperi_rustlib::cli::ServiceRuntime,
    ) -> Result<(), CliError> {
        run_transform_service(&self.common, config)
            .await
            .map_err(|e| CliError::Service(e.to_string()))
    }
}

#[tokio::main]
async fn main() {
    let app = App::parse();

    // Handle deployment artefact and assemble commands before entering the
    // DfeApp lifecycle (these don't need the full logging/config/run pipeline)
    if let Some(ref cmd) = app.command {
        match cmd {
            AppCommand::EmitDockerfile => {
                println!("{}", deployment::emit_dockerfile());
                return;
            }
            AppCommand::EmitChart { dir } => {
                let contract = deployment::contract();
                if let Err(e) = generate_chart(&contract, dir) {
                    eprintln!("error: failed to generate Helm chart: {e}");
                    std::process::exit(1);
                }
                eprintln!("Helm chart generated in {dir}/");
                return;
            }
            AppCommand::EmitCompose => {
                let contract = deployment::contract();
                println!("{}", generate_compose_fragment(&contract));
                return;
            }
            AppCommand::EmitContract => {
                let contract = deployment::contract();
                println!("{}", contract.to_json());
                return;
            }
            AppCommand::Assemble { dir } => {
                let config = match Config::load(app.common.config.as_deref()) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("error: failed to load configuration: {e}");
                        std::process::exit(1);
                    }
                };
                if let Err(e) = config.validate() {
                    eprintln!("error: configuration validation failed: {e}");
                    std::process::exit(1);
                }
                let config_dir =
                    PathBuf::from(dir.as_deref().unwrap_or(assembler::DEFAULT_CONFIG_DIR));
                if let Err(e) = assembler::assemble(&config, &config_dir) {
                    eprintln!("error: failed to assemble Vector config: {e}");
                    std::process::exit(1);
                }
                eprintln!("Vector config assembled in {}", config_dir.display());
                return;
            }
            _ => {}
        }
    }

    // Delegate to standard DfeApp lifecycle (logging → config → run_service)
    if let Err(e) = hyperi_rustlib::cli::run_app(app).await {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}

/// Main service loop — called by the DfeApp lifecycle after logging and config.
async fn run_transform_service(common: &CommonArgs, config: Config) -> anyhow::Result<()> {
    info!(
        pipeline = %config.pipeline.name,
        version = env!("CARGO_PKG_VERSION"),
        "starting dfe-transform-vector"
    );

    // Log startup config at debug level for diagnostics
    debug!(
        vector_binary = %config.vector.binary,
        vector_data_dir = %config.vector.data_dir,
        vector_api_address = %config.vector.api_address,
        vector_log_level = %config.vector.log_level,
        config_path = ?config.reload.enabled.then_some("reload enabled"),
        restart_initial_secs = 1,
        restart_max_secs = 60,
        restart_backoff_multiplier = 2.0,
        restart_reset_after_secs = 300,
        "startup config"
    );

    // Fire-and-forget startup version check
    VersionCheck::new(VersionCheckConfig {
        product: "dfe-transform-vector".into(),
        current_version: env!("CARGO_PKG_VERSION").into(),
        ..Default::default()
    })
    .check_on_startup();

    // Lifecycle state machine
    let lifecycle = Lifecycle::new();
    lifecycle.set(State::Initialising);

    // Check Vector binary version
    let version = check_vector_version(&config.vector).await?;
    if !version.is_empty() {
        info!(vector_version = %version, "Vector binary version detected");
    }

    // Assemble Vector config directory
    lifecycle.set(State::Validating);
    let config_dir = PathBuf::from(assembler::DEFAULT_CONFIG_DIR);
    assembler::assemble(&config, &config_dir)?;

    // Run vector validate on assembled config
    vector_validate(&config.vector, &config_dir).await?;
    info!("Vector config validation passed");

    // Wrapper metrics (installs global recorder, registers DfeMetrics + AppMetrics)
    let metrics = Arc::new(WrapperMetrics::new(COMMIT));
    let started_at = Instant::now();

    // Shutdown signal channel
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Spawn health server
    let health_lifecycle = lifecycle.clone();
    let health_address = config.health.address.clone();
    tokio::spawn(async move {
        if let Err(e) = serve_health(&health_address, health_lifecycle).await {
            error!(error = %e, "health server failed");
        }
    });

    // Spawn metrics server
    let metrics_lifecycle = lifecycle.clone();
    let metrics_address = config.metrics.address.clone();
    let vector_metrics_address = config.metrics.vector_metrics_address.clone();
    let metrics_clone = metrics.clone();
    tokio::spawn(async move {
        if let Err(e) = serve_metrics(
            &metrics_address,
            metrics_clone,
            metrics_lifecycle,
            started_at,
            vector_metrics_address,
        )
        .await
        {
            error!(error = %e, "metrics server failed");
        }
    });

    // Shared Vector PID for reload loop → SIGHUP
    let vector_pid: Arc<Mutex<Option<u32>>> = Arc::new(Mutex::new(None));

    // Reload channel: SIGHUP handler and file watcher send triggers
    let (reload_tx, reload_rx) = tokio::sync::mpsc::channel::<ReloadTrigger>(4);

    // Signal handler: SIGTERM/SIGINT → shutdown, SIGHUP → manual reload
    let shutdown_tx_signal = shutdown_tx.clone();
    let reload_tx_signal = reload_tx.clone();
    #[allow(clippy::unwrap_used)]
    tokio::spawn(async move {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        let mut sigint =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
        let mut sighup =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()).unwrap();

        loop {
            tokio::select! {
                _ = sigterm.recv() => {
                    info!("received SIGTERM");
                    SecurityEvent::new("process.shutdown", "SIGTERM", SecurityOutcome::Success)
                        .actor("system")
                        .detail("graceful shutdown initiated")
                        .emit();
                    let _ = shutdown_tx_signal.send(true);
                    return;
                }
                _ = sigint.recv() => {
                    info!("received SIGINT");
                    SecurityEvent::new("process.shutdown", "SIGINT", SecurityOutcome::Success)
                        .actor("system")
                        .detail("graceful shutdown initiated")
                        .emit();
                    let _ = shutdown_tx_signal.send(true);
                    return;
                }
                _ = sighup.recv() => {
                    info!("received SIGHUP, triggering manual config reload");
                    security::config_changed("signal", "system", "SIGHUP received, manual config reload");
                    let _ = reload_tx_signal.send(ReloadTrigger::Manual).await;
                }
            }
        }
    });

    // Spawn config reload loop (if enabled)
    if config.reload.enabled {
        let reload_config = config.clone();
        let reload_config_path = common.config.clone();
        let reload_config_dir = config_dir.clone();
        let reload_lifecycle = lifecycle.clone();
        let reload_metrics = metrics.clone();
        let reload_pid = vector_pid.clone();
        let reload_shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            run_reload_loop(
                reload_config,
                reload_config_path,
                reload_config_dir,
                reload_lifecycle,
                reload_metrics,
                reload_pid,
                reload_rx,
                reload_shutdown,
            )
            .await;
        });
    }

    // Run Vector subprocess lifecycle loop
    let backoff = BackoffConfig::default();
    let result = run_lifecycle(
        &config.vector,
        &config_dir,
        &lifecycle,
        &backoff,
        vector_pid.clone(),
        shutdown_rx,
    )
    .await;

    if let Err(e) = &result {
        error!(error = %e, "Vector lifecycle exited with error");
    }

    // Update final metrics
    let final_state = lifecycle.state();
    metrics.set_lifecycle_state(final_state);

    info!("shutdown complete");
    Ok(())
}
