// Project:   dfe-transform-vector
// File:      src/main.rs
// Purpose:   CLI entry point and orchestrator
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! CLI entry point for dfe-transform-vector.
//!
//! Orchestrates: config load → assemble → validate → spawn Vector
//! → health/metrics servers → shutdown handling.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use tracing::{error, info};

use dfe_transform_vector::config::Config;
use dfe_transform_vector::config::assembler;
use dfe_transform_vector::config::validate::{check_vector_version, vector_validate};
use dfe_transform_vector::health::serve_health;
use dfe_transform_vector::metrics::{WrapperMetrics, serve_metrics};
use dfe_transform_vector::vector::lifecycle::State;
use dfe_transform_vector::vector::{BackoffConfig, Lifecycle, run_lifecycle};

#[derive(Parser, Debug)]
#[command(name = "dfe-transform-vector")]
#[command(about = "Kafka-to-Kafka transform pipelines powered by Vector.dev")]
#[command(version)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, env = "DFE_TRANSFORM_CONFIG")]
    config: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, env = "DFE_TRANSFORM_LOG_LEVEL", default_value = "info")]
    log_level: String,

    /// Log format (json, text)
    #[arg(long, env = "DFE_TRANSFORM_LOG_FORMAT", default_value = "json")]
    log_format: String,

    /// Validate config and exit
    #[arg(long)]
    validate: bool,

    /// Print effective config and exit
    #[arg(long)]
    print_config: bool,

    /// Assemble Vector config and exit (writes to --config-dir)
    #[arg(long)]
    assemble_only: bool,

    /// Output directory for assembled Vector config
    #[arg(long, default_value = assembler::DEFAULT_CONFIG_DIR)]
    config_dir: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialise tracing based on format
    if args.log_format == "json" {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(&args.log_level)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(&args.log_level)
            .init();
    }

    // Load configuration
    let config = match Config::load(args.config.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to load configuration");
            std::process::exit(1);
        }
    };

    // Validate configuration
    if let Err(e) = config.validate() {
        error!(error = %e, "configuration validation failed");
        std::process::exit(1);
    }

    // Print config and exit if requested
    if args.print_config {
        println!("{:#?}", config);
        return Ok(());
    }

    // Validate only mode
    if args.validate {
        info!("configuration is valid");
        return Ok(());
    }

    // Startup version check (fire-and-forget, never blocks)
    // Requires hyperi-rustlib — uncomment when registry auth is configured
    // hyperi_rustlib::VersionCheck::new(hyperi_rustlib::VersionCheckConfig {
    //     product: "dfe-transform-vector".into(),
    //     current_version: env!("CARGO_PKG_VERSION").into(),
    //     ..Default::default()
    // })
    // .check_on_startup();

    info!(
        pipeline = %config.pipeline.name,
        version = env!("CARGO_PKG_VERSION"),
        "starting dfe-transform-vector"
    );

    // Lifecycle state machine
    let lifecycle = Lifecycle::new();
    lifecycle.set(State::Initialising);

    // Check Vector binary version
    match check_vector_version(&config.vector).await {
        Ok(version) => {
            if !version.is_empty() {
                info!(vector_version = %version, "Vector binary version detected");
            }
        }
        Err(e) => {
            error!(error = %e, "Vector version check failed");
            std::process::exit(1);
        }
    }

    // Assemble Vector config directory
    lifecycle.set(State::Validating);
    let config_dir = PathBuf::from(&args.config_dir);
    if let Err(e) = assembler::assemble(&config, &config_dir) {
        error!(error = %e, "failed to assemble Vector config");
        std::process::exit(1);
    }

    // Run vector validate on assembled config
    if let Err(e) = vector_validate(&config.vector, &config_dir).await {
        error!(error = %e, "Vector config validation failed");
        std::process::exit(1);
    }
    info!("Vector config validation passed");

    // Assemble-only mode
    if args.assemble_only {
        info!(dir = %config_dir.display(), "assembled config written");
        return Ok(());
    }

    // Wrapper metrics
    let metrics = Arc::new(WrapperMetrics::new());
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

    // Signal handler: SIGTERM and SIGINT trigger shutdown
    let shutdown_tx_signal = shutdown_tx.clone();
    tokio::spawn(async move {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        let mut sigint =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();

        tokio::select! {
            _ = sigterm.recv() => {
                info!("received SIGTERM");
            }
            _ = sigint.recv() => {
                info!("received SIGINT");
            }
        }
        let _ = shutdown_tx_signal.send(true);
    });

    // Run Vector subprocess lifecycle loop
    let backoff = BackoffConfig::default();
    let result = run_lifecycle(
        &config.vector,
        &config_dir,
        &lifecycle,
        &backoff,
        shutdown_rx,
    )
    .await;

    if let Err(e) = &result {
        error!(error = %e, "Vector lifecycle exited with error");
    }

    // Update final metrics
    metrics.set_lifecycle_state(lifecycle.state());

    info!("shutdown complete");
    Ok(())
}
