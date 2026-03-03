// Project:   dfe-transform-vector
// File:      src/main.rs
// Purpose:   CLI entry point
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! CLI entry point for dfe-transform-vector.

use clap::Parser;
use tracing::{error, info};

use dfe_transform_vector::config::Config;

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

    // Subprocess lifecycle, health/metrics servers, signal handling
    // deferred to TODO 3.x and 4.x

    info!("shutdown complete");
    Ok(())
}
