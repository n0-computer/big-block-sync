#![allow(dead_code)]
use anyhow::{Context, Result, bail};
use bao_tree::blake3;
use big_block_sync::{Config, print_bitfields, print_stats, sync};
use clap::Parser;
use iroh::{SecretKey, Watcher, endpoint};
use iroh_blobs::{BlobFormat, BlobsProtocol, ticket::BlobTicket};
use rand::RngCore;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Gets a secret key from the IROH_SECRET environment variable or generates a new random one.
/// If the environment variable is set, it must be a valid string representation of a secret key.
pub fn get_or_generate_secret_key() -> Result<SecretKey> {
    use std::{env, str::FromStr};

    use anyhow::Context;
    use rand::thread_rng;
    if let Ok(secret) = env::var("IROH_SECRET") {
        // Parse the secret key from string
        SecretKey::from_str(&secret).context("Invalid secret key format")
    } else {
        // Generate a new random key
        let secret_key = SecretKey::generate(&mut thread_rng());
        println!(
            "Generated new secret key: {}",
            hex::encode(secret_key.to_bytes())
        );
        println!("To reuse this key, set the IROH_SECRET environment variable to this value");
        Ok(secret_key)
    }
}

// set the RUST_LOG env var to one of {debug,info,warn} to see logging info
pub fn setup_logging() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(EnvFilter::from_default_env())
        .try_init()
        .ok();
}

mod cli {
    use std::path::PathBuf;

    use clap::{Parser, Subcommand};
    use iroh_blobs::ticket::BlobTicket;

    #[derive(Debug, Parser)]
    #[command(version, about)]
    pub struct Args {
        #[clap(subcommand)]
        pub command: Commands,
    }

    #[derive(Subcommand, Clone, Debug)]
    pub enum Commands {
        /// Launch an iroh node and provide the content at the given path
        ///
        /// This is just a standard iroh-blobs provide, it is only in the
        /// example so the example is self-contained.
        #[group(required = true, multiple = false)]
        Provide {
            /// path to the file you want to provide.
            path: Option<PathBuf>,

            /// Size in bytes, data will be randomly generated
            #[arg(long, short)]
            n: Option<usize>,
        },
        /// Syncs a blob from multiple sources, given as tickets.
        ///
        /// As opposed to other examples, the blobs must not have the same hash!
        /// They just must have the same size. The resulting file will be
        /// built out of ranges of the source blobs, but will not necessarily
        /// have the same hash as any of the input blobs unless there is only
        /// one provider.
        ///
        /// Syncing does not accept connections, it is a pure client.
        Sync {
            tickets: Vec<BlobTicket>,

            /// Path to the file where the synced content will be saved
            #[clap(long)]
            target: Option<PathBuf>,

            /// Verbosity level
            ///
            /// 1 will show stats,
            /// 2 will show stats and bitfields
            #[arg(short, long, action = clap::ArgAction::Count)]
            verbose: u8,

            /// Block size in BLAKE3 chunks of 1024 bytes.
            ///
            /// Making this too large will reduce adaptation rate.
            ///
            /// if size / (block_size * 1024) is larger than the number of
            /// nodes to download from, you will get reduced parallelism.
            #[arg(long, default_value_t = 1024)]
            block_size: u64,

            /// Parallelism level. Even when many nodes are provided, the
            /// downloader will not use all of them concurrently but will try
            /// to choose the best ones.
            #[arg(long, default_value_t = 8)]
            parallelism: usize,
        },
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    setup_logging();
    let args = cli::Args::parse();
    match args.command {
        cli::Commands::Provide { path, n } => {
            let secret = get_or_generate_secret_key()?;
            let store = iroh_blobs::store::mem::MemStore::new();
            let endpoint = endpoint::Endpoint::builder()
                .secret_key(secret)
                .bind()
                .await?;
            let tag = if let Some(path) = path {
                store.add_path(path).await?
            } else if let Some(n) = n {
                let mut rng = rand::thread_rng();
                let mut data = vec![0u8; n];
                rng.fill_bytes(&mut data);
                store.add_bytes(data).await?
            } else {
                bail!("No path or size provided");
            };
            endpoint.home_relay().initialized().await;
            let addr = endpoint.node_addr().initialized().await;
            let node_id = addr.node_id;
            let proto = BlobsProtocol::new(&store, endpoint.clone(), None);
            let router = iroh::protocol::Router::builder(endpoint.clone())
                .accept(iroh_blobs::ALPN, proto.clone())
                .spawn();
            let ticket = BlobTicket::new(addr, tag.hash, BlobFormat::Raw);
            println!("Node id: {node_id}");
            println!("Providing content with ticket:\n{ticket}");
            tokio::signal::ctrl_c().await?;
            router.shutdown().await?;
        }
        cli::Commands::Sync {
            tickets,
            target,
            verbose,
            block_size,
            parallelism,
        } => {
            let blobs = tickets
                .into_iter()
                .map(|t| (t.node_addr().clone(), t.hash()))
                .collect::<Vec<_>>();
            let config = Config {
                parallelism,
                block_size,
                min_rate: None,
                rate_ratio: None,
                latency: Default::default(),
            };
            let (res, stats) = sync(blobs, config, verbose).await?;
            if verbose > 0 {
                print_stats(&stats);
            }
            if verbose > 1 {
                print_bitfields(&stats, res.len());
            }
            if let Some(target) = target {
                tokio::fs::write(target, res)
                    .await
                    .context("Failed to write content to target file")?;
            } else {
                println!(
                    "Downloaded content: {} bytes, hash {}",
                    res.len(),
                    blake3::hash(&res)
                );
            }
        }
    }
    Ok(())
}
