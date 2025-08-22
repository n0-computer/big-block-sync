use std::{
    collections::HashMap,
    ops::DerefMut,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use bao_tree::{ChunkNum, ChunkRanges, blake3, io::BaoContentItem};
use clap::Parser;
use iroh::{Endpoint, NodeId, Watcher, discovery::static_provider::StaticProvider, endpoint};
use iroh_blobs::{
    BlobFormat, BlobsProtocol, Hash,
    get::{
        self,
        fsm::{BlobContentNext, ConnectedNext, EndBlobNext},
    },
    protocol::{ChunkRangesExt, GetRequest},
    ticket::BlobTicket,
    util::connection_pool::ConnectionPool,
};
mod common;
use common::{get_or_generate_secret_key, setup_logging};
use n0_future::{BufferedStreamExt, FuturesUnordered, StreamExt, future::Boxed, stream};
use range_collections::range_set::RangeSetRange;
use tokio::sync::oneshot;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    setup_logging();
    let args = cli::Args::parse();
    match args.command {
        cli::Commands::Provide { path } => {
            let secret = get_or_generate_secret_key()?;
            let store = iroh_blobs::store::mem::MemStore::new();
            let endpoint = endpoint::Endpoint::builder()
                .secret_key(secret)
                .bind()
                .await?;
            let path = path.canonicalize()?;
            let tag = store.add_path(path).await?;
            endpoint.home_relay().initialized().await;
            let addr = endpoint.node_addr().initialized().await;
            let proto = BlobsProtocol::new(&store, endpoint.clone(), None);
            let router = iroh::protocol::Router::builder(endpoint.clone())
                .accept(iroh_blobs::ALPN, proto.clone())
                .spawn();
            let ticket = BlobTicket::new(addr, tag.hash, BlobFormat::Raw);
            println!("Providing content with ticket:\n{ticket}");
            tokio::signal::ctrl_c().await?;
            router.shutdown().await?;
        }
        cli::Commands::Sync {
            tickets,
            target,
            verbose,
        } => {
            sync(tickets, target, verbose).await?;
        }
    }
    Ok(())
}

/// Get latency and size for a single hash
///
/// We get the size just so we have timings, then get the latency from the
/// endpoint.
async fn get_latency_and_size(
    endpoint: &Endpoint,
    pool: &ConnectionPool,
    node_id: &NodeId,
    hash: &Hash,
) -> Result<(Duration, u64)> {
    let conn = pool.get_or_connect(*node_id).await?;
    let (size, _stats) = iroh_blobs::get::request::get_verified_size(&conn, &hash).await?;
    let latency = endpoint
        .remote_info(*node_id)
        .and_then(|info| info.latency)
        .context("No latency information available")?;
    Ok((latency, size))
}

/// Get latencies and sizes for multiple hashes.
///
/// This gives us some initial estimate of the connection quality and also will
/// immediately filter out nodes that are not reachable.
async fn get_latencies_and_sizes(
    infos: &HashMap<NodeId, Hash>,
    endpoint: &Endpoint,
    pool: &ConnectionPool,
    parallelism: usize,
) -> HashMap<NodeId, Result<(Duration, u64)>> {
    stream::iter(infos.iter().clone())
        .map(|(id, hash)| {
            let pool = pool.clone();
            let endpoint = endpoint.clone();
            async move {
                match get_latency_and_size(&endpoint, &pool, id, hash).await {
                    Ok((latency, size)) => (*id, Ok((latency, size))),
                    Err(e) => (*id, Err(e)),
                }
            }
        })
        .buffered_unordered(parallelism)
        .collect::<HashMap<_, _>>()
        .await
}

async fn sync(tickets: Vec<BlobTicket>, target: Option<PathBuf>, verbose: u8) -> Result<()> {
    // if there are multiple hashes for one node id, we will just choose the last one!
    let hashes = tickets
        .iter()
        .map(|t| (t.node_addr().node_id, t.hash()))
        .collect::<HashMap<_, _>>();
    // we take all addr info. If there are multiple, they will be combined except for
    // the relay url, which will be the last one.
    let addrs = tickets
        .iter()
        .map(|t| t.node_addr().clone())
        .collect::<Vec<_>>();
    // give the endpoint the info it needs to dial all nodes.
    //
    // we don't use dynamic discovery but just give it the info directly, so
    // connections won't work if the direct addresses and the relay URL in the
    // tickets is no longer correct.
    let discovery = StaticProvider::from_node_info(addrs);
    let endpoint = endpoint::Endpoint::builder()
        .add_discovery(discovery)
        .bind()
        .await?;
    // create a connection pool
    let pool = ConnectionPool::new(endpoint.clone(), iroh_blobs::ALPN, Default::default());
    // get latency and size for all nodes. This should be very quick!
    let latencies_and_sizes = get_latencies_and_sizes(&hashes, &endpoint, &pool, 32).await;
    let sizes = latencies_and_sizes
        .iter()
        .filter_map(|(_, res)| res.as_ref().ok().map(|(_, s)| *s))
        .collect::<Vec<_>>();
    if sizes.is_empty() {
        bail!("No valid nodes found to sync from.");
    }
    let size = sizes[0];
    if sizes.iter().any(|s| *s != size) {
        bail!("All nodes must have the same size, but got: {:?}", sizes);
    }
    // Latency can be used as an initial hint which nodes are good
    let latency = latencies_and_sizes
        .iter()
        .filter_map(|(id, res)| res.as_ref().ok().map(|(l, _)| (*id, *l)))
        .collect::<HashMap<_, _>>();
    println!("{:?}", latency);
    // Print non-reachable nodes
    for (id, r) in latencies_and_sizes {
        if r.is_err() {
            println!("Node{id} is not reachable: {:?}", r);
        }
    }
    let size = usize::try_from(size).context("Size is too large to fit into a usize")?;
    let block_size = ChunkNum(1024); // 1 MiB block size
    let downloader = Downloader::new(hashes, size, block_size, pool);
    let (res, stats) = downloader
        .run()
        .await
        .context("Failed to download content")?;
    if verbose > 0 {
        print_stats(&stats);
    }
    if verbose > 1 {
        print_bitfields(&stats, size);
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
    Ok(())
}

fn total_chunks(ranges: &ChunkRanges) -> Option<ChunkNum> {
    let mut res = 0;
    for range in ranges.iter() {
        match range {
            RangeSetRange::Range(r) => {
                res += r.end.0 - r.start.0;
            }
            RangeSetRange::RangeFrom(_) => return None,
        }
    }
    Some(ChunkNum(res))
}

/// Claim up to max chunks from the unclaimed chunks.
///
/// The unclaimed ranges are not modified. You have to do this yourself.
fn claim(unclaimed: &ChunkRanges, max: ChunkNum) -> ChunkRanges {
    let mut res = ChunkRanges::empty();
    let mut remaining = max;
    for range in unclaimed.iter() {
        match range {
            RangeSetRange::Range(r) => {
                let end = *r.start + remaining;
                if &end <= r.end {
                    res |= ChunkRanges::from(*r.start..end);
                    break;
                } else {
                    res |= ChunkRanges::from(*r.start..*r.end);
                    remaining = remaining - (*r.end - *r.start);
                }
            }
            RangeSetRange::RangeFrom(r) => {
                let end = *r.start + remaining;
                res |= ChunkRanges::from(*r.start..end);
                break;
            }
        }
    }
    res
}

#[derive(Debug, Default)]
struct Target {
    data: Vec<u8>,
    missing: ChunkRanges,
}

#[derive(Debug, Default)]
struct PerNodeStats {
    // total downloaded chunks from this node
    ranges: ChunkRanges,
    // error count
    errors: u64,
    // total time this node was downloading
    time: Duration,
    // start of the current download
    start: Option<Instant>,
}

fn print_stats(stats: &HashMap<NodeId, PerNodeStats>) {
    println!("Node{}\tErrors\tChunks\tDuration\tRate", " ".repeat(64 - 3));
    for (id, stat) in stats {
        let total = total_chunks(&stat.ranges).unwrap_or(ChunkNum(0));
        let rate = (total.0 as f64) / 1024.0 / stat.time.as_secs_f64();
        println!(
            "{}\t{}\t{}\t{:<8.3}s\t{:<8.3} MiB/s",
            id,
            stat.errors,
            total.0,
            stat.time.as_secs_f64(),
            rate
        );
    }
}

fn print_bitfield(bitfield: &ChunkRanges, size: usize) -> String {
    let bucket_size_bytes = (size + 99) / 100;
    let buckets = (size + bucket_size_bytes - 1) / bucket_size_bytes;
    let mut count = vec![0usize; buckets];
    for range in bitfield.iter() {
        match range {
            RangeSetRange::Range(r) => {
                // thanks claude!
                let range_start_bytes = r.start.0 as usize * 1024;
                let range_end_bytes = r.end.0 as usize * 1024; // exclusive

                let start_bucket = range_start_bytes / bucket_size_bytes;
                let end_bucket = (range_end_bytes - 1) / bucket_size_bytes; // Last actual byte

                for bucket_idx in start_bucket..=end_bucket.min(buckets - 1) {
                    let bucket_start = bucket_idx * bucket_size_bytes;
                    let bucket_end = ((bucket_idx + 1) * bucket_size_bytes).min(size);

                    let overlap_start = range_start_bytes.max(bucket_start);
                    let overlap_end = range_end_bytes.min(bucket_end);

                    count[bucket_idx] += overlap_end - overlap_start;
                }
            }
            RangeSetRange::RangeFrom(_) => {
                return "Open range".into();
            }
        }
    }
    fn bucket_to_char(count: usize, bucket_size_bytes: usize) -> char {
        let ratio = count as f64 / bucket_size_bytes as f64;
        match ratio {
            r if r == 0.0 => ' ',   // Empty (white/background)
            r if r <= 0.125 => '░', // Light gray
            r if r <= 0.25 => '░',  // Light gray
            r if r <= 0.375 => '▒', // Medium gray
            r if r <= 0.5 => '▒',   // Medium gray
            r if r <= 0.625 => '▓', // Dark gray
            r if r <= 0.75 => '▓',  // Dark gray
            r if r <= 0.875 => '█', // Almost black
            _ => '█',               // Full black
        }
    }
    count
        .iter()
        .map(|&c| bucket_to_char(c, bucket_size_bytes))
        .collect()
}

fn print_bitfields(stats: &HashMap<NodeId, PerNodeStats>, size: usize) {
    println!("Node       Bitfield");
    for (id, stat) in stats {
        let bitfield_str = print_bitfield(&stat.ranges, size);
        println!("{} {}", id.fmt_short(), bitfield_str);
    }
}

struct Downloader {
    ctx: Arc<Ctx>,
    tasks: FuturesUnordered<Boxed<(NodeId, ChunkRanges, anyhow::Result<()>)>>,
    unclaimed: ChunkRanges,
    hashes: HashMap<NodeId, Hash>,
    stats: HashMap<NodeId, PerNodeStats>,
    current: HashMap<NodeId, oneshot::Sender<()>>,
    block_size: ChunkNum,
}

impl Downloader {
    fn new(
        hashes: HashMap<NodeId, Hash>,
        size: usize,
        block_size: ChunkNum,
        pool: ConnectionPool,
    ) -> Self {
        let target = Target::new(size);
        let unclaimed = target.missing.clone();
        Self {
            ctx: Arc::new(Ctx {
                target: Mutex::new(target),
                pool,
            }),
            tasks: FuturesUnordered::new(),
            unclaimed,
            hashes,
            current: HashMap::new(),
            stats: HashMap::new(),
            block_size,
        }
    }

    async fn run(mut self) -> Result<(Vec<u8>, HashMap<NodeId, PerNodeStats>)> {
        let chunk_size = self.block_size;
        for (id, _) in self.hashes.clone() {
            let claim = claim(&self.unclaimed, chunk_size);
            if claim.is_empty() {
                break;
            }
            self.spawn_download(id, claim);
        }
        loop {
            tokio::select! {
                res = self.tasks.next() => {
                    let Some((id, ranges, result)) = res else {
                        break;
                    };
                    let stats = self.stats.entry(id).or_default();
                    self.current.remove(&id);
                    stats.ranges |= ranges.clone();
                    let start = stats.start.expect("Start time should be set when spawning download");
                    stats.time += start.elapsed();
                    if result.is_err() {
                        let target = self.ctx.target.lock().unwrap();
                        self.unclaimed |= ranges & target.missing.clone();
                        stats.errors += 1;
                    }
                    let claim = claim(&self.unclaimed, chunk_size);
                    if !claim.is_empty() {
                        self.spawn_download(id, claim);
                    } else {
                        // todo: go into finish mode
                    }
                }
            }
        }
        let target = std::mem::take(self.ctx.target.lock().unwrap().deref_mut());
        assert!(
            self.unclaimed.is_empty(),
            "Unclaimed ranges should be empty at the end"
        );
        assert!(
            target.missing.is_empty(),
            "Target should have no missing ranges at the end"
        );
        // take the target out of the context
        Ok((target.data, self.stats))
    }

    fn spawn_download(&mut self, id: NodeId, claim: ChunkRanges) {
        let hash = self.hashes[&id].clone();
        info!("Downloading chunks {:?} from {}", claim, id);
        self.unclaimed -= claim.clone();
        let (tx, rx) = oneshot::channel();
        self.current.insert(id, tx);
        self.stats.entry(id).or_default().start = Some(Instant::now());
        self.tasks.push(Box::pin(
            self.ctx.clone().download_range(id, hash, claim, rx),
        ));
    }
}

struct Ctx {
    target: Mutex<Target>,
    pool: ConnectionPool,
}

impl Ctx {
    async fn download_range(
        self: Arc<Self>,
        id: NodeId,
        hash: Hash,
        ranges: ChunkRanges,
        cancel: oneshot::Receiver<()>,
    ) -> (NodeId, ChunkRanges, anyhow::Result<()>) {
        let result = tokio::select! {
            res = self.download_range_impl(id, hash, ranges.clone()) => res,
            _ = cancel => Err(anyhow::anyhow!("Download cancelled")),
        };
        (id, ranges.clone(), result)
    }

    async fn download_range_impl(&self, id: NodeId, hash: Hash, ranges: ChunkRanges) -> Result<()> {
        let connection = self
            .pool
            .get_or_connect(id)
            .await
            .context("Failed to connect to node")?;
        let request = GetRequest::builder().next(ranges).build(hash);
        let fsm = get::fsm::start(connection.clone(), request, Default::default());
        let connected = fsm.next().await?;
        let ConnectedNext::StartRoot(root) = connected.next().await? else {
            bail!("Expected StartRoot state");
        };
        let (mut c, _size) = root.next().next().await?;
        let end = loop {
            match c.next().await {
                BlobContentNext::More((c1, r)) => {
                    if let BaoContentItem::Leaf(leaf) = r? {
                        let offset = usize::try_from(leaf.offset).context("offset too large")?;
                        let len = leaf.data.len();
                        let mut target = self.target.lock().unwrap();
                        target.data[offset..offset + len].copy_from_slice(&leaf.data);
                        target.missing -= ChunkRanges::bytes(leaf.offset..leaf.offset + len as u64);
                    }
                    c = c1;
                }
                BlobContentNext::Done(end) => {
                    break end;
                }
            }
        };
        let EndBlobNext::Closing(closing) = end.next() else {
            bail!("Expected Closing state");
        };
        let _stats = closing.next().await?;
        Ok(())
    }
}

impl Target {
    fn new(size: usize) -> Self {
        Self {
            data: vec![0; size],
            missing: ChunkRanges::bytes(0..size as u64),
        }
    }
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
        Provide {
            /// path to the file you want to provide.
            path: PathBuf,
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

            #[arg(short, long, action = clap::ArgAction::Count)]
            verbose: u8,
        },
    }
}
