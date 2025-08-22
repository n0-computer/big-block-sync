use std::{
    cmp::Reverse,
    collections::HashMap,
    ops::DerefMut,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use bao_tree::{ChunkNum, ChunkRanges, blake3, io::BaoContentItem};
use clap::Parser;
use iroh::{NodeId, Watcher, discovery::static_provider::StaticProvider, endpoint};
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
use rand::RngCore;
use range_collections::range_set::RangeSetRange;
use tokio::sync::oneshot;
use tracing::{info, warn};

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
            sync(tickets, target, block_size, verbose, parallelism).await?;
        }
    }
    Ok(())
}

/// Get latency and size for a single hash
///
/// We get the size just so we have timings, then get the latency from the
/// endpoint.
async fn get_latency_and_size(
    pool: &ConnectionPool,
    node_id: &NodeId,
    hash: &Hash,
) -> Result<(Duration, u64)> {
    let conn = pool.get_or_connect(*node_id).await?;
    let (size, _stats) = iroh_blobs::get::request::get_verified_size(&conn, &hash).await?;
    let latency = conn.rtt();
    Ok((latency, size))
}

/// Get latencies and sizes for multiple hashes.
///
/// This gives us some initial estimate of the connection quality and also will
/// immediately filter out nodes that are not reachable.
async fn get_latencies_and_sizes(
    infos: &HashMap<NodeId, Hash>,
    pool: &ConnectionPool,
    parallelism: usize,
) -> HashMap<NodeId, Result<(Duration, u64)>> {
    stream::iter(infos.iter().clone())
        .map(|(id, hash)| {
            let pool = pool.clone();
            async move {
                match get_latency_and_size(&pool, id, hash).await {
                    Ok((latency, size)) => (*id, Ok((latency, size))),
                    Err(e) => (*id, Err(e)),
                }
            }
        })
        .buffered_unordered(parallelism)
        .collect::<HashMap<_, _>>()
        .await
}

async fn sync(
    tickets: Vec<BlobTicket>,
    target: Option<PathBuf>,
    block_size_chunks: u64,
    verbose: u8,
    parallelism: usize,
) -> Result<()> {
    let block_size = ChunkNum(block_size_chunks);
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
    let latencies_and_sizes = get_latencies_and_sizes(&hashes, &pool, 32).await;
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
    if verbose > 0 {
        println!("Node       Initial Latency (ms)");
        for (id, l) in &latency {
            println!("{} {:<8.3}ms", id.fmt_short(), l.as_secs_f64() * 1000.0);
        }
    }
    // Print non-reachable nodes
    for (id, r) in latencies_and_sizes {
        if r.is_err() {
            println!("Node{id} is not reachable: {:?}", r);
        }
    }
    let size = usize::try_from(size).context("Size is too large to fit into a usize")?;
    let downloader = Downloader::new(hashes, size, pool, &latency, block_size, parallelism);
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
    // initial latency
    latency: Duration,
}

impl PerNodeStats {
    /// Total number of chunks downloaded from this node
    fn total_chunks(&self) -> Option<ChunkNum> {
        total_chunks(&self.ranges)
    }

    /// Total number of bytes downloaded from this node
    fn total_bytes(&self) -> Option<usize> {
        Some(self.total_chunks()?.to_bytes() as usize)
    }

    /// Download rate from this node, if available
    fn rate(&self) -> Option<f64> {
        let total = self.total_bytes()?;
        if self.time == Duration::ZERO {
            // we have not yet downloaded anything!
            return None;
        }
        Some(total as f64 / self.time.as_secs_f64())
    }

    /// Quality metric for this node, lower is better
    fn quality(&self) -> Quality {
        Quality::new(self.errors, self.rate(), self.latency)
    }
}

/// Quality metric for this node, lower is better
///
/// Ordering is as follows:
/// - connections without errors will be preferred, lower errors wins
/// - connections with rate will be preferred, higher rate wins
/// - connections without rate will be sorted by latency, lower wins
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Quality {
    errors: u64,
    rate: Reverse<Option<u64>>, // Some comes first, higher rates come first
    latency: Duration,
}

impl Quality {
    fn new(errors: u64, rate: Option<f64>, latency: Duration) -> Self {
        Self {
            errors,
            rate: Reverse(rate.map(|x| x as u64)),
            latency,
        }
    }

    fn rate(&self) -> Option<u64> {
        self.rate.0
    }
}

impl std::fmt::Display for Quality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "errors: {}, rate: {:?}, latency: {:?}",
            self.errors, self.rate.0, self.latency
        )
    }
}

#[test]
fn test_quality_ordering() {
    // thanks claude!
    let mut qualities = vec![
        // Worst: high errors, no rate, high latency
        Quality::new(2, None, Duration::from_millis(100)),
        // Bad: high errors with rate
        Quality::new(2, Some(1000.5), Duration::from_millis(20)),
        // Medium: some errors, no rate, low latency
        Quality::new(1, None, Duration::from_millis(10)),
        // Good: no errors, no rate
        Quality::new(0, None, Duration::from_millis(50)),
        // Better: no errors, low rate
        Quality::new(0, Some(500.8), Duration::from_millis(30)),
        // Same rate after truncation, higher latency (worse)
        Quality::new(0, Some(2000.9), Duration::from_millis(40)),
        // Same rate after truncation, lower latency (better)
        Quality::new(0, Some(2000.1), Duration::from_millis(10)),
        // Best: no errors, highest rate, lowest latency
        Quality::new(0, Some(3000.0), Duration::from_millis(5)),
    ];

    qualities.sort();

    // Verify the sorted order (best to worst)
    let expected = vec![
        Quality::new(0, Some(3000.0), Duration::from_millis(5)), // Best
        Quality::new(0, Some(2000.1), Duration::from_millis(10)), // Same rate after truncation, lower latency wins
        Quality::new(0, Some(2000.9), Duration::from_millis(40)), // Same rate after truncation, higher latency
        Quality::new(0, Some(500.8), Duration::from_millis(30)),
        Quality::new(0, None, Duration::from_millis(50)), // No rate, uses latency
        Quality::new(1, None, Duration::from_millis(10)),
        Quality::new(2, Some(1000.5), Duration::from_millis(20)),
        Quality::new(2, None, Duration::from_millis(100)), // Worst
    ];

    assert_eq!(qualities, expected);
}
fn print_stats(stats: &HashMap<NodeId, PerNodeStats>) {
    println!("Node       Errors\tChunks\tDuration\tRate");
    for (id, stat) in stats {
        let total = stat.total_chunks().unwrap_or_default();
        let rate = stat.rate().unwrap_or(f64::NAN) / (1024.0 * 1024.0);
        println!(
            "{}\t{}\t{}\t{:<8.3}s\t{:<8.3} MiB/s",
            id.fmt_short(),
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
    /// Contect needed for the per-node tasks
    ctx: Arc<Ctx>,
    /// Futures for currently active downloads
    tasks: FuturesUnordered<Boxed<(NodeId, ChunkRanges, Option<anyhow::Result<()>>)>>,
    /// Unclaimed chunks that are not yet assigned to any download
    unclaimed: ChunkRanges,
    /// Mapping from node id to hash, to know what do download
    hashes: HashMap<NodeId, Hash>,
    /// Per node statistics. Note that this will also be filled for nodes we never
    /// talked to, using the initial latency.
    stats: HashMap<NodeId, PerNodeStats>,
    /// Kill handles for currently active downloads
    current: HashMap<NodeId, oneshot::Sender<()>>,
    /// Block size for downloads
    block_size: ChunkNum,
    /// Maximum number of concurrent downloads
    parallelism: usize,
}

impl Downloader {
    fn new(
        hashes: HashMap<NodeId, Hash>,
        size: usize,
        pool: ConnectionPool,
        latency: &HashMap<NodeId, Duration>,
        block_size: ChunkNum,
        parallelism: usize,
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
            stats: latency
                .iter()
                .map(|(id, l)| {
                    (
                        *id,
                        PerNodeStats {
                            latency: *l,
                            ..Default::default()
                        },
                    )
                })
                .collect(),
            block_size,
            parallelism,
        }
    }

    /// Best n nodes, free or busy (for quality, lower is better)
    #[allow(dead_code)]
    fn all_by_quality(&self, n: usize) -> Vec<(Quality, NodeId)> {
        let mut qualities: Vec<_> = self
            .stats
            .iter()
            .map(|(id, stat)| (stat.quality(), *id))
            .collect();
        qualities.sort();
        qualities.truncate(n);
        qualities
    }

    /// Give the first n free nodes by quality (for quality, lower is better)
    fn free_by_quality(&self, n: usize) -> Vec<(Quality, NodeId)> {
        let mut qualities: Vec<_> = self
            .stats
            .iter()
            .filter(|(id, _)| !self.current.contains_key(*id))
            .map(|(id, stat)| (stat.quality(), *id))
            .collect();
        qualities.sort();
        qualities.truncate(n);
        qualities
    }

    /// Give the first n busy nodes by reverse quality (higher, so worse, qualities first)
    ///
    /// The worst node should come first.
    fn busy_by_quality_rev(&self, n: usize) -> Vec<(Quality, NodeId)> {
        let mut qualities: Vec<_> = self
            .stats
            .iter()
            .filter(|(id, _)| self.current.contains_key(*id))
            .map(|(id, stat)| (stat.quality(), *id))
            .collect();
        qualities.sort_by(|a, b| b.cmp(a));
        qualities.truncate(n);
        qualities
    }

    /// Update state based on task result
    fn handle_task_result(&mut self, res: (NodeId, ChunkRanges, Option<anyhow::Result<()>>)) {
        let (id, ranges, result) = res;
        let stats = self.stats.entry(id).or_default();
        self.current.remove(&id);
        stats.ranges |= ranges.clone();
        let start = stats
            .start
            .expect("Start time should be set when spawning download");
        stats.time += start.elapsed();
        let success = matches!(result, Some(Ok(_)));
        if !success {
            // add back unclaimed ranges since the download failed
            if let Some(Err(e)) = result {
                warn!(
                    "Download from {} failed for ranges {:?}: {}",
                    id.fmt_short(),
                    ranges,
                    e
                );
                // only increase error count if there was an actual error.
                // when we kill the task that is not the fault of the remote node.
                stats.errors += 1;
            }
            let target = self.ctx.target.lock().unwrap();
            self.unclaimed |= ranges & target.missing.clone();
        }
    }

    /// Claim a chunk and spawn a task.
    ///
    /// If there are no chunks to be claimed, this will see if there is a busy
    /// task that is not performing well and kill it.
    ///
    /// The latter will only happen as the download nears the end, so it is called
    /// finish mode.
    async fn claim_and_spawn(&mut self) -> Result<()> {
        let chunk_size = self.block_size;
        let claim = claim(&self.unclaimed, chunk_size);
        if !claim.is_empty() {
            // choose the best node to download from. In many cases this will be the same node
            // that we just used, but not always. E.g. if the op produced an error.
            let Some((_, id)) = self.free_by_quality(1).into_iter().next() else {
                // this should never happen unless we started with 0 nodes.
                // even nodes with errors will be considered here!
                bail!("No free nodes available to download from");
            };
            // todo: abort here if even the best node has lots of errors?
            self.spawn_download(id, claim);
        } else {
            // find the worst busy node
            let Some((worst_current, current_id)) = self.busy_by_quality_rev(1).into_iter().next()
            else {
                return Ok(());
            };
            let Some((best_free, _)) = self.free_by_quality(1).into_iter().next() else {
                // this should never happen unless we started with 0 nodes.
                // even nodes with errors will be considered here!
                bail!("No free nodes available to download from");
            };
            if worst_current < best_free {
                // worst_current is better than best_free, so we can keep it running
                return Ok(());
            }

            // we don't look at actually downloaded data for the worst. Maybe we should, because
            // it might be almost done. But then again, probably does not matter.
            let kill = match (worst_current.rate(), best_free.rate()) {
                // both have rates, kill if worst rate is much worse than best free rate
                (Some(w), Some(b)) => w * 4 < b,
                // worst current has a rate, but best free does not, so we can keep it running
                (Some(_), None) => false,
                // worst current has no rate, but best free does. kill it.
                (None, Some(_)) => true,
                // we don't know rate for any, might as well let it run
                (None, None) => false,
            };

            if kill {
                info!(
                    "Killing download from {} because best free is much better\n-curr: {}\n-free: {}",
                    current_id.fmt_short(),
                    worst_current,
                    best_free
                );
                // just cancelling the current worst download is enough.
                // this will make room in claimed and respawn a new download,
                // unless the download has finished by now.
                self.current
                    .remove(&current_id)
                    .expect("Current ID should be in current")
                    .send(())
                    .ok();
            }
        }
        Ok(())
    }

    async fn run(mut self) -> Result<(Vec<u8>, HashMap<NodeId, PerNodeStats>)> {
        let chunk_size = self.block_size;
        let initial = self.free_by_quality(self.parallelism);
        for (_, id) in initial {
            let claim = claim(&self.unclaimed, chunk_size);
            if claim.is_empty() {
                break;
            }
            self.spawn_download(id, claim);
        }
        while let Some(res) = self.tasks.next().await {
            self.handle_task_result(res);
            self.claim_and_spawn().await?;
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
        info!("Downloading chunks {:?} from {}", claim, id.fmt_short());
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
    /// download range task.
    ///
    /// This is a separate fn since we want to thread through id, hash and ranges
    /// even in case of an error.
    ///
    /// A result of None indicates that the task has been killed. This should not
    /// count towards errors.
    async fn download_range(
        self: Arc<Self>,
        id: NodeId,
        hash: Hash,
        ranges: ChunkRanges,
        cancel: oneshot::Receiver<()>,
    ) -> (NodeId, ChunkRanges, Option<anyhow::Result<()>>) {
        let result = tokio::select! {
            res = self.download_range_impl(id, hash, ranges.clone()) => {
                if res.is_err() {
                    // tell the pool that there was an error and this connection is bad.
                    // todo: we should only do this if it was a connection error, not a
                    // blobs error, maybe?
                    self.pool.close(id).await.ok();
                }
                Some(res)
            },
            _ = cancel => None,
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
