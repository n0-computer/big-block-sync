use std::{
    collections::HashMap,
    ops::DerefMut,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use bao_tree::{blake3, io::BaoContentItem, ChunkNum, ChunkRanges};
use clap::Parser;
use iroh::{discovery::static_provider::StaticProvider, endpoint, Endpoint, NodeId, Watcher};
use iroh_blobs::{
    get::{
        self,
        fsm::{BlobContentNext, ConnectedNext, EndBlobNext},
    },
    protocol::{ChunkRangesExt, GetRequest},
    ticket::BlobTicket,
    util::connection_pool::ConnectionPool,
    BlobFormat, BlobsProtocol, Hash,
};
mod common;
use common::{get_or_generate_secret_key, setup_logging};
use n0_future::{future::Boxed, stream, BufferedStreamExt, FuturesUnordered, StreamExt};
use range_collections::range_set::RangeSetRange;
use tokio::sync::oneshot;

use crate::stats::ConnectionStats;

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
        cli::Commands::Sync { tickets, target } => {
            sync(tickets, target).await?;
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

async fn sync(tickets: Vec<BlobTicket>, target: Option<PathBuf>) -> Result<()> {
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
    let downloader = Downloader::new(hashes, size, pool);
    let res = downloader
        .run()
        .await
        .context("Failed to download content")?;
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

/// Connection statistics - not used for now!
mod stats {
    #![allow(dead_code)]
    use std::{
        collections::{BTreeMap, HashMap},
        time::Instant,
    };

    use iroh::NodeId;

    #[derive(Debug, Default)]
    struct ConnectionStatsInner {
        stats: HashMap<NodeId, PerNodeStats>,
    }

    #[derive(Debug, Clone, Default)]
    pub struct ConnectionStats {
        data: tokio::sync::watch::Sender<ConnectionStatsInner>,
    }

    #[derive(Debug, Default)]
    struct PerNodeStats {
        log: BTreeMap<Instant, ConnectionEvent>,
    }

    impl ConnectionStatsInner {
        fn insert(&mut self, id: NodeId, at: Instant, event: ConnectionEvent) {
            self.stats.entry(id).or_default().log.insert(at, event);
        }
    }

    impl ConnectionStats {
        fn log(&self, node: NodeId, at: Instant, event: ConnectionEvent) {
            self.data.send_modify(|x| {
                x.insert(node, at, event);
            });
        }

        pub fn start(&self, node: NodeId, at: Instant) -> Recorder {
            self.log(node, at, ConnectionEvent::Start);
            Recorder {
                id: node,
                data: self.data.clone(),
            }
        }
    }

    #[derive(Debug)]
    pub struct Recorder {
        id: NodeId,
        data: tokio::sync::watch::Sender<ConnectionStatsInner>,
    }

    impl Recorder {
        pub fn download(&self, at: Instant, bytes: u64) {
            self.data.send_modify(|x| {
                x.insert(self.id, at, ConnectionEvent::Download(bytes));
            });
        }

        pub fn end(self, at: Instant, success: bool) {
            self.data.send_modify(|x| {
                x.insert(
                    self.id,
                    at,
                    ConnectionEvent::End(if success {
                        EndReason::Success
                    } else {
                        EndReason::Error
                    }),
                );
            });
        }
    }

    impl Drop for Recorder {
        fn drop(&mut self) {
            self.data.send_modify(|x| {
                x.insert(
                    self.id,
                    Instant::now(),
                    ConnectionEvent::End(EndReason::Drop),
                );
            });
        }
    }

    #[derive(Debug)]
    enum ConnectionEvent {
        Start,
        Download(u64),
        End(EndReason),
    }

    #[derive(Debug)]
    enum EndReason {
        Success,
        Error,
        Drop,
    }
}

#[derive(Debug, Default)]
struct Target {
    data: Vec<u8>,
    missing: ChunkRanges,
}

struct Downloader {
    ctx: Arc<Ctx>,
    tasks: FuturesUnordered<Boxed<(NodeId, ChunkRanges, anyhow::Result<()>)>>,
    unclaimed: ChunkRanges,
    hashes: HashMap<NodeId, Hash>,
    current: HashMap<NodeId, oneshot::Sender<()>>,
}

impl Downloader {
    fn new(hashes: HashMap<NodeId, Hash>, size: usize, pool: ConnectionPool) -> Self {
        let target = Target::new(size);
        let unclaimed = target.missing.clone();
        Self {
            ctx: Arc::new(Ctx {
                stats: ConnectionStats::default(),
                target: Mutex::new(target),
                pool,
            }),
            tasks: FuturesUnordered::new(),
            unclaimed,
            hashes,
            current: HashMap::new(),
        }
    }

    async fn run(mut self) -> Result<Vec<u8>> {
        let chunk_size = ChunkNum(1024);
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
                    self.current.remove(&id);
                    if result.is_err() {
                        let target = self.ctx.target.lock().unwrap();
                        self.unclaimed |= ranges & target.missing.clone();
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
        Ok(target.data)
    }

    fn spawn_download(&mut self, id: NodeId, claim: ChunkRanges) {
        self.unclaimed -= claim.clone();
        let (tx, rx) = oneshot::channel();
        self.current.insert(id, tx);
        self.tasks.push(Box::pin(self.ctx.clone().download_range(
            id,
            self.hashes[&id],
            claim,
            rx,
        )));
    }
}

struct Ctx {
    stats: ConnectionStats,
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
        let recorder = self.stats.start(id, Instant::now());
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
                        recorder.download(Instant::now(), len as u64);
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
        recorder.end(Instant::now(), true);
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
        Provide {
            /// path to the file you want to provide.
            ///
            /// This is just a standard iroh-blobs provide, it is only in the
            /// example so the example is self-contained.
            path: PathBuf,
        },
        Sync {
            /// Syncs a blob from multiple sources.
            ///
            /// As opposed to other examples, the blobs must not have the same hash!
            /// They just must have the same size. The resulting file will be
            /// built out of ranges of the source blobs, but will not necessarily
            /// have the same hash as
            tickets: Vec<BlobTicket>,

            /// Path to the file where the synced content will be saved
            #[clap(long)]
            target: Option<PathBuf>,
        },
    }
}
