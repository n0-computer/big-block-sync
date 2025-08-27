use std::{collections::HashMap, time::Duration};

use iroh::{NodeAddr, NodeId, Watcher, protocol::Router};
use iroh_blobs::Hash;
use n0_future::{BufferedStreamExt, StreamExt, stream};
use testresult::TestResult;

use crate::{Config, PerNodeStats, sync, tests::test_provider::TestProvider};

const SETUP_PARALLELISM: usize = 32;

/// Create `n` providers for a blob of size `size`
///
/// The data will be an uniform array that is never 0.
async fn create_providers(
    n: usize,
    size: usize,
    delay: Vec<Option<Duration>>,
) -> TestResult<(Vec<iroh::protocol::Router>, Vec<(NodeAddr, Hash)>)> {
    let index_and_delay = (0..n)
        .map(|i| (i, delay.get(i).and_then(|x| x.as_ref()).cloned()))
        .collect::<Vec<_>>();
    let res = stream::iter(index_and_delay)
        .map(|(i, delay)| async move {
            let endpoint = iroh::endpoint::Endpoint::builder().bind().await?;
            endpoint.home_relay().initialized().await;
            let addr = endpoint.node_addr().initialized().await;
            let fill = (i % 254) as u8 + 1;
            let data = vec![fill; size];
            let blobs = TestProvider::new(data.into(), delay);
            let hash = blobs.hash();
            let router = Router::builder(endpoint.clone())
                .accept(iroh_blobs::ALPN, blobs.clone())
                .spawn();
            TestResult::Ok((router, (addr, hash)))
        })
        .buffered_unordered(SETUP_PARALLELISM)
        .collect::<Vec<_>>()
        .await;
    let mut routers = Vec::new();
    let mut contents = Vec::new();
    for item in res {
        let (router, content) = item?;
        routers.push(router);
        contents.push(content);
    }
    Ok((routers, contents))
}

async fn shutdown_routers(routers: Vec<Router>) {
    stream::iter(routers)
        .for_each_concurrent(SETUP_PARALLELISM, |r| async move {
            r.shutdown().await.ok();
        })
        .await;
}

fn used_providers(stats: &HashMap<NodeId, PerNodeStats>) -> usize {
    stats
        .iter()
        .filter(|(_, stats)| !stats.ranges.is_empty())
        .count()
}

/// Just performs a download from 8 possible sources and checks that
/// 1. The download is successful
/// 2. The downloaded data is complete
/// 3. config.parallelism sources are used
#[tokio::test]
async fn smoke() -> TestResult<()> {
    let size = 1024 * 1024;
    let (routers, blobs) = create_providers(8, size, Vec::new()).await?;
    let config = Config {
        parallelism: 4,
        block_size: 64, // 64 KiB
        min_rate: None,
        rate_ratio: None,
        latency: Default::default(),
    };
    let (res, stats) = sync(blobs, config, 0).await?;
    assert_eq!(res.len(), size);
    assert!(res.iter().all(|b| *b != 0));
    // check that 4 providers are actually used for downloads.
    assert_eq!(used_providers(&stats), 4);
    shutdown_routers(routers).await;
    Ok(())
}

/// Just performs a download from 8 possible sources with an unrealistically high min_rate and checks that
/// 1. The download is successful
/// 2. The downloaded data is complete
/// 3. all sources are tried
#[tokio::test]
async fn slow() -> TestResult<()> {
    let size = 1024 * 1024;
    let (routers, blobs) = create_providers(8, size, Vec::new()).await?;
    let config = Config {
        parallelism: 4,
        block_size: 64,                            // 64 KiB
        min_rate: Some(1024 * 1024 * 1024 * 1024), // absurdly high
        rate_ratio: None,
        latency: Default::default(),
    };
    let (res, stats) = sync(blobs, config, 0).await?;
    assert_eq!(res.len(), size);
    assert!(res.iter().all(|b| *b != 0));
    // it should use all providers in a futile attempt to find one that is as fast as min_rate
    assert_eq!(used_providers(&stats), 8);
    shutdown_routers(routers).await;
    Ok(())
}

/// Checks that the downloader will try new sources if one of the closest nodes in terms of latency is slow.
/// 1. The download is successful
/// 2. The downloaded data is complete
/// 3. all sources are tried
#[tokio::test]
async fn slow_but_close() -> TestResult<()> {
    let size = 1024 * 1024 * 256;

    // first three providers are slow, 50 ms for each 16 KiB chunk
    let delay = vec![
        Some(Duration::from_millis(50)),
        Some(Duration::from_millis(50)),
        Some(Duration::from_millis(50)),
    ];
    let (routers, blobs) = create_providers(8, size, delay).await?;
    let ids = blobs
        .iter()
        .map(|(addr, _)| addr.node_id)
        .collect::<Vec<_>>();
    // first 4 providers are close
    let latency = blobs
        .iter()
        .enumerate()
        .map(|(i, (addr, _))| {
            (
                addr.node_id,
                if i < 4 {
                    Duration::from_millis(1)
                } else {
                    Duration::from_millis(100)
                },
            )
        })
        .collect::<HashMap<_, _>>();
    let config = Config {
        parallelism: 4,
        block_size: 64, // 64 KiB
        min_rate: None,
        rate_ratio: Some(10),
        latency: latency.clone(),
    };
    let (res, stats) = sync(blobs, config, 0).await?;
    assert_eq!(res.len(), size);
    assert!(res.iter().all(|b| *b != 0));
    // it should use all providers in a futile attempt to find one that is as fast as min_rate
    for id in ids {
        println!(
            "{}: {:<8.3}",
            id.fmt_short(),
            stats[&id].rate().unwrap_or(0.0)
        );
    }
    assert_eq!(used_providers(&stats), 7);
    shutdown_routers(routers).await;
    Ok(())
}

/// A test provider that provides a single blob with an optional per chunk group delay to simulate slow providers.
mod test_provider {
    use std::{io, sync::Arc, time::Duration};

    use anyhow::{bail, ensure};
    use bao_tree::io::{
        mixed::{EncodedItem, traverse_ranges_validated},
        outboard::PreOrderMemOutboard,
    };
    use bytes::Bytes;
    use iroh::{endpoint::SendStream, protocol::ProtocolHandler};
    use iroh_blobs::{Hash, protocol::Request, store::IROH_BLOCK_SIZE};
    use tokio::io::AsyncWriteExt;

    #[derive(Debug, Clone)]
    pub struct TestProvider(Arc<TestProviderInner>);

    impl TestProvider {
        pub fn new(data: Bytes, delay: Option<Duration>) -> Self {
            let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
            Self(Arc::new(TestProviderInner {
                data,
                outboard,
                delay,
            }))
        }

        pub fn hash(&self) -> Hash {
            Hash::from(self.0.outboard.root)
        }
    }

    #[derive(Debug)]
    struct TestProviderInner {
        data: Bytes,
        outboard: PreOrderMemOutboard<Vec<u8>>,
        delay: Option<Duration>,
    }

    struct Sender {
        send: SendStream,
        delay: Option<Duration>,
    }

    impl bao_tree::io::mixed::Sender for Sender {
        type Error = io::Error;

        async fn send(&mut self, item: EncodedItem) -> std::result::Result<(), Self::Error> {
            match item {
                EncodedItem::Size(size) => {
                    self.send.write_u64_le(size).await?;
                }
                EncodedItem::Parent(parent) => {
                    let mut data = vec![0u8; 64];
                    data[..32].copy_from_slice(parent.pair.0.as_bytes());
                    data[32..].copy_from_slice(parent.pair.1.as_bytes());
                    self.send.write_all(&data).await.map_err(io::Error::from)?;
                }
                EncodedItem::Leaf(leaf) => {
                    self.send
                        .write_chunk(leaf.data)
                        .await
                        .map_err(io::Error::from)?;
                    if let Some(d) = self.delay {
                        tokio::time::sleep(d).await;
                    }
                }
                EncodedItem::Done => {}
                EncodedItem::Error(cause) => return Err(cause.into()),
            }
            Ok(())
        }
    }

    impl ProtocolHandler for TestProvider {
        async fn accept(
            &self,
            connection: iroh::endpoint::Connection,
        ) -> Result<(), iroh::protocol::AcceptError> {
            loop {
                let (send, mut recv) = connection.accept_bi().await?;
                let this = self.clone();
                tokio::spawn(async move {
                    let request = recv.read_to_end(1024).await?;
                    let request: Request = postcard::from_bytes(&request)?;
                    let Request::Get(get) = request else {
                        bail!("only Get requests supported");
                    };
                    let Some((offset, ranges)) = get.ranges.as_single() else {
                        bail!("only single ranges supported");
                    };
                    ensure!(offset == 0, "HashSeq not supported");
                    if get.hash != Hash::from(this.0.outboard.root) {
                        bail!("unknown hash");
                    }
                    let mut sender = Sender {
                        send,
                        delay: this.0.delay,
                    };
                    traverse_ranges_validated(&this.0.data, &this.0.outboard, ranges, &mut sender)
                        .await?;

                    anyhow::Ok(())
                });
            }
        }
    }
}
