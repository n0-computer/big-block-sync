# Sync from multiple sources with different hash

## Provide side

This is just a vanilla iroh-blobs blobs provide, just so the cli is self-contained.

Make up some random data and share it.
```
> head -c 1000000000 /dev/urandom > test1
> cargo run --release provide test1

Providing content with ticket:
blobac7gr35ay6cpsw2xw4wed4zfzmrtbc5mcpp5nyx2ygallc5kitdxcajinb2hi4dthixs6ylqomys2mjoojswyylzfzxdaltjojxwqltjojxwqltmnfxgwlrpaiaalxzlfgnn4aqbfiaqj7yc6akniaaaaaaaaaaaagn54aqa4xnynd53jhyewkn2ayynga3axgdtxm4ygy2acj4zbz4umdkvnwra
```

Alternatively, share some random data

```
> cargo run --release provide -n 1000000000

Providing content with ticket:
blobadqfuc5jvidteqajzlrxbsgdkdrjocigoowdwss5z5dzpk2qht2gyajinb2hi4dthixs6zlvmmys2mjoojswyylzfzxdaltjojxwqltjojxwqltmnfxgwlrpaiae7sdexkdjwayayculffmgtmbqaiqo4jkthf6j4euyg6bimypexqlexdivrel4myohgv5uwe5urmqj
```

## Sync side

The sync side takes multiple blob tickets for raw blobs. The hashes don't have to be identical, but the size must be.

It will download from all tickets concurrently and build the result out of the pieces.

You can print verbose statistics with `-v` or `-v -v`.

You can also provide a target path, in case you actually want the data.

```
cargo run --release sync -v -v \
  blobac7gr35ay6cpsw2xw4wed4zfzmrtbc5mcpp5nyx2ygallc5kitdxcajinb2hi4dthixs6ylqomys2mjoojswyylzfzxdaltjojxwqltjojxwqltmnfxgwlrpaiaalxzlfgnn4aqbfiaqj7yc6akniaaaaaaaaaaaagn54aqa4xnynd53jhyewkn2ayynga3axgdtxm4ygy2acj4zbz4umdkvnwra
  blobadqfuc5jvidteqajzlrxbsgdkdrjocigoowdwss5z5dzpk2qht2gyajinb2hi4dthixs6zlvmmys2mjoojswyylzfzxdaltjojxwqltjojxwqltmnfxgwlrpaiae7sdexkdjwayayculffmgtmbqaiqo4jkthf6j4euyg6bimypexqlexdivrel4myohgv5uwe5urmqj

Node       Errors	Chunks	Duration	Rate
feca429989	0	963251	9.331   s	100.807  MiB/s
be68efa0c7	1	14336	9.331   s	1.500    MiB/s
Node       Bitfield
feca429989 ████████████████████████████████████████████████████████████████████████████████████████████████████
be68efa0c7 ░                          ░      ░     ░     ░     ░     ░     ░    ░     ░     ░    ░░    ░     ░ 
Downloaded content: 1000000000 bytes, hash b57f9fd9af7dc03fd254a26da58176b374cae00af0a981f98fd71f5d95fc748a
```

To get even more info, run with `RUST_LOG=big_block_sync=trace`.

# Algorithm

<!-- thanks claude -->

This implementation provides a smart parallel downloader that syncs content from multiple sources simultaneously, using dynamic quality-based scheduling and bitfield tracking for optimal performance.
The algorithm begins by connecting to all provided sources and measuring their initial latency to establish baseline quality metrics. Each source is ranked by a composite quality score that prioritizes nodes with fewer errors, higher download rates, and lower latency. Content is divided into fixed size blocks, with the system maintaining separate bitfields to track which chunks are missing from the target, which ranges are currently claimed by active downloads, and which ranges each node has successfully contributed.

The core scheduling logic operates by claiming unclaimed chunks and assigning them to the highest-quality available nodes up to a configurable parallelism limit. As downloads complete, the algorithm updates the per-node statistics including error counts, download rates, and contributed ranges. When no unclaimed chunks remain but downloads are still active, the system enters a *finish mode* where it compares the worst-performing busy node against the best available free node. If the performance gap is significant (typically a 4x rate difference), it cancels the slow download to reassign those chunks to a faster node.

This adaptive approach ensures optimal resource utilization throughout the download process, automatically load-balancing based on real-time performance while providing detailed statistics and visual bitfield representations showing exactly which parts of the content each node contributed to the final result.