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

## Sync side

