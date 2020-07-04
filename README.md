# POSH
Posh, the "Process Offload Shell'", is a shell and runtime that automatically reduces data movement when running shell pipelines on data stored in remote storage, such as NFS.
This enables speedups for I/O heavy shell applications that access remote
filesystems.
For more details, check out our [paper](https://deeptir.me/papers/posh-atc20.pdf) which will be published at [Usenix ATC 2020](https://www.usenix.org/conference/atc20).
This implementation is a research prototype -- use at your own risk!

## Demo Video
Coming soon!

## Dependencies
1. The latest version of [Rust](https://www.rust-lang.org/tools/install); follow
   the instructions at the link provided to install.

## Building Posh
0. Our prototype has been fully tested on Linux (versions 18.04 and 19.10).
1. To build the repo, run from the main directory:
```bash
cargo b --release
```
2. Run the following to make sure the integration tests work.
This ensures all `stdout` forwarding and other Posh functionality will work when
running applications.
```bash
cargo test
```

## Building and Configuring Posh
1. Setup client and proxy server access to NFS repo
2. Sample config provided in `config/sample.config`.
3. Run Dash server (compiled to `target/release/server`) and Posh client.

## Annotations
- Sample annotations are provided in [`config/eval_annotations.txt`](config/eval_annotations.txt)
- See [annotations.md](annotations.md) for more information on the annotation
  format and adding your own annotations.

## Integration tests
- Run `cargo test` to run all integration tests and unit tests, to make sure
  `stdout` forwarding and various other Posh functionality will work when
  running Posh.
