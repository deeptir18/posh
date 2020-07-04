# POSH
Posh, the "Process Offload Shell'" is a `smart` shell and runtime that reduces data movement when running shell pipelines on data stored in remote storage, such as NFS.
For more details, check out our [paper](https://deeptir.me/papers/posh-atc20.pdf) which will be published at [Usenix ATC 2020](https://www.usenix.org/conference/atc20).

# dependencies
1. The latest version of [Rust](https://www.rust-lang.org/tools/install); follow
   the instructions at the link provided to install.

# configuring posh
1. Setup client and proxy server access to NFS repo
2. Sample config provided in `config/sample.config`.
3. Run Dash server (compiled to `target/release/server`) and Posh client.

# annotations
- Sample annotations are provided in [`config/eval_annotations.txt`](config/eval_annotations.txt)
- See [annotations.md](annotations.md) for more information on the annotation
  format and adding your own annotations.

# integration tests
- Run `cargo test` to run all integration tests and unit tests, to make sure
  `stdout` forwarding and various other Posh functionality will work when
  running Posh.
