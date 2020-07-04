# POSH
Posh, the "Process Offload Shell'", is a shell and runtime that automatically reduces data movement when running shell pipelines on data stored in remote storage, such as NFS.
Posh enables speedups for I/O heavy shell applications that access remote
filesystems by pushing computation to proxy servers closer to the data.
Posh uses [annotations](https://github.com/deeptir18/posh#annotations), metadata
about individual shell commands (`awk`, `cat`, `grep`, etc.) to understand which files an arbitrary shell pipeline will access to schedule and execute the command across proxy servers, so the computation looks like it had been running locally.
For more details, check out our research paper, [POSH: A Data Aware Shell](https://deeptir.me/papers/posh-atc20.pdf) which will be published at [Usenix ATC 2020](https://www.usenix.org/conference/atc20).
This implementation is a research prototype -- use at your own risk!

## Demo Video
Coming soon!

## Dependencies
1. The latest version of Rust. See [this link] (https://www.rust-lang.org/tools/install) for installation details.

## Building Posh
0. Our prototype has been fully tested on Linux (versions 18.04 and 19.10).
1. To build the repo, run from the main directory:
```bash
cargo b --release
```
2. Run the integration tests and unit tests to ensure that Posh can properly redirect `stdin`, `stdout` and `stderr` between processes:
```bash
cargo test
```

## Configuring and Running Posh
- Posh includes a _server binary_ that runs at a proxy server, which must have
access to the same remote filesystem data that the client is trying to access.
- Posh includes two _client binaries_, that intercept shell commands and
schedule and execute them across the proxy servers:
    - The first executes shell scripts (with one or more commands) over Posh
    - The second starts a shell prompt and lets user type in individual
      commands.

### Posh proxy server program
1. Currently, a proxy server must have access to _one remote folder_ on behalf
   of a client. The proxy server could be a remote storage server itself (store this data
   locally) or even access this data over NFS or another remote filesystem
   protocol.
   Run the following at the proxy server:
```bash
$POSH_DIR/target/release/server 
    --folder <client_folder> # folder this Proxy provides access to
    --ip_address <ip_addr> # ip address of the client
    --runtime_port <runtime_port> # port server has open for all Posh
    communication
    --tmpfile <path/to/temporary/directory> # place for Posh to keep temporary
    output while running commands
```

### Posh client program
2. Sample config provided in `config/sample.config`.

### Client configuration file

## Annotations
- Sample annotations are provided in [`config/eval_annotations.txt`](config/eval_annotations.txt)
- See [annotations.md](annotations.md) for more information on the annotation
  format and adding your own annotations.

## End to end deployment example
Here, we'll describe the end to end steps for running a program over Posh, for a
simple pipeline that runs `cat` of files from two different NFS mounts, and
pipes the output to `grep`.
