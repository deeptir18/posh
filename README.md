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
1. The latest version of Rust. See [this link](https://www.rust-lang.org/tools/install) for installation details.

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

### General setup.
- The client and server must communicate over a custom port, which can be
configured in both the server and client binaries; the default is 1235. The
server must keep this port open for TCP traffic.
- The client and server binaries require a directory to store temporary output
  while processes are running.

### Posh proxy server program
1. A proxy server must have access to _one remote folder_ on behalf
   of a client. The proxy server could be a remote storage server itself (store this data
   locally) or even access this data over NFS or another remote filesystem
   protocol.
   Run the following at the proxy server:
```bash
$POSH_SRC/target/release/server 
    --folder <client_folder> # folder this Proxy provides access to, required
    --ip_address <ip_addr> # ip address of the client, required
    --runtime_port <runtime_port> # port server has open for all Posh communication, default = 1235
    --tmpfile <path/to/temporary/directory> # place for Posh to keep temporary output while running commands, required
```

### Posh client program
2. The Posh client shell requires an [_annotations
   file_](https://github.com/deeptir18/posh#annotations) and a [_configuration file_](https://github.com/deeptir18/posh#client-configuration-file) to understand and schedule shell commands.
   Each section, linked contains further information about the information these
   files much contain.
- To run the shell script binary, run:
```bash
$POSH_SRC/target/release/shell-exec
    <binary> # shell script to run over Posh, required
    --annotations_file <path> # path to annotations, required
    --mount_file <path> # path to config file, required
    --pwd <directory> # directory to execute this script from, required
    --tmpfile <path/to/temporary/directory> # place for Posh to keep temporary output while running commands, required
    --runtime_port <runtime_port> # port to communicate with server with, default = 1235
    --splitting_factor <splitting factor> # parallelization factor, default = 1
    --tracing_level <tracing_level> # log debug outpu†, default = none
```
- To run the shell prompt binary, run:
```bash
$POSH_SRC/target/release/shell-client
    --annotations_file <path> # path to annotations, required
    --mount_file <path> # path to config file, required
    --tmpfile <path/to/temporary/directory> # place for Posh to keep temporary output while running commands, required
    --runtime_port <runtime_port> # port to communicate with server with, default = 1235
    --splitting_factor <splitting factor> # parallelization factor, default = 1
    --tracing_level <tracing_level> # log debug outpu†, default = none
```
- Syntax allowed:
    - Posh can accelerate commands with standard shell syntax, including pipes
      (`|`), and `stdin`, `stdout` and `stderr` redirections (`<`, `>`, `2>`)
    - Posh allows export commands (e.g. `export VAR=VALUE`) to configure
      environment variables within scripts
    - We are working on including more standard syntax.

### Client configuration file
- A sample config file is provided in [`config/sample.config`](config/sample.config). To use Posh, edit the lines under `mounts` with your configuration information.
- The config file has up to 3 parts. # 1 is required, while 2 and 3 are
  only necessary for experimental features.
    1. **[Required]** A list of `mounts`, e.g. a list of IPs for proxy servers mapped to the
       corresponding client remote mounted directory, which must be an absolute
       path, for example:
          ```yaml
            mounts:
                "255.255.255.0": "/home/user/remote_mount1"
                "255.255.255.1": "/home/user/remote_mount2"
          ```
    2. [Optional] A list of rough link speeds between different proxies, where the `client` is included as a local proxy. This is used for an experimental scheduling algorithm. For example:
          ```yaml
            links:
                "(255.255.255.0,client)": 500 # in Mbps
                "(255.255.255.1,client)": 500 # in Mbps
            ```
    3. [Optional] A list of temporary file locations on each proxy server
       that Posh can write to.
        ```yaml
        tmp_directory:
                "255.255.255.1": "/tmp/posh"
        ```

## Annotations
- Sample annotations are provided in [`config/eval_annotations.txt`](config/eval_annotations.txt)
- See [ANNOTATIONS.md](ANNOTATIONS.md) for more information on the annotation
  format and adding your own annotations.

## Example usage
Here, we'll describe the end to end steps for running a program over Posh, for a
simple pipeline that runs `cat` of files from two different NFS mounts, and
pipes the output to `grep`. The proxy servers will run on each NFS mount directly.

0. Configure NFS at the client and servers so the two servers expose NFS mounts to the client.
See [EXPERIMENTS.md](EXPERIMENTS.md) for more details on the NFS setup in our experiments.
In this example, each NFS servers hosts the shared directory at `/mnt/logs` and the client mounts them at 
`/home/user/mount1` and `/home/user/mount2` respectively.

1. The data we'll be using is network access logs from the SEC's Edgar log dataset.
    Download two sample logs, one to each mount, from the edgar log website:
    ```bash 
    # (at the first NFS server)
    wget http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2017/Qtr1/log20170314.zip 
    unzip log20170314.zip
    mv log20170314.csv /mnt/logs/log.csv
    
    # (at the second NFS server)
    wget http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2017/Qtr1/log20170315.zip 
    unzip log20170315.zip
    mv log20170315.csv /mnt/logs/log.csv
    ```

2. At each of the proxy servers, run the following command (substituting the client IP address):
```bash
$POSH_SRC/target/release/server  --folder /mnt/logs --ip_address $CLIENT_IP --tmpfile /tmp/posh
```
3. Configure the client configuration file as described in [the above section](https://github.com/deeptir18/posh#client-configuration-file) to look like the following:
```yaml
    mounts:
        "FIRST_SERVER_IP": "/home/user/mount1"
        "SECOND_SERVER_IP": "/home/user/mount2"
```

3. Run the following at the client:
```bash
cd /home/user
$POSH_SRC/target/release/shell-client --annotations_file $POSH_SRC/config/eval_annotations.txt --mount_file $POSH_SRC/config/sample.config
```

4. At the resulting prompt, type in:
```bash
cat mount1/log.csv mount2/log.csv | grep "127.0.0.1"
```
The result will show up faster than using `bash`, as Posh offloads a `cat | grep` command to run at each proxy server and just aggregates the output in the correct order.
