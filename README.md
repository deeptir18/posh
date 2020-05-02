# dash
Dash is a `smart` shell and runtime that reduces data movement when running shell pipelines on data stored in remote storage, such as NFS.
It determines parts of pipelines (individual commands) that can be offloaded to proxy servers (that have access to the same data).
Dash does this without requiring changes to _either_ the shell pipeline or the individual binaries for the individual commands (e.g. cat or grep).
The Dash interface assumes that developers provide `annotations` for any programs that can be offloaded to the server.
`annotations` specify a type assignment between the arguments that follow a program, so Dash can figure out which arguments correspond to files that are remote.
The Dash crate includes the server program, client program and backend execution engine necessary to execute programs.
Dash currently supports a shared filesystem interface, where the client and the server see the same view of the filesystem.
Our work will be published in Usenix ATC 2020.

# configuring dash
- (_todo_: write more detailed instructions)
1. Setup client and proxy server access to NFS repo
2. Sample config provided in `config/sample.config`.
3. Run Dash server (compiled to `target/release/server`) and Dash client.

# annotations
- Sample annotations are provided in `config/eval_annotations.txt`.

# integration tests
- Run `cargo test` to run all integration tests and unit tests, to make sure
  `stdout` forwarding and various other Dash functionality will work when
  running Dash.
