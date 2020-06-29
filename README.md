# posh
Posh, the ``Process Offload Shell'' is a `smart` shell and runtime that reduces data movement when running shell pipelines on data stored in remote storage, such as NFS. It determines parts of pipelines (individual commands) that can be offloaded to proxy servers (that have access to the same data).
Posh does this without requiring changes to _either_ the shell pipeline or the individual binaries for the individual commands (e.g. cat or grep).
The Posh interface assumes that developers provide `annotations` for any programs that can be offloaded to the server.
`annotations` specify a type assignment between the arguments that follow a program, so Posh can figure out which arguments correspond to files that are remote.
The Posh crate includes the server program, client program and backend execution engine necessary to execute programs.
Posh currently supports a shared filesystem interface, where the client and the server see the same view of the filesystem.
Our work will appear at Usenix ATC 2020.

# configuring posh
- (_todo_: write more detailed instructions)
1. Setup client and proxy server access to NFS repo
2. Sample config provided in `config/sample.config`.
3. Run Dash server (compiled to `target/release/server`) and Posh client.

# annotations
- Sample annotations are provided in `config/eval_annotations.txt`.

# integration tests
- Run `cargo test` to run all integration tests and unit tests, to make sure
  `stdout` forwarding and various other Posh functionality will work when
  running Posh.
