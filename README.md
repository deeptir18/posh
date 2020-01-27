# dash
Dash is a `smart` shell and runtime that reduces data movement when running shell pipelines on data stored in remote storage, such as NFS.
It determines parts of pipelines (individual commands) that can be offloaded to proxy servers (that have access to the same data).
Dash does this without requiring changes to _either_ the shell pipeline or the individual binaries for the individual commands (e.g. cat or grep).
The Dash interface assumes that developers provide `annotations` for any programs that can be offloaded to the server.
`annotations` specify a type assignment between the arguments that follow a program, so Dash can figure out which arguments correspond to files that are remote.
The Dash crate includes the server program, client program and backend execution engine necessary to execute programs.
Dash currently supports a shared filesystem interface, where the client and the server see the same view of the filesystem.

### ISSUES:
* Parser
    * Shell parsing: is it possible to support more complicated shell syntax, such as the true subshell syntax where there is a subshell that executes in its own file descriptor? This might not be
      super necessary
    * Annotations: would be nice to specify them via a yaml-like interface, rather than this current list like interface
    * In the parser, would be nice to abstract out the steps better so the code (especially surrounding the clap based parsing) isn't so repetitive/tricky. Probably just rewrite/cleanup this entire
      thing.
    * Make it easier to experiment with schedulers -- and schedulers that depend on knowledge such as link knowledge between the various machines (is this even possible?)
* Execution engine
    * Loop where there are multiple streams inputting to a node is extremely badly implemented currently -> need to make this truly asychronous somehow, but with the child processes running in threads
      as that seems easiest.
* Experiments/Research
    * Find more APPLICATIONS. Maybe ideally one that involves cloud to cloud communication? Where a job could be distributed among machines in the cloud, to show the power of the scheduler? This could
      be some sort of sorting application. Also a comp-bio application could be good.
    * Actually try to write annotations for all of the bash utils.
    * The current way to set off experiments was sort of annoying for some reason. Make it less annoying

    

