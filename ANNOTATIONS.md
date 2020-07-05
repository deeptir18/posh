# Shell Annotations
Posh determines how to accelerate shell pipelines by using metadata about each
command, _annotations_.
Annotations summarize the command line semantics of each
individual command (`awk`, `tar`, `cat`, `grep`) so, from an arbitrary
invocation, Posh can understand which
files these will access as well as other semantics useful to scheduling the
invocation across the execution engine.

# Annotation Interface
The interface lets users specify...


