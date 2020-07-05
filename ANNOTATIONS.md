# Shell Annotations
Posh determines how to accelerate shell pipelines by using metadata about each
command, _annotations_.
Annotations summarize the command line semantics of each
individual command (`awk`, `tar`, `cat`, `grep`) so, from an arbitrary
pipeline (one or more of these commands with standard shell syntax).

# Contents:
[Motivation](##Motivation)

[Interface](##Interface)

[Examples](##Examples)

## Motivation
Consider a simple pipeline:
```bash
cat A.txt B.txt C.txt | grep "foo" | tee output.txt
```
When a user types an arbitrary shell pipeline into the Posh prompt, Posh needs
to schedule each command to execute either locally or at a proxy server. To do
this, it needs to know:
1. *Which files each command in the pipeline access*: if the `output.txt` argument
   to `tee` is local, for example, Posh cannot offload `tee`.
2. *Filtering semantics*: Posh should have a notion that some
   commands like `grep likely filter their input, so, here, the pipe between
   `cat` and `grep` transmits more data than the pipe between `grep` and `tee`.
3. *Parallelization semantics*: In some cases, Posh could automatically behave
   like `gnuparallel` and try to divide a command into portions tghat execute in
   parallel. This can enable offloading in some cases, if `A.txt, B.txt, and
   C.txt` are stored in different places.

## Annotation Interface
Posh allows users to specify 1 or more annotations for a command, and an
annotation contains the following information:
1. A list of arguments to the command, of which there are three types:
    - `Flags` (single options like `-d` or `--debug`)
    - `OptParams` (parameters followed by flags such as `-f foo.txt`)
    - `Params` (parameters not followed by flags)

## Examples
foo
bar

