# Shell Annotations
Posh determines how to accelerate shell pipelines by using metadata about each
command, _annotations_.
Annotations summarize the command line semantics of each
individual command (`awk`, `tar`, `cat`, `grep`) so, from an arbitrary
pipeline (one or more of these commands with standard shell syntax).

# Contents

[Motivation](https://github.com/deeptir18/posh/blob/master/ANNOTATIONS.md#motivation)

[Interface](https://github.com/deeptir18/posh/blob/master/ANNOTATIONS.md#annotation-interface)

[Examples](https://github.com/deeptir18/posh/blob/master/ANNOTATIONS.md#examples)

[Using the annotations](https://github.com/deeptir18/posh/blob/master/ANNOTATIONS.md#using-the-annotations)

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
   commands like `grep` likely filter their input, so, here, the pipe between
   `cat` and `grep` transmits more data than the pipe between `grep` and `tee`.
3. *Parallelization semantics*: In some cases, Posh could automatically behave
   like `gnuparallel` and try to divide a command into portions tghat execute in
   parallel. This can enable offloading in some cases, if `A.txt, B.txt`, and
   `C.txt` are stored in different places.

## Annotation Interface
Posh allows users to specify 1 or more annotations for a command, and an
annotation contains the following information:
1. A list of arguments to the command, of which there are three types:
    - `Flags` (single options like `-d` or `--debug`)
    - `OptParams` (parameters followed by flags such as `-f foo.txt`)
    - `Params` (parameters not followed by flags)
2. Metadata about each parameter:
    - `long` or `short` option name (e.g., `-d` or `--debug`) (only relevant for
      parameters preceeded by options)
    - `type`: `input_file`, `output_file`, `str`
    - `size`: `1`, `specific_size(x)`, `list` (variable size)
        - If list, specify a `list_separator` for the list (usually a space)
    - If the argument is `splittable`: if the command can be split in a
      data-parallel way across this argument. This is only allowed for up to a
      single argument
3. Metadata about the entire command:
    - `needs_current_dir`: Whether the command implicitly relies on the current
      directory (like `git status` would)
    - `splittable_across_input`: Whether the command is data parallel across its
      standard input, like `grep` is
    - `filters_input`: Whether the command is likely to have a smaller input
      than output
    - `long_args_single_dash`: Most programs use doubledashes before long arguments (`--debug`), but some programs require long arguments be preceded by a singledash. (e.g.`-debug`)

## Examples
The Posh parser understands annotation if the information above is
specified in a single line.
The current syntax for specifying this models the following:
```bash
command_name[needs_current_dir,filters_input]: FLAGS:[(arg0),(arg1)...] OPTPARAMS:[(),()...] PARAMS:[()...]
```
1. First, the command's name is listed at the beginning followed by one or more
   keywords pertaining to metadata about the entire command, which are comma
   separated
    - `grep[filters_input]:`
2. Then, there is a list of `PARAMS`, `OPTPARAMS`, and `FLAGS`
3. Each argument type is followed by a list of the arguments that fall under
   that category, separated by parentheses and comma, like the following:
    - `FLAGS:[(arg0)]`
    - `PARAMS:[(arg1),(arg2)]`
    - `OPTPARAMS:[(arg3),]`
4. The information about each argument specifies the information about that
   argument. From #3, `arg0` might actually look like:
    - `(short:d,long:debug) # for a flag`
    - `(short:f,long:file,size:1,type:input_file) # for an optparam`
    - `(type:input_file,size:list(list_separator( )) # for a param`


Putting this together, here are some examples:
```bash
cat: PARAMS:[(type:input_file,size:list(list_separator:( ))),]
tar: FLAGS:[(short:c),(short:z)] OPTPARAMS:[(short:f,type:output_file,size:1)] PARAMS:[(type:input_file,size:list(list_separator:( )))]
git status[needs_current_dir]: OPTPARAMS:[(short:C,size:1,type:input_file)]
```
See [this file](config/eval_annotations.txt) for more examples.
Note that the annotations need not contain an *exhaustive list* of arguments
that the command can take; Posh will only try to parse a command if it has
information about all the arguments used in the invocation.
For example, if the user typed in `tar -x`, but there was no annotation for
`tar` that specified `-x` as a flag, Posh would not try to parse or accelerate
the command.

## Using the annotations
- All the annotations must be in a single file
- On running the Posh client, specify the annotation file as an argument. See
  the README for more details.


