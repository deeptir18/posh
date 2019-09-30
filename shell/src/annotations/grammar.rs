//! This file defines the formal grammar related to parsing annotations for execv arguments
//! In this BNF format or whatever lol
//! The actual *way* this is parsed doesn't really matter
//! <name> ::= expansion
//! for EBNF -> square brackets around an expansion [ expansion ] indicates that it's optional,
//! e.g.:
//! <term> ::= [ "-" ] <factor>
//! Repetition: curly braces indicate the expression is repeated 0 or more times
//! <args> ::= <arg> {"," <arg>} // i.e. 1 arg and maybe more args
//! Grouping: use () to define the order of an expension
//! <expr> ::= <term> ("+" | "-") <expr>
//! Concatenation: , explicitly denotes concatenation
//! base things:
//!
//!
//! So need to define BASE things I want to group (terminals)
//! And then ways to combine the terminals into more complex expressions
//! The weird syntax that I had before is:
//! [commandname]: OPT:-[name:a|name:b,num:1,delim:" "|name:argname,num:2,delim:"delim",is_file|...]
//! I need to think of a way to define what my GRAMMAR is and how commands are represented
//! Also things like -- taking in stdin? is that allowed?
//! Commands are generally commandname, followed by some options (usually short -, long --)
//! The single letter options could be combined
//! We want to find a way to build a parser for a specific command so we can assign types to their
//! input and output files -- and we can do something interesting with those types
//! there has to be both a mapping of these concepts to how they look so the thing can be parsed
//! and a mapping from the concepts into data structures so they can be used in the shell's
//! execution
//! also eventually need to think about how "params" can refer to files right?
//! Maybe just say a single argument can refer
//! let's assume we have words and letters
//! How do we know about things that can be 1 or more??? I guess {} will take care of that
//! Also -- how do we represent user provided strings?
//!
//!
//! GRAMMAR to define the ANNOTATIONS
//! letter = ...
//! word = letter {letter} // multiple letters (this is just represented by a string)
//! type = "input_file" | "output_file" | "str" | "stdin" | "stdout" // different types to assign to arguments
//! param_delim = " " | "
//! list_separator = " " | "," // to separate a list of args
//! short_opt = letter
//! long_opt = word
//! opt = short_opt,long_opt
//! param_size = "zero" | "one" | ("many", list_separator)
//! arg = type,":",param_size // the type of arguments and the size of the list
//! argument = opt | opt,param_delim,arg | arg
//! command_name = word
//! invocation = command_name {" ",argument} // command name followed by one more more args
//! The corresponding data structures is below:

//! Grammar that this then sort of defines:
//! list_separator = " " | "," // to separate a list of args, TODO: anything?
//! short_opt = -letter
//! long_opt = --word
//! opt = short_opt | long_opt // assumes - and -- are used
//! param = word // we know what type this represents by the type assignment from the annotation
//! params = param, {list_separator, param} // 1 or more parameters
//! argument = opt | opt,params | params // full argument is option along with one or more params
//! command = word
//! invocation = command {" ", argument} // command followed by one or more arguments
// different types to assign to command line arguments
// is it better to just assign long option and short option together...
pub enum ArgType {
    InputFile,
    OutputFile,
    Str,
    Stdin,
    Stdout,
}

// demarcates what's in between the parameter and the list of arguments
pub enum ParamDelim {
    Space,
    Equals,
    NoArgs, // no delim if there are no args after it
}

pub enum ListSeparator {
    Space,
    Comma,
}

pub enum ParamSize {
    Zero,                // nothing following this option
    One,                 // exactly one thing following this option
    List(ListSeparator), // a list of things following this option (separated by separator)
}

pub struct Opt {
    short: String,
    long: String,
    desc: String,
}

pub enum Argument {
    LoneOption(Opt),          // flag
    OptWithParam(Opt, Param), // option with an argument
    LoneParam(Param),         // free argument
}

pub struct Param {
    param_type: ArgType, // what type is this argument
    size: ParamSize,     // doesn't need to be a specific number, list 0, 1 or List
}

// things to think about: how do we define order?
// I.e. for some commands -- certain arguments should be after certain other arguments, but for
// others position doesn't really matter
pub struct Command {
    pub command_name: String,
    pub args: Vec<Argument>,
}
