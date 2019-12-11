//! GRAMMAR to define the ANNOTATIONS
//! This is still sort of a weird syntax -- but necessary to assign types to arguments
//! letter = ...
//! word = letter {letter} // multiple letters (this is just represented by a string)
//! type = "input_file" | "output_file" | "str" | "stdin" | "stdout" // different types to assign to arguments
//! param_delim = " " | "
//! list_separator = " " | "," // to separate a list of args
//! short_opt = letter
//! long_opt = word
//! opt = short=[short_opt],long=[long_opt],[desc=description],[occurrences=single|multiple]
//! param_size = "zero" | "one" | ("specific_size", size) | ("many", list_separator)
//! arg = type,":",param_size // the type of arguments and the size of the list
//! argument = opt | opt,param_delim,arg | arg
//! command_name = word
//! invocation = command_name {" ",argument} // command name followed by one more more args
//! The corresponding data structures is below:

//! Grammar that this then sort of defines:
//! list_separator = " " | "," // to separate a list of args, TODO: anything?
//! short_opt = -letter
//! long_opt = --word
//! param_delim = "=" | " " // equals for long opt, spaces for short
//! opt = short_opt | long_opt // assumes - and -- are used
//! param = word // we know what type this represents by the type assignment from the annotation
//! params = param, {list_separator, param} // 1 or more parameters
//! argument = opt | opt,param_delim,params | params // full argument example
//! command = word
//! invocation = command {" ", argument} // command followed by one or more arguments
// different types to assign to command line arguments
// is it better to just assign long option and short option together...

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ArgType {
    InputFile,
    OutputFile,
    Str,
    InputFileList,
    OutputFileList,
}

impl Default for ArgType {
    fn default() -> Self {
        ArgType::Str
    }
}

// demarcates what' s in between the parameter and the list of arguments

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ParamDelim {
    Space,
    Equals,
    NoArgs, // no delim if there are no args after it
}

impl Default for ParamDelim {
    fn default() -> Self {
        ParamDelim::Space
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ListSeparator {
    Space,
    Comma,
}

impl Default for ListSeparator {
    fn default() -> Self {
        ListSeparator::Comma
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ParamSize {
    Zero,                             // nothing following this option
    One,                              // exactly one thing following this option
    SpecificSize(u64, ListSeparator), // a specific size
    List(ListSeparator), // a list of things following this option (separated by separator)
}

impl Default for ParamSize {
    fn default() -> Self {
        ParamSize::One
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Opt {
    pub short: String,
    pub long: String,
    pub desc: String,
    pub multiple: bool,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Argument {
    LoneOption(Opt),          // flag
    OptWithParam(Opt, Param), // option with an argument
    LoneParam(Param),         // free argument
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Param {
    pub param_type: ArgType, // what type is this argument
    pub size: ParamSize,     // doesn't need to be a specific number, list 0, 1 or List
    pub default_value: String,
    pub multiple: bool,
}

/// All the possible things provided in the annotation.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Info {
    ParamType(ArgType),
    Size(ParamSize),
    DefaultValue(String),
    Delim(ParamDelim),
    Short(String),
    Long(String),
    Desc(String), // I really should remove this one it's just clutter
    Multiple,     // allow multiple occurrences or not
}

pub enum SizeInfo {
    Num(u64),
    Delimiter(ListSeparator),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ParsingOptions {
    /// Option to configure that long options can be parsed with a single dash.
    pub long_arg_single_dash: bool,
}

impl Default for ParsingOptions {
    fn default() -> Self {
        ParsingOptions {
            long_arg_single_dash: true,
        }
    }
}

pub enum IndividualParseOption {
    LongArgSingleDash,
}

/// An annotation is a command name and a vector of args
#[derive(Debug, PartialEq, Eq)]
pub struct Command {
    /// Name of command to be parsed.
    pub command_name: String,
    /// Summary of arguments passed and their types.
    pub args: Vec<Argument>,
    /// Separate parsing options that should be passed into the parser.
    pub parsing_options: ParsingOptions,
}

/// A ParsedCommand is a command name, with *specific* String arguments
/// Each string argument is associated with a specific ArgType
#[derive(Debug)]
pub struct ParsedCommand {
    pub command_name: String,
    pub typed_args: Vec<(String, ArgType)>,
}

impl ParsedCommand {
    pub fn contains(&self, arg: (String, ArgType)) -> bool {
        self.typed_args.iter().any(|v| v == &arg)
    }
}

impl Clone for Command {
    fn clone(&self) -> Self {
        Command {
            command_name: self.command_name.clone(),
            args: self.args.iter().map(|x| x.clone()).collect(),
            parsing_options: self.parsing_options.clone(),
        }
    }
}

// TODO: need to figure out a way to parse this grammar to figure out the information about the
// annotations
// Maybe for now just use nom because you understand that better?
/* Example commands:
 * 1. cat
 * command_name: cat
 * args: vec![Argument::LoneParam(Param{type: input_file, size:
 * ParamSize::List(ListSeparator::Space)})]
 *
 * 2. tar
 * command_name: tar
 * args: vec![Argument::LoneOption(Opt{short: "-x", long: "", desc: ""}),
 *            Argument::LoneOption(Opt{short: "-v", long: "", desc: ""}),
 *            Argument::LoneOption(Opt{short: "-c", long: "", desc: ""}),
 *            Argument::LoneOption(Opt{short: "-z", long: "", desc: ""}),
 *            Argument::OptWithParam(Opt{short: "-f", long: "", desc: ""},
 *                                   Param{param_type: input_file, size: ParamSize::One}),
 *            Argument::OptWithParam(Opt{short: "-C", long: "", desc: ""},
 *                                   Param{param_type: input_file, size: ParamSize::One})]
 */
