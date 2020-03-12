///! Grammar Abstraction that defines the syntax of command line arguments.
use super::annotation_parser::parse_annotation;
use dash::util::Result;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::*;

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
    /// Is this parameter splittable, while keeping the other arguments the same?
    pub splittable: bool,
    /// Sometimes short args are attached to the opt, e.g. pr -s,
    pub attached_to_short: bool,
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
    Splittable,   // If this argument is splittable
    Attached,     // If argument is attached to the option name
}

pub enum SizeInfo {
    Num(u64),
    Delimiter(ListSeparator),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ParsingOptions {
    /// Option to configure that long options can be parsed with a single dash.
    pub long_arg_single_dash: bool,
    /// If this command can be split across a specific standard in stream.
    pub splittable_across_input: bool,
    /// If the command reduces the input size
    pub reduces_input: bool,
    /// If the command implicitly relies on the current directory (e.g., many git commands)
    pub needs_current_dir: bool,
}

impl Default for ParsingOptions {
    fn default() -> Self {
        ParsingOptions {
            long_arg_single_dash: false,
            splittable_across_input: false,
            reduces_input: false,
            needs_current_dir: false,
        }
    }
}

pub enum IndividualParseOption {
    /// If the option should be parsed with -long.
    LongArgSingleDash,
    /// If the command is splittable across input
    SplittableAcrossInput,
    /// If the command, as per this invocation, reduces the input size.
    ReducesInput,
    /// Relies on the current dir (implicitly)
    NeedsCurrentDir,
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
#[derive(Debug, Clone)]
pub struct ParsedCommand {
    pub command_name: String,
    pub typed_args: Vec<(String, ArgType)>,
}

impl ParsedCommand {
    pub fn contains(&self, arg: (String, ArgType)) -> bool {
        self.typed_args.iter().any(|v| v == &arg)
    }

    pub fn new(name: &str) -> Self {
        ParsedCommand {
            command_name: name.to_string(),
            typed_args: Vec::new(),
        }
    }

    pub fn add_arg(&mut self, arg: (String, ArgType)) {
        self.typed_args.push(arg)
    }
    pub fn set_args(&mut self, args: Vec<(String, ArgType)>) {
        self.typed_args = args;
    }

    pub fn get(&mut self, ind: usize) -> Option<(String, ArgType)> {
        if ind >= self.typed_args.len() {
            return None;
        }
        Some(self.typed_args.get(ind).unwrap().clone())
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

impl Command {
    pub fn new(ann: &str) -> Result<Self> {
        match parse_annotation(CompleteByteSlice(ann.as_bytes())) {
            Ok(a) => match a.1 {
                Ok(annotation) => Ok(annotation),
                Err(e) => bail!("Failed parsing annotaton: {:?}", e),
            },
            Err(e) => bail!("Failed parsing annotaton: {:?}", e),
        }
    }

    pub fn long_arg_single_dash(&self) -> bool {
        self.parsing_options.long_arg_single_dash
    }

    /// Used for checking if an option passed into the program matches a long option.
    /// For options that are preceeded by a single dash instead of multiple dashes.
    // TODO: this is sort of hacky -> e.g. what is a better way to do this check?
    pub fn check_matches_long_option(&self, word: &str) -> Option<Argument> {
        // first check if this argument starts with a -
        match word.chars().next() {
            Some(ch) => {
                if ch != "-".chars().next().unwrap() {
                    return None;
                }
            }
            None => {
                return None;
            }
        }

        for arg in self.args.iter() {
            match arg {
                Argument::LoneOption(opt) => {
                    if opt.long == "" {
                        continue;
                    }
                    let name = format!("-{}", opt.long);
                    if word.to_owned().starts_with(name.as_str()) {
                        return Some(arg.clone());
                    }
                }
                Argument::OptWithParam(opt, _param) => {
                    if opt.long == "" {
                        continue;
                    }
                    let name = format!("-{}", opt.long);
                    if word.to_owned().starts_with(name.as_str()) {
                        return Some(arg.clone());
                    }
                }
                _ => {}
            }
        }
        return None;
    }
}

pub fn parse_annotation_file(file: &str) -> Result<Vec<Command>> {
    let mut ret: Vec<Command> = Vec::new();
    let file = File::open(file)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line_src = line?;
        let cmd = Command::new(line_src.as_ref())?;
        ret.push(cmd);
    }
    Ok(ret)
}
