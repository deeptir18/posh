extern crate clap;
extern crate dash;
extern crate shellwords;

use super::grammar;
use clap::{App, Arg, ArgMatches};
use dash::dag::{node, stream};
use dash::util::Result;
use failure::bail;
use shellwords::split;
use std::collections::HashMap;

/// Takes a particular invocation of a command and splits it into shell Words
/// Arguments:
/// * `invocation`: &str - command invocation to be parsed
///
/// Return:
/// * Vector of shell words
/// * Potentially QuotesMismatch error
pub fn split_invocation(invocation: &str) -> Result<Vec<String>> {
    match split(invocation) {
        Ok(v) => Ok(v),
        Err(e) => bail!("{:?}", e),
    }
}

/// A parser represents a list of annotations associated with a certain command.
/// These annotations are a "whitelist".
/// Dash will only assign types if the invocation fits within one of the annotations.
pub struct Parser {
    name: String,
    annotations: Vec<grammar::Command>,
}

impl Parser {
    pub fn new(name: &str) -> Self {
        Parser {
            name: name.to_string(),
            annotations: vec![],
        }
    }

    /// Validates the annotation to ensure that it implies valid parsing options.
    /// Rules:
    ///     - options (with or without values)  must have either -short or --long as non empty string
    ///     - for lone parameters -- only the last one (by order in the command vector) can have
    ///     size > 1 UNLESS (list or specific size that is more than 1):
    ///         - the delimiter is something other than a space
    fn validate_annotation(&self, annotation: &grammar::Command) -> Result<()> {
        Ok(())
    }

    pub fn add_annotation(&mut self, annotation: grammar::Command) {
        self.annotations.push(annotation);
    }

    /// Parses specific invocation of a command
    /// Builds a clap parser based on the grammar,
    /// and then executes the parser.
    ///
    /// Arguments:
    /// * `invocation`: Vec<String> - command and argument list to be parsed
    /// * `ind`: usize - which annotation in whitelist to try
    ///
    /// Returns:
    /// * node::Program that represents the execution of this command, with type assignments
    fn parse_invocation(
        &mut self,
        invocation: &Vec<String>,
        ind: usize,
    ) -> Result<grammar::ParsedCommand> {
        let annotation = &self.annotations[ind];
        let mut app = App::new(annotation.command_name.clone())
            .version("1.0")
            .author("doesn't matter"); // local variable

        let mut counter: u32 = annotation.args.len() as u32; // index args
        for i in 0..(counter as usize) {
            match &annotation.args[i] {
                grammar::Argument::LoneOption(opt) => {
                    let mut arg = Arg::with_name(i.to_string());
                    app = app.arg(
                        Arg::with_name(argname[i].as_str())
                            .short(&opt.short)
                            .long(&opt.long),
                    );
                    if opt.multiple {}
                }
                grammar::Argument::OptWithParam(opt, param_info) => {
                    // TODO: do something with the param_delim,
                    // TODO: need to do something with the default val
                    let mut optarg = Arg::with_name(argname[i].as_str())
                        .short(&opt.short)
                        .long(&opt.long);
                    // based on the param_info and the
                    match param_info.size {
                        grammar::ParamSize::Zero => {
                            unreachable!();
                        }
                        grammar::ParamSize::One => {
                            optarg = optarg.takes_value(true);
                        }
                        grammar::ParamSize::SpecificSize(amt, separator) => {
                            // default delimiter should be a comma
                            optarg = optarg.takes_value(true);
                            optarg = optarg.number_of_values(amt);
                            optarg = optarg.value_terminator(" ");
                        }
                        grammar::ParamSize::List(separator) => {
                            optarg = optarg.takes_value(true);
                            match separator {
                                grammar::ListSeparator::Space => {
                                    optarg = optarg.value_terminator(" ");
                                }
                                grammar::ListSeparator::Comma => {} // default
                            }
                        }
                    }
                    app = app.arg(optarg);
                }
                grammar::Argument::LoneParam(param) => {
                    // TODO: figure out the one to use here
                    let mut optarg = Arg::with_name(argname[i].as_str());
                    match param.size {
                        grammar::ParamSize::Zero => {
                            unreachable!();
                        }
                        grammar::ParamSize::One => {
                            optarg = optarg.takes_value(true);
                        }
                        grammar::ParamSize::SpecificSize(num, separator) => {
                            optarg = optarg.takes_value(true);
                            optarg = optarg.number_of_values(num);
                        }
                        grammar::ParamSize::List(separator) => {
                            optarg = optarg.takes_value(true);
                            match separator {
                                grammar::ListSeparator::Space => {
                                    optarg = optarg.value_terminator(" ");
                                }
                                grammar::ListSeparator::Comma => {} // default
                            }
                        }
                    }
                    app = app.arg(optarg);
                }
            }
            counter += 1;
        }

        // split the command and run the actual parsing
        // TODO: this function consumes the invocation, maybe we should clone it to pass it into
        // "assign types"
        let mut matches: ArgMatches = app.get_matches_from(invocation.clone());
        self.assign_types(invocation.clone(), &mut matches, &annotation)
    }

    /// Given clap's argmatches of a command invocation,
    /// assign types based on the type map in the annotation.
    /// This does NOT decide execution location, just parses and assigns "types".
    /// * invocation - Vec<String> - the particular invocation we are parsing.
    /// * matches - ArgMatches - the result of running clap over the original invocation
    /// * annotation - &grammar::Command - the annotation the parser matches were generated with
    ///
    fn assign_types(
        &self,
        invocation: Vec<String>,
        matches: &mut ArgMatches,
        annotation: &grammar::Command,
    ) -> Result<grammar::ParsedCommand> {
        // ideally the parser should error out if you provide an option that ISN'T covered
        let typed_args: Vec<(String, grammar::ArgType)> = Vec::new();
        for i in 0..(annotation.args.len()) {
            let argname: String = i.to_string();
            //match &annotation.args[i] {
            // argname in clap is the counter
            //}
        }
        // if the arg is present, then assign all the strings corresponding to that arg with
        // that type
        // if it's a flag -- scan the vec for -short or --long. But if it's the short
        // option -- then it could be concatenated with larger things (and you assign
        // string to the entire thing)

        // if it's an arg with a param -- then scan the string for -short, --long, and the
        // words clap finds for it
        //

        // if it's a lone param -- just scan the string for these words

        Ok(grammar::ParsedCommand {
            command_name: self.name.clone(),
            typed_args: typed_args,
        })
    }

    /// Tries to parse a command with each of the parsers in the whitelist.
    /// Returns the first Program that matches a parser.
    /// If no parser matches this invocation, returns a parsed command where all arguments are of
    /// type "str" (the default).
    /// TODO: finish this.
    /// TODO: this command also needs information about the filesystem to be able to construct the
    /// program.
    pub fn parse_command(&mut self, invocation: Vec<String>) -> Result<grammar::ParsedCommand> {
        // go through all of the annotations for this command
        // try to assign types to an invocation of the command
        // If it doesn't work for any, just construct a program where everything is Strings (i.e.
        // the default type) and don't assign anything special to anything
        // And return that for execution
        for i in (0..self.annotations.len()) {
            match self.parse_invocation(&invocation, i) {
                Ok(p) => {
                    return Ok(p);
                }
                Err(e) => {
                    println!(
                        "Warning: invocation {:?} does not match annotation {:?} with error: {:?}",
                        invocation, i, e
                    );
                }
            }
        }
        println!(
            "Warning: invocation {:?} was not parsed by any parsers",
            invocation
        );
        // the command was not parsed
        let mut typed_args: Vec<(String, grammar::ArgType)> = Vec::new();
        for arg in invocation {
            typed_args.push((arg, grammar::ArgType::Str));
        }

        Ok(grammar::ParsedCommand {
            command_name: self.name.clone(),
            typed_args: typed_args,
        })
    }
}
