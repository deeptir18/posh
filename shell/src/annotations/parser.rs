extern crate clap;
extern crate dash;
extern crate shellwords;
extern crate itertools;

use super::grammar;
use clap::{App, Arg, ArgMatches};
use dash::dag::{node, stream};
use dash::util::Result;
use failure::bail;
use shellwords::split;
use std::collections::HashMap;
use itertools::free::join;

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

    /// Validates an annotation.
    /// Ensures:
    ///     - lone options have short or long specified
    ///     - options with params have short or long specified
    ///     - lone params cannot have multiple values until the last one
    fn validate(&self, annotation: &grammar::Command) -> Result<()> {
        let mut lone_args_with_multiple = false;

        for arg in annotation.args.iter() {
            match arg {
                grammar::Argument::LoneOption(opt) => {
                    if (opt.short == "" && opt.long == "") {
                        bail!("Atleast one of short or long should be specified for option.");
                    }
                }
                grammar::Argument::OptWithParam(opt, param) => {
                    if (opt.short == "" && opt.long == "") {
                        bail!("Atleast one of short or long should be specified for option.");
                    }
                }
                grammar::Argument::LoneParam(param) => {
                    // can only have multiple args if it's the last one
                    match param.size {
                        grammar::ParamSize::Zero => {}
                        grammar::ParamSize::One => {}
                        grammar::ParamSize::SpecificSize(size, sep) => {
                            // update this when we allow more delims
                            if size > 1 && sep != grammar::ListSeparator::Comma {
                                if lone_args_with_multiple {
                                    bail!("Cannot have multiple args with size > 1");
                                }
                                lone_args_with_multiple = true;
                            }
                        }
                        grammar::ParamSize::List(sep) => {
                            if sep != grammar::ListSeparator::Comma {
                                if lone_args_with_multiple {
                                    bail!("Cannot have multiple args with size > 1");
                                }
                                lone_args_with_multiple = true;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn add_annotation(&mut self, annotation: grammar::Command) -> Result<()> {
        match self.validate(&annotation) {
            Ok(_) => {
                self.annotations.push(annotation);
                Ok(())
            }
            Err(e) => {
                bail!("Error validating annotation: {:?}", e);
            }
        }
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
        let mut annotation_map: HashMap<String, usize> = Default::default();
        let mut app = App::new(annotation.command_name.clone())
            .version("1.0")
            .author("doesn't matter"); // local variable
        let argnames: Vec<String> = (0..annotation.args.len() as u32)
            .map(|x| x.to_string())
            .collect();
        for (i, argument) in annotation.args.iter().enumerate() {
            let argname = &argnames[i];
            let mut arg = Arg::with_name(argname);
            annotation_map.insert(argnames[i].to_string(), i);
            match argument {
                grammar::Argument::LoneOption(opt) => {
                    if opt.short != "" {
                        arg = arg.short(&opt.short);
                    }
                    if opt.long != "" {
                        arg = arg.long(&opt.long);
                    }
                    if opt.multiple {
                        arg = arg.multiple(true);
                    }
                    app = app.arg(arg);
                }
                grammar::Argument::OptWithParam(opt, param) => {
                    // TODO: do something with the default value
                    // based on the param_info and the
                    if opt.short != "" {
                        arg = arg.short(&opt.short);
                    }
                    if opt.long != "" {
                        arg = arg.long(&opt.long);
                    }
                    match param.size {
                        grammar::ParamSize::Zero => {
                            unreachable!();
                        }
                        grammar::ParamSize::One => {
                            arg = arg.takes_value(true);
                        }
                        grammar::ParamSize::SpecificSize(amt, separator) => {
                            // default delimiter should be a comma
                            arg = arg.takes_value(true);
                            arg = arg.number_of_values(amt);
                            match separator {
                                // TODO: other separators
                                grammar::ListSeparator::Comma => {
                                    arg = arg.use_delimiter(true);
                                    arg = arg.value_terminator(",");
                                }
                                _ => {}
                            }
                        }
                        grammar::ParamSize::List(separator) => {
                            arg = arg.takes_value(true);
                            arg = arg.multiple(true);
                            match separator {
                                grammar::ListSeparator::Comma => {
                                    arg = arg.use_delimiter(true);
                                    arg = arg.value_terminator(",");
                                }
                                _ => {} // default
                            }
                        }
                    }
                    if opt.multiple || param.multiple {
                        arg = arg.multiple(true);
                    }
                    app = app.arg(arg);
                }
                grammar::Argument::LoneParam(param) => {
                    match param.size {
                        grammar::ParamSize::Zero => {
                            unreachable!();
                        }
                        grammar::ParamSize::One => {
                            arg = arg.takes_value(true);
                        }
                        grammar::ParamSize::SpecificSize(num, separator) => {
                            arg = arg.takes_value(true);
                            arg = arg.number_of_values(num);
                            match separator {
                                grammar::ListSeparator::Comma => {
                                    arg = arg.use_delimiter(true);
                                    arg = arg.value_terminator(",");
                                }
                                _ => {} // default
                            }
                        }
                        grammar::ParamSize::List(separator) => {
                            arg = arg.takes_value(true);
                            arg = arg.multiple(true);
                            match separator {
                                grammar::ListSeparator::Comma => {
                                    arg = arg.use_delimiter(true);
                                    arg = arg.value_terminator(",");
                                }
                                _ => {} // default
                            }
                        }
                    }
                    app = app.arg(arg);
                }
            }
        }

        // split the command and run the actual parsing
        // TODO: this function consumes the invocation, maybe we should clone it to pass it into
        // "assign types"
        let mut matches: ArgMatches = app.get_matches_from_safe(invocation.clone())?;
        self.assign_types(
            invocation.clone(),
            &mut matches,
            &annotation,
            annotation_map,
        )
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
        annotation_map: HashMap<String, usize>,
    ) -> Result<grammar::ParsedCommand> {
        // iterate through each of the args
        // for each arg -- refer to which index the we are in the string by making a list of
        // then ORDER the list by index
        // once ordered by index, iterate via index and keep a counter of the "indices so far"
        // to find the correct place in the string
        // basically, everytime there's a reason why many SEPARATE args are concatenated
        // together (i.e short indexes together
        // then
        // ideally the parser should error out if you provide an option that ISN'T covered
        // iterate over the matches
        // find the STRING in the arg that corresponds to the
        // tar -xcf next
        // -x = 1, -c = 2, -f = 3, next = 4
        // but here -xcf = 1, next = 2 => dif = 2
        let typed_args: Vec<(String, grammar::ArgType)> = Vec::new();
        let index_difference = 0;
        // TODO: matches.args.iter() might not be publicly available
        for arg in matches.args.iter() {
            let match_info = arg.1;
            let arg_info: &grammar::Argument =
                &annotation.args[annotation_map[arg.0.clone()] as usize];
            match arg_info {
                // FOR NOW: assume multiple is not true, handle the case where each arg c
                // an only
                // appear at most once
                grammar::Argument::LoneOption(opt) => {
                    if opt.short != "" {
                        typed_args.append((format!("-{}", &opt.short), grammar::ArgType::Str));
                    } else {
                        typed_args.append((format!("--{}", &opt.long), grammar::ArgType::Str));
                    }
                }
                grammar::Argument::OptWithParam(opt, param) => {
                    if opt.short != "" {
                        typed_args.append((format!("-{}", &opt.short), grammar::ArgType::Str));
                    } else {
                        typed_args.append((format!("--{}", &opt.long), grammar::ArgType::Str));
                    }

                    let values = matches.values_of(arg.0.clone()).unwrap();
                    match param.size {
                        grammar::ParamSize::Zero => {
                            unreachable!();
                        }
                        grammar::ParamSize::One => {
                            typed_args.append((values[0].clone(), opt.param_type));
                        }
                        grammar::ParamSize::SpecificSize(amt, sep) => {
                            // if sep is a space ==> then go get every separate arg
                            // if sep is a list ==> then append the list of args deliminated by a
                            // comma
                            
                            match sep {
                                grammar::ListSeparator::Space {
                                    for val in values.iter() {
                                        typed_args.append(val.clone(), opt.param_type));
                                    }
                                }
                                grammar::ListSeparator::Comma {
                                    typed_args.append((join(values.iter(), ",")), opt.param_type));
                                }
                            }
                        }
                        grammar::ParamSize::List(sep) => {
                            match sep {
                                grammar::ListSeparator::Space {
                                    for val in values.iter() {
                                        typed_args.append(val.clone(), opt.param_type));
                                    }
                                }
                                grammar::ListSeparator::Comma {
                                    typed_args.append((join(values.iter(), ",")), opt.param_type));
                                }
                            }
                        }
                    }


                    // find the -opt or --opt (for each occurence if multiple)
                    // Then find the string(s) containing the following values
                    // Assign the corresponding type to them
                }
                grammar::Argument::LoneParam(param) => {
                    let indices = matches.indices_of(arg.0.clone()).unwrap();
                    let values = matches.values_of(arg.0.clone()).unwrap();
                    // Find the string(s) containing the values (based on the index)
                    // Assign the corresponding type to them
                }
            }
        }

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
