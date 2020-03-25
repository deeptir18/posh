extern crate clap;
extern crate dash;
extern crate itertools;
extern crate shellwords;

use super::argument_matcher::ArgMatch;
use super::grammar;
use clap::{App, Arg};
use dash::util::Result;
use failure::bail;
use std::collections::HashMap;
use std::fmt;
use tracing::debug;

/// A parser represents a list of annotations associated with a certain command.
/// These annotations are a "whitelist".
/// Dash will only assign types if the invocation fits within one of the annotations.
/// TODO: how do we properly deal with subcommands? Ideally those are also specified in the
/// annotation with their own syntax.
pub struct CmdParser {
    /// Command this is parsing.
    name: String,
    /// List of annotations for this command.
    annotations: Vec<grammar::Command>,
    /// Used for debug printing.
    debug: bool,
    /// Temporarily here. Max splitting factor for parallelization
    splitting_factor: u32,
}

impl fmt::Debug for CmdParser {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(&format!("Name {:?}", self.name))?;
        for ann in self.annotations.iter() {
            f.pad(&format!("\n{:?}", ann))?;
        }
        f.pad("")
    }
}

impl CmdParser {
    pub fn new(name: &str) -> Self {
        CmdParser {
            name: name.to_string(),
            annotations: vec![],
            debug: false,
            splitting_factor: 1,
        }
    }

    pub fn set_splitting_factor(&mut self, factor: u32) {
        self.splitting_factor = factor;
    }

    /// Validates an annotation.
    /// Ensures:
    ///     - lone options have short or long specified
    ///     - options with params have short or long specified
    ///     - lone params cannot have multiple values until the last one
    ///     - should be at most 1 SPLITTABLE ARG (no more)
    ///     TODO: should the error be a specific error type?
    fn validate(&self, annotation: &grammar::Command) -> Result<()> {
        if annotation.command_name != self.name {
            bail!("Annotation does not refer to the same command as the parser");
        }
        let mut lone_args_with_multiple = false;
        let mut found_splittable = false;
        for arg in annotation.args.iter() {
            match arg {
                grammar::Argument::LoneOption(opt) => {
                    if opt.short == "" && opt.long == "" {
                        bail!("Atleast one of short or long should be specified for option.");
                    }
                }
                grammar::Argument::OptWithParam(opt, param) => {
                    if opt.short == "" && opt.long == "" {
                        bail!("Atleast one of short or long should be specified for option.");
                    }
                    if param.splittable {
                        found_splittable = true;
                    }
                }
                grammar::Argument::LoneParam(param) => {
                    // can only have multiple args if it's the last one
                    if param.splittable {
                        if found_splittable {
                            bail!("Cannot have more than 1 arg with splittable turned on");
                        }
                        found_splittable = true;
                    }
                    match param.size {
                        grammar::ParamSize::Zero => {
                            bail!("Cannot have param with size 0");
                        }
                        grammar::ParamSize::One => {
                            if param.splittable {
                                bail!("Cannot have splittable command with size 1");
                            }
                        }
                        grammar::ParamSize::SpecificSize(size, sep) => {
                            if size == 1 && param.splittable {
                                bail!("Cannot have splittable command with size 1");
                            }
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
    /// * Result<ArgMatch>
    ///     - allows the interpreter to later reconstruct the arguments back together
    fn parse_invocation(&self, invocation: &Vec<String>, ind: usize) -> Result<ArgMatch> {
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
                    if param.default_value != "".to_string() {
                        arg = arg.default_value(&param.default_value);
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

        let mut invocation_clone = invocation.clone();
        // if the command name has more than 1 word --> need to remove words from the arglist
        let parser_name_list: Vec<String> = self
            .name
            .clone()
            .split(" ")
            .map(|x| x.to_string())
            .collect();
        let num_args_to_pop = parser_name_list.len() - 1;
        if num_args_to_pop > 0 {
            if invocation_clone.len() < num_args_to_pop {
                bail!(
                    "Not enough arguments in invocation {:?} to fill the command name {:?}",
                    invocation_clone,
                    self.name
                );
            }
            for i in 0..num_args_to_pop {
                let popped = invocation_clone.remove(0);
                if popped != parser_name_list[i + 1] {
                    bail!(
                        "Invocation {:?} does not match full command name {:?}",
                        invocation,
                        self.name
                    );
                }
            }
        }

        // now, if lone_args_single_dash turned on, deal with this
        // Note that ALL long args will be turned back into -dashes at the end of the parsing.
        if annotation.long_arg_single_dash() {
            for word in invocation_clone.iter_mut() {
                match annotation.check_matches_long_option(&word) {
                    Some(_arg) => {
                        word.insert_str(0, "-");
                    }
                    None => {}
                }
            }
        }

        invocation_clone.insert(0, self.name.clone());
        let matches: clap::ArgMatches = match app.get_matches_from_safe(invocation_clone) {
            Ok(m) => m,
            Err(e) => bail!("Could not get matches: {:?}", e),
        };

        // construct and return an argmatch object
        ArgMatch::new(parser_name_list, matches, &annotation, annotation_map)
    }

    /// Tries to parse a command with each of the parsers in the whitelist.
    /// Returns the first Program that matches a parser.
    /// If no parser matches this invocation, returns a parsed command where all arguments are of
    /// type "str" (the default).
    pub fn parse_command(&self, invocation: &Vec<String>) -> Result<ArgMatch> {
        for i in 0..self.annotations.len() {
            match self.parse_invocation(&invocation, i) {
                Ok(ret) => {
                    return Ok(ret);
                }
                Err(e) => {
                    if self.debug {
                        debug!("Failed to parse: {:?}", e);
                    }
                }
            }
        }
        debug!(
            "Warning: invocation {:?} was not parsed by any parsers",
            invocation
        );
        return Ok(ArgMatch::new_default(&self.name, &invocation));
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_invalid_opt() {
        let annotation = grammar::Command {
            command_name: "test_command".to_string(),
            args: vec![grammar::Argument::LoneOption(grammar::Opt::default())],
            parsing_options: Default::default(),
        };
        let parser = CmdParser::new("test_command");
        match parser.validate(&annotation) {
            Ok(_) => {
                assert!(false, "annotation should not have been parsed correctly.");
            }
            Err(_e) => {
                // TODO: assert the failure
                /*assert_eq!(
                    e,
                    bail!("Atleast one of short or long should be specified for option.")
                );*/
            }
        }
    }

    #[test]
    fn test_validate_invalid_optparam() {
        let annotation = grammar::Command {
            command_name: "test_command".to_string(),
            args: vec![grammar::Argument::OptWithParam(
                grammar::Opt::default(),
                grammar::Param::default(),
            )],
            parsing_options: Default::default(),
        };
        let parser = CmdParser::new("test_command");
        match parser.validate(&annotation) {
            Ok(_) => {
                assert!(false, "annotation should not have been parsed correctly.");
            }
            Err(_e) => {
                // TODO: assert the failure
                {
                    /*assert_eq!(
                        e,
                        bail!("Atleast one of short or long should be specified for option.")
                    );*/
                }
            }
        }
    }

    #[test]
    fn test_validate_invalid_param_list() {
        let param_with_list: grammar::Argument = grammar::Argument::LoneParam(grammar::Param {
            param_type: grammar::ArgType::Str,
            size: grammar::ParamSize::List(grammar::ListSeparator::Space),
            default_value: "".to_string(),
            multiple: false,
        });
        let annotation = grammar::Command {
            command_name: "test_command".to_string(),
            args: vec![
                grammar::Argument::OptWithParam(grammar::Opt::default(), grammar::Param::default()),
                param_with_list.clone(),
                param_with_list.clone(),
            ],
            parsing_options: Default::default(),
        };
        let parser = CmdParser::new("test_command");
        match parser.validate(&annotation) {
            Ok(_) => {
                assert!(false, "annotation should not have been parsed correctly.");
            }
            Err(_e) => {
                // TODO: assert the failure
                {
                    /*assert_eq!(
                        e,
                        bail!("Cannot have multiple args with size > 1")
                    );*/
                }
            }
        }
    }

    #[test]
    fn test_validate_valid_param_list() {
        let param_with_list: grammar::Argument = grammar::Argument::LoneParam(grammar::Param {
            param_type: grammar::ArgType::Str,
            size: grammar::ParamSize::List(grammar::ListSeparator::Space),
            default_value: "".to_string(),
            multiple: false,
        });
        let param_with_comma: grammar::Argument = grammar::Argument::LoneParam(grammar::Param {
            param_type: grammar::ArgType::Str,
            size: grammar::ParamSize::List(grammar::ListSeparator::Comma),
            default_value: "".to_string(),
            multiple: false,
        });
        let annotation = grammar::Command {
            command_name: "test_command".to_string(),
            args: vec![param_with_comma.clone(), param_with_list.clone()],
            parsing_options: Default::default(),
        };
        let parser = CmdParser::new("test_command");
        assert_eq!(parser.validate(&annotation).unwrap(), ());
    }
}
