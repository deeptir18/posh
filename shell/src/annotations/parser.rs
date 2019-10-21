extern crate clap;
extern crate dash;
extern crate itertools;
extern crate shellwords;

use super::grammar;
use clap::{App, Arg, ArgMatches};
use dash::util::Result;
use failure::bail;
use itertools::free::join;
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

    /// Validates an annotation.
    /// Ensures:
    ///     - lone options have short or long specified
    ///     - options with params have short or long specified
    ///     - lone params cannot have multiple values until the last one
    ///     TODO: should the error be a specific error type?
    fn validate(&self, annotation: &grammar::Command) -> Result<()> {
        if annotation.command_name != self.name {
            bail!("Annotation does not refer to the same command as the parser");
        }
        let mut lone_args_with_multiple = false;

        for arg in annotation.args.iter() {
            match arg {
                grammar::Argument::LoneOption(opt) => {
                    if opt.short == "" && opt.long == "" {
                        bail!("Atleast one of short or long should be specified for option.");
                    }
                }
                grammar::Argument::OptWithParam(opt, _param) => {
                    if opt.short == "" && opt.long == "" {
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
    /// * Result<grammar::ParsedCommand> that maps each String in the command to a "type".
    ///     Some strings, such as short options bunched together, will be broken up into separate
    ///     strings.
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

        let mut matches: ArgMatches = app.get_matches_from_safe(invocation.clone())?;
        self.assign_types(&mut matches, &annotation, annotation_map)
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
        matches: &mut ArgMatches,
        annotation: &grammar::Command,
        annotation_map: HashMap<String, usize>,
    ) -> Result<grammar::ParsedCommand> {
        let mut typed_args: Vec<(String, grammar::ArgType)> = Vec::new();
        // TODO: matches.args.iter() might not be publicly available
        for arg in matches.args.iter() {
            let arg_info: &grammar::Argument =
                &annotation.args[annotation_map[arg.0.clone()] as usize];
            match arg_info {
                // TODO: handle multiple true properly
                grammar::Argument::LoneOption(opt) => {
                    if opt.short != "" {
                        typed_args.push((format!("-{}", &opt.short), grammar::ArgType::Str));
                    } else {
                        typed_args.push((format!("--{}", &opt.long), grammar::ArgType::Str));
                    }
                }
                grammar::Argument::OptWithParam(opt, param) => {
                    if opt.short != "" {
                        typed_args.push((format!("-{}", &opt.short), grammar::ArgType::Str));
                    } else {
                        typed_args.push((format!("--{}", &opt.long), grammar::ArgType::Str));
                    }

                    let values = matches.values_of(arg.0.clone()).unwrap();
                    match param.size {
                        grammar::ParamSize::Zero => {
                            unreachable!();
                        }
                        grammar::ParamSize::One => {
                            values.for_each(|val| {
                                typed_args.push((val.to_string(), param.param_type));
                            });
                        }
                        grammar::ParamSize::SpecificSize(_, sep)
                        | grammar::ParamSize::List(sep) => {
                            let mut assigned_type: grammar::ArgType = param.param_type;
                            if param.param_type == grammar::ArgType::InputFile
                                && sep == grammar::ListSeparator::Comma
                            {
                                assigned_type = grammar::ArgType::InputFileList;
                            }
                            if param.param_type == grammar::ArgType::OutputFile
                                && sep == grammar::ListSeparator::Comma
                            {
                                assigned_type = grammar::ArgType::OutputFileList;
                            }

                            match sep {
                                grammar::ListSeparator::Space => {
                                    values.for_each(|val| {
                                        typed_args.push((val.to_string(), assigned_type));
                                    });
                                }
                                grammar::ListSeparator::Comma => {
                                    typed_args.push((join(values, ","), assigned_type));
                                }
                            }
                        }
                    }
                }
                grammar::Argument::LoneParam(param) => {
                    let values = matches.values_of(arg.0.clone()).unwrap();
                    match param.size {
                        grammar::ParamSize::Zero => {
                            unreachable!();
                        }
                        grammar::ParamSize::One => {
                            values.for_each(|val| {
                                typed_args.push((val.to_string(), param.param_type));
                            });
                        }
                        grammar::ParamSize::SpecificSize(_, sep)
                        | grammar::ParamSize::List(sep) => {
                            let mut assigned_type: grammar::ArgType = param.param_type;
                            if param.param_type == grammar::ArgType::InputFile
                                && sep == grammar::ListSeparator::Comma
                            {
                                assigned_type = grammar::ArgType::InputFileList;
                            }
                            if param.param_type == grammar::ArgType::OutputFile
                                && sep == grammar::ListSeparator::Comma
                            {
                                assigned_type = grammar::ArgType::OutputFileList;
                            }
                            match sep {
                                grammar::ListSeparator::Space => {
                                    values.for_each(|val| {
                                        typed_args.push((val.to_string(), assigned_type));
                                    });
                                }
                                grammar::ListSeparator::Comma => {
                                    typed_args.push((join(values, ","), assigned_type));
                                }
                            }
                        }
                    }
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
    pub fn parse_command(&mut self, invocation: Vec<String>) -> Result<grammar::ParsedCommand> {
        for i in 0..self.annotations.len() {
            match self.parse_invocation(&invocation, i) {
                Ok(p) => {
                    return Ok(p);
                }
                Err(_) => {}
            }
        }
        println!(
            "Warning: invocation {:?} was not parsed by any parsers",
            invocation
        );

        self.default_parse(invocation)
    }

    fn default_parse(&mut self, mut invocation: Vec<String>) -> Result<grammar::ParsedCommand> {
        let command = invocation.remove(0);
        if command != self.name {
            bail!("Invocation does not include initial command name");
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_invalid_opt() {
        let annotation = grammar::Command {
            command_name: "test_command".to_string(),
            args: vec![grammar::Argument::LoneOption(grammar::Opt::default())],
        };
        let parser = Parser::new("test_command");
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
        };
        let parser = Parser::new("test_command");
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
        };
        let parser = Parser::new("test_command");
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
        };
        let parser = Parser::new("test_command");
        assert_eq!(parser.validate(&annotation).unwrap(), ());
    }

    #[test]
    fn test_parse_simple_cat_invocation() {
        let file_param: grammar::Argument = grammar::Argument::LoneParam(grammar::Param {
            param_type: grammar::ArgType::InputFile,
            size: grammar::ParamSize::List(grammar::ListSeparator::Space),
            default_value: "".to_string(),
            multiple: false,
        });

        let annotation = grammar::Command {
            command_name: "cat".to_string(),
            args: vec![file_param],
        };

        let mut parser = Parser::new("cat");
        parser.add_annotation(annotation).unwrap();

        let invocation = vec!["cat".to_string(), "file1".to_string(), "file2".to_string()];
        let parsed_command: grammar::ParsedCommand = parser.parse_command(invocation).unwrap();
        assert_eq!(parsed_command.command_name, "cat".to_string());
        assert_eq!(parsed_command.typed_args.len(), 2);
        assert_eq!(
            parsed_command.typed_args,
            vec![
                ("file1".to_string(), grammar::ArgType::InputFile),
                ("file2".to_string(), grammar::ArgType::InputFile)
            ]
        );
    }

    #[test]
    fn test_tar_invocation() {
        let file_param_input: grammar::Argument = grammar::Argument::OptWithParam(
            grammar::Opt {
                short: "f".to_string(),
                long: "".to_string(),
                desc: "".to_string(),
                multiple: false,
            },
            grammar::Param {
                param_type: grammar::ArgType::InputFile,
                size: grammar::ParamSize::One,
                default_value: "".to_string(),
                multiple: false,
            },
        );
        let file_param_output: grammar::Argument = grammar::Argument::OptWithParam(
            grammar::Opt {
                short: "f".to_string(),
                long: "".to_string(),
                desc: "".to_string(),
                multiple: false,
            },
            grammar::Param {
                param_type: grammar::ArgType::OutputFile,
                size: grammar::ParamSize::One,
                default_value: "".to_string(),
                multiple: false,
            },
        );

        let x_opt: grammar::Argument = grammar::Argument::LoneOption(grammar::Opt {
            short: "x".to_string(),
            ..Default::default()
        });
        let z_opt: grammar::Argument = grammar::Argument::LoneOption(grammar::Opt {
            short: "z".to_string(),
            ..Default::default()
        });
        let c_opt: grammar::Argument = grammar::Argument::LoneOption(grammar::Opt {
            short: "c".to_string(),
            ..Default::default()
        });

        let v_opt: grammar::Argument = grammar::Argument::LoneOption(grammar::Opt {
            short: "v".to_string(),
            ..Default::default()
        });

        let create_input_file: grammar::Argument = grammar::Argument::LoneParam(grammar::Param {
            param_type: grammar::ArgType::InputFile,
            size: grammar::ParamSize::List(grammar::ListSeparator::Space),
            ..Default::default()
        });

        let extract_output_file: grammar::Argument = grammar::Argument::OptWithParam(
            grammar::Opt {
                short: "C".to_string(),
                ..Default::default()
            },
            grammar::Param {
                param_type: grammar::ArgType::OutputFile,
                size: grammar::ParamSize::One,
                default_value: ".".to_string(),
                multiple: false,
            },
        );

        let extract_annotation = grammar::Command {
            command_name: "tar".to_string(),
            args: vec![
                x_opt.clone(),
                z_opt.clone(),
                v_opt.clone(),
                file_param_input,
                extract_output_file,
            ],
        };
        let create_annotation = grammar::Command {
            command_name: "tar".to_string(),
            args: vec![c_opt, z_opt, v_opt, file_param_output, create_input_file],
        };

        let mut parser = Parser::new("tar");
        parser.add_annotation(create_annotation).unwrap();
        parser.add_annotation(extract_annotation).unwrap();

        let create_invocation = vec![
            "tar".to_string(),
            "-czf".to_string(),
            "foobar.tar".to_string(),
            "foo".to_string(),
            "bar".to_string(),
        ];
        let parsed_create: grammar::ParsedCommand =
            parser.parse_command(create_invocation).unwrap();
        assert_eq!(parsed_create.command_name, "tar".to_string());
        assert_eq!(parsed_create.typed_args.len(), 6);
        assert!(parsed_create
            .typed_args
            .contains(&("-c".to_string(), grammar::ArgType::Str)));
        assert!(parsed_create
            .typed_args
            .contains(&("-z".to_string(), grammar::ArgType::Str)));
        assert!(parsed_create
            .typed_args
            .contains(&("-f".to_string(), grammar::ArgType::Str)));

        assert!(parsed_create
            .typed_args
            .contains(&("foobar.tar".to_string(), grammar::ArgType::OutputFile)));
        assert!(parsed_create
            .typed_args
            .contains(&("foo".to_string(), grammar::ArgType::InputFile)));
        assert!(parsed_create
            .typed_args
            .contains(&("bar".to_string(), grammar::ArgType::InputFile)));

        let extract_invocation1 = vec![
            "tar".to_string(),
            "-xzf".to_string(),
            "foobar.tar".to_string(),
            "-C".to_string(),
            "foo/".to_string(),
        ];
        let parsed_extract: grammar::ParsedCommand =
            parser.parse_command(extract_invocation1).unwrap();
        assert_eq!(parsed_extract.command_name, "tar".to_string());
        assert_eq!(parsed_extract.typed_args.len(), 6);
        assert!(parsed_extract
            .typed_args
            .contains(&("-x".to_string(), grammar::ArgType::Str)));
        assert!(parsed_extract
            .typed_args
            .contains(&("-z".to_string(), grammar::ArgType::Str)));
        assert!(parsed_extract
            .typed_args
            .contains(&("-f".to_string(), grammar::ArgType::Str)));

        assert!(parsed_extract
            .typed_args
            .contains(&("foobar.tar".to_string(), grammar::ArgType::InputFile)));
        assert!(parsed_extract
            .typed_args
            .contains(&("-C".to_string(), grammar::ArgType::Str)));
        assert!(parsed_extract
            .typed_args
            .contains(&("foo/".to_string(), grammar::ArgType::OutputFile)));
        let extract_invocation2 = vec![
            "tar".to_string(),
            "-xzf".to_string(),
            "foobar.tar".to_string(),
        ];
        let parsed_extract2: grammar::ParsedCommand =
            parser.parse_command(extract_invocation2).unwrap();
        assert_eq!(parsed_extract.command_name, "tar".to_string());
        assert_eq!(parsed_extract.typed_args.len(), 6);
        assert!(parsed_extract2
            .typed_args
            .contains(&("-x".to_string(), grammar::ArgType::Str)));
        assert!(parsed_extract2
            .typed_args
            .contains(&("-z".to_string(), grammar::ArgType::Str)));
        assert!(parsed_extract2
            .typed_args
            .contains(&("-f".to_string(), grammar::ArgType::Str)));

        assert!(parsed_extract2
            .typed_args
            .contains(&("foobar.tar".to_string(), grammar::ArgType::InputFile)));
        assert!(parsed_extract2
            .typed_args
            .contains(&("-C".to_string(), grammar::ArgType::Str)));
        assert!(parsed_extract2
            .typed_args
            .contains(&(".".to_string(), grammar::ArgType::OutputFile)));

        // TODO: test should check foobar.tar follows -f and "." follows -C
    }
}
