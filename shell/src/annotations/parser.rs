extern crate clap;
extern crate dash;

use super::grammar;
use clap::{App, Arg};
use dash::dag::{node, stream};
use dash::util::Result;
use failure::bail;
use std::collections::HashMap;
// TODO: should this be a tree of parsers somehow?
struct Parser<'a, 'b> {
    annotation: grammar::Command,
    app: App<'a, 'b>,
    counter: usize,
    type_map: HashMap<String, grammar::ArgType>,
}

impl<'a, 'b> Parser<'a, 'b> {
    //! Takes a Command and builds a parser out of it
    //! So we build this parser -- but how do we associate it with the type information
    //! Also for param size many -- we need to know how to parse the list (i.e. this thing will
    //! consider it free args)
    pub fn new(cmd: grammar::Command) -> Result<Self> {
        let app = App::new("program").version("1.0").author("doesn't matter");
        let type_map: HashMap<String, grammar::ArgType> = HashMap::new();
        let counter: usize = cmd.args.len();
        Ok(Parser {
            annotation: cmd,
            counter: counter,
            type_map: type_map,
            app: app,
        })
    }

    pub fn build_parser(&mut self) {
        let mut counter: u32 = 0; // index args
        for i in 0..counter {
            match self.annotation.args[i as usize] {
                grammar::Argument::LoneOption(opt) => {
                    self.app = self.app.arg(
                        Arg::with_name(self.annotation.arg_keys[i as usize].as_str())
                            .short(&opt.short)
                            .long(&opt.long),
                    );
                }
                grammar::Argument::OptWithParam(opt, param_info) => {
                    let mut optarg = Arg::with_name(&self.annotation.arg_keys[i as usize])
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
                    self.type_map
                        .insert(counter.to_string(), param_info.param_type);
                    self.app = self.app.arg(optarg);
                }
                grammar::Argument::LoneParam(param) => {
                    // TODO: figure out the one to use here
                    let mut optarg = Arg::with_name(&self.annotation.arg_keys[i as usize]);
                    match param.size {
                        grammar::ParamSize::Zero => {
                            unreachable!();
                        }
                        grammar::ParamSize::One => {
                            optarg = optarg.takes_value(true);
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
                    self.type_map.insert(counter.to_string(), param.param_type);
                    self.app = self.app.arg(optarg);
                }
            }
            counter += 1;
        }
    }

    pub fn parse_command(&self, cmd: String) -> Result<node::Program> {
        // split the String to pass into the app
        // then construct the program from this

        unimplemented!();
    }
}
