extern crate clap;
extern crate dash;

use super::grammar;
use clap::{App, Arg};
use dash::dag::{node, stream};
use dash::util::Result;
use failure::bail;
use std::collections::HashMap;
// TODO: should this be a tree of parsers somehow?
struct Parser<'a> {
    name: String,
    app: App,
    num_args: u32,
    type_map: HashMap<String, grammar::ArgType>,
}

impl<'a> Parser {
    //! Takes a Command and builds a parser out of it
    //! So we build this parser -- but how do we associate it with the type information
    //! Also for param size many -- we need to know how to parse the list (i.e. this thing will
    //! consider it free args)
    pub fn new(cmd: grammar::Command) -> Result<Self> {
        let app = App::new("program").version("1.0").author("doesn't matter");
        let type_map: HashMap<String, grammar::ArgType> = HashMap::new();
        let counter: u32 = 0; // index args
        for arg in cmd.args {
            match arg {
                grammar::Argument::LoneOption(opt) => {
                    app.arg(
                        Arg::with_name(String::from(counter).as_ref())
                            .short(opt.short.as_ref())
                            .long(opt.long.as_ref()),
                    );
                }
                grammar::Argument::OptWithParam(opt, param_info) => {
                    let mut arg = Arg::with_name(String::from(counter).as_ref())
                        .short(opt.short.as_ref())
                        .long(opt.long.as_ref());
                    // based on the param_info and the
                    match param_info.size {
                        grammar::ParamSize::Zero => {
                            unreachable!();
                        }
                        grammar::ParamSize::One => {
                            arg.takes_value(true);
                        }
                        grammar::ParamSize::List(separator) => {
                            arg.takes_value(true);
                            match separator {
                                grammar::ListSeparator::Space => {
                                    arg.value_terminator(" ");
                                }
                                grammar::ListSeparator::Comma => {} // default
                            }
                        }
                    }
                    type_map.insert(counter, param_info.param_type);
                    app.arg(arg);
                }
                grammar::Argument::LoneParam(param) => {
                    // TODO: figure out the one to use here
                    let mut arg = Arg::with_name(String::from(counter).as_ref());
                    match param.size {
                        grammar::ParamSize::Zero => {
                            unreachable!();
                        }
                        grammar::ParamSize::One => {
                            arg.takes_value(true);
                        }
                        grammar::ParamSize::List(separator) => {
                            arg.takes_value(true);
                            match separator {
                                grammar::ListSeparator::Space => {
                                    arg.value_terminator(" ");
                                }
                                grammar::ListSeparator::Comma => {} // default
                            }
                        }
                    }
                    type_map.insert(counter, param.param_type);
                    app.arg(arg);
                }
            }
            counter += 1;
        }

        Ok(Parser {
            app: app,
            type_map: type_map,
            counter: counter,
        })
    }

    pub fn parse_command(&self, cmd: String) -> Result<node::Program> {
        // split the String to pass into the app
        // then construct the program from this

        unimplemented!();
    }
}
