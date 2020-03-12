use super::{annotations2, config, scheduler, shellparser, Result};
use annotations2::{argument_matcher, cmd_parser};
use cmd_parser::Parser;
use argument_matcher::ArgMatch;
use scheduler::
use config::network::{FileNetwork, ServerInfo, ServerKey};
use failure::bail;
use shellparser::shellparser::ShellSplit;

pub struct Interpreter<T> {
    config: FileNetwork,
    parser: Parser,
    scheduler: 

}

impl Interpreter {}
