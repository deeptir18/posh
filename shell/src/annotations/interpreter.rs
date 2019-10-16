extern crate dash;

use dash::util::Result;
use failure::bail;

use super::annotation_parser::parse_annotation_file;
use super::fileinfo::FileMap;
use super::grammar::*;
use super::parser::Parser;
use super::shell_interpreter;
use dash::dag::{node, stream};
use std::collections::HashMap;
pub struct Interpreter {
    pub parsers: HashMap<String, Parser>,
    pub filemap: FileMap,
}

impl Interpreter {
    pub fn new(annotations_file: &str, folders_file: &str) -> Result<Self> {
        let folders = FileMap::new(folders_file)?;
        let mut parser_map: HashMap<String, Parser> = Default::default();
        let cmds = parse_annotation_file(annotations_file)?;

        for cmd in cmds {
            if parser_map.contains_key(&cmd.command_name) {
                let parser: &mut Parser = parser_map.get_mut(&cmd.command_name).unwrap(); // Should be safe to unwrap here
                parser.add_annotation(cmd)?;
            } else {
                let name = cmd.command_name.clone();
                let mut parser = Parser::new(&name);
                parser.add_annotation(cmd);
                parser_map.insert(name, parser);
            }
        }

        Ok(Interpreter {
            parsers: parser_map,
            filemap: folders,
        })
    }

    pub fn parse_command(&mut self, command: &str) -> Result<node::Program> {
        // shell level parser
        let mut program: node::Program = Default::default();
        let mut shell_program: Vec<(node::Node, Vec<String>)> =
            shell_interpreter::shell_split(command)?;
        for (mut op, args) in shell_program {
            // if no args, just continue
            if args.len() == 0 {
                program.add_op(op);
                continue;
            }

            // if a parser exists, try to parse the command into a parsed command
            if self.parsers.contains_key(&op.name) {
                // TODO: might not need to be mutable here
                let parser: &mut Parser = self.parsers.get_mut(&op.name).unwrap();

                // use the parser to turn the String args into "types"
                let typed_args = parser.parse_command(args)?;

                // interpret typed arguments and add them to the op
                self.interpret_types(typed_args, &mut op)?;
            } else {
                for arg in args {
                    op.add_arg(node::OpArg::Arg(arg));
                }
            }

            // now add op to the program
            program.add_op(op);
        }

        // with all the information about all nodes, assign an execution location to each operation
        self.assign_location(&mut program);
        Ok(program)
    }

    /// Turn the parsed command into a graph node
    fn interpret_types(&mut self, cmd: ParsedCommand, op: &mut node::Node) -> Result<()> {
        for arg in cmd.typed_args.iter() {
            match arg.1 {
                ArgType::Str => {
                    op.add_arg(node::OpArg::Arg(arg.0.clone()));
                }
                ArgType::InputFile | ArgType::OutputFile => {
                    // check where the file resolves to
                    match self.filemap.find_match(&arg.0) {
                        // TODO: add in support for *multiple mounts* and the file being from a
                        Some(fileinfo) => {
                            let datastream = stream::DataStream::strip_prefix(
                                stream::StreamType::RemoteFile,
                                &arg.0,
                                &fileinfo.0,
                            )?;
                            op.add_arg(node::OpArg::Stream(datastream));
                        }
                        None => {
                            let datastream =
                                stream::DataStream::new(stream::StreamType::LocalFile, &arg.0);
                            op.add_arg(node::OpArg::Stream(datastream));
                        }
                    }
                }
                ArgType::InputFileList | ArgType::OutputFileList => {
                    unimplemented!();
                }
            }
        }
        Ok(())
    }

    // With all the information about the command and the files they open, decide on an execution
    // location for each operation
    fn assign_location(&mut self, prog: &mut node::Program) {
        // iterate over the program and if any of the:
        // stdin, input_file, or output_file is Local datastream, then assign all the nodes to
        // local
        // Otherwise, everything can be remote
        let mut any_local = false;
        for i in (0..prog.len()) {
            let op: &mut node::Node = prog.get_mut(i).unwrap();
            if op.has_local_dependencies() {
                any_local = true;
            }
        }

        for i in (0..prog.len()) {
            let location: node::ExecutionLocation = match any_local {
                true => node::ExecutionLocation::Client,
                false => node::ExecutionLocation::StorageServer,
            };
            let op: &mut node::Node = prog.get_mut(i).unwrap();
            op.set_location(location);
        }
    }
}
