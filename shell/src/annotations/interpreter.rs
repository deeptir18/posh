extern crate dash;

use dash::util::Result;
//use failure::bail;

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
                parser.add_annotation(cmd)?;
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
        let shell_program: Vec<(node::Node, Vec<String>)> =
            shell_interpreter::shell_split(command, &self.filemap)?;
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
        for i in 0..prog.len() {
            let op: &mut node::Node = prog.get_mut(i).unwrap();
            if op.has_local_dependencies() {
                any_local = true;
            }
        }

        for i in 0..prog.len() {
            let location: node::ExecutionLocation = match any_local {
                true => node::ExecutionLocation::Client,
                false => node::ExecutionLocation::StorageServer,
            };
            let op: &mut node::Node = prog.get_mut(i).unwrap();
            op.set_location(location);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    fn get_test_filemap() -> FileMap {
        let mut map: HashMap<String, String> = HashMap::default();
        map.insert("/d/c/".to_string(), "127.0.0.1".to_string());
        FileMap::construct(map)
    }

    // "tar: FLAGS:[(short:o,long:option,desc:(foo foo)),(short:d,long:debug,desc:(debug mode))] OPTPARAMS:[(short:d,long:directory,type:input_file,size:1,default_value:\".\"),(short:p,long:parent,desc:(parent dir),type:str,size:1,default_value:\"..\"),]"
    fn get_cat_parser() -> Parser {
        let mut parser = Parser::new("cat");
        let annotation = "cat: PARAMS:[(type:input_file,size:list(list_separator:( ))),]";
        parser.add_annotation(Command::new(annotation).unwrap());
        parser
    }

    fn get_grep_parser() -> Parser {
        let mut parser = Parser::new("grep");
        let annotation = "grep: OPTPARAMS:[(short:e,long:regexp,type:str,size:1),(short:f,long:file,type:input_file,size:1)] PARAMS:[(type:input_file,size:list(list_separator:( )))]";
        parser.add_annotation(Command::new(annotation).unwrap());
        parser
    }

    fn get_sort_parser() -> Parser {
        let mut parser = Parser::new("sort");
        let annotation = "sort: FLAGS:[(short:r,long:reverse)] PARAMS:[(type:input_file,size:list(list_separator:( )))]";
        parser.add_annotation(Command::new(annotation).unwrap());
        parser
    }

    fn get_wc_parser() -> Parser {
        let mut parser = Parser::new("wc");
        let annotation = "wc: FLAGS:[(short:l,long:lines)] PARAMS:[(type:input_file,size:list(list_separator:( )))]";
        parser.add_annotation(Command::new(annotation).unwrap());
        parser
    }

    fn get_test_parser() -> HashMap<String, Parser> {
        let mut parsers: HashMap<String, Parser> = HashMap::default();
        parsers.insert("cat".to_string(), get_cat_parser());
        parsers.insert("grep".to_string(), get_grep_parser());
        parsers.insert("sort".to_string(), get_sort_parser());
        parsers.insert("wc".to_string(), get_wc_parser());
        parsers
    }

    fn get_test_interpreter() -> Interpreter {
        Interpreter {
            parsers: get_test_parser(),
            filemap: get_test_filemap(),
        }
    }

    #[test]
    fn test_parse_command_remote() {
        let mut interpreter = get_test_interpreter();
        let mut program = interpreter.parse_command(
            "cat /d/c/b/foo.txt /d/c/bar.txt | grep -e 'a|b|c|d' | sort -r | wc > /d/c/blah.txt",
        ).unwrap();
        let mut expected_program = node::Program::default();
        expected_program.add_op(node::Node::construct(
            "cat".to_string(),
            vec![
                node::OpArg::Stream(stream::DataStream::new(
                    stream::StreamType::RemoteFile,
                    "b/foo.txt",
                )),
                node::OpArg::Stream(stream::DataStream::new(
                    stream::StreamType::RemoteFile,
                    "bar.txt",
                )),
            ],
            stream::DataStream::default(),
            stream::DataStream::new(stream::StreamType::Pipe, "pipe_0"),
            stream::DataStream::new(stream::StreamType::LocalStdout, ""),
            node::OpAction::Spawn,
            node::ExecutionLocation::StorageServer,
        ));

        expected_program.add_op(node::Node::construct(
            "grep".to_string(),
            vec![
                node::OpArg::Arg("-e".to_string()),
                node::OpArg::Arg("a|b|c|d".to_string()),
            ],
            stream::DataStream::new(stream::StreamType::Pipe, "pipe_0"),
            stream::DataStream::new(stream::StreamType::Pipe, "pipe_1"),
            stream::DataStream::new(stream::StreamType::LocalStdout, ""),
            node::OpAction::Spawn,
            node::ExecutionLocation::StorageServer,
        ));
        expected_program.add_op(node::Node::construct(
            "sort".to_string(),
            vec![node::OpArg::Arg("-r".to_string())],
            stream::DataStream::new(stream::StreamType::Pipe, "pipe_1"),
            stream::DataStream::new(stream::StreamType::Pipe, "pipe_2"),
            stream::DataStream::new(stream::StreamType::LocalStdout, ""),
            node::OpAction::Spawn,
            node::ExecutionLocation::StorageServer,
        ));
        expected_program.add_op(node::Node::construct(
            "wc".to_string(),
            vec![],
            stream::DataStream::new(stream::StreamType::Pipe, "pipe_2"),
            stream::DataStream::new(stream::StreamType::RemoteFile, "blah.txt"),
            stream::DataStream::new(stream::StreamType::LocalStdout, ""),
            node::OpAction::Run,
            node::ExecutionLocation::StorageServer,
        ));
        assert_eq!(program, expected_program);
    }

    #[test]
    fn test_parse_command_local() {
        let mut interpreter = get_test_interpreter();
        let mut program = interpreter
            .parse_command(
                "cat /d/c/b/foo.txt bar.txt | grep -e 'a|b|c|d' | sort -r | wc > /d/c/blah.txt",
            )
            .unwrap();
        let mut expected_program = node::Program::default();
        expected_program.add_op(node::Node::construct(
            "cat".to_string(),
            vec![
                node::OpArg::Stream(stream::DataStream::new(
                    stream::StreamType::RemoteFile,
                    "b/foo.txt",
                )),
                node::OpArg::Stream(stream::DataStream::new(
                    stream::StreamType::LocalFile,
                    "bar.txt",
                )),
            ],
            stream::DataStream::default(),
            stream::DataStream::new(stream::StreamType::Pipe, "pipe_0"),
            stream::DataStream::new(stream::StreamType::LocalStdout, ""),
            node::OpAction::Spawn,
            node::ExecutionLocation::Client,
        ));

        expected_program.add_op(node::Node::construct(
            "grep".to_string(),
            vec![
                node::OpArg::Arg("-e".to_string()),
                node::OpArg::Arg("a|b|c|d".to_string()),
            ],
            stream::DataStream::new(stream::StreamType::Pipe, "pipe_0"),
            stream::DataStream::new(stream::StreamType::Pipe, "pipe_1"),
            stream::DataStream::new(stream::StreamType::LocalStdout, ""),
            node::OpAction::Spawn,
            node::ExecutionLocation::Client,
        ));
        expected_program.add_op(node::Node::construct(
            "sort".to_string(),
            vec![node::OpArg::Arg("-r".to_string())],
            stream::DataStream::new(stream::StreamType::Pipe, "pipe_1"),
            stream::DataStream::new(stream::StreamType::Pipe, "pipe_2"),
            stream::DataStream::new(stream::StreamType::LocalStdout, ""),
            node::OpAction::Spawn,
            node::ExecutionLocation::Client,
        ));
        expected_program.add_op(node::Node::construct(
            "wc".to_string(),
            vec![],
            stream::DataStream::new(stream::StreamType::Pipe, "pipe_2"),
            stream::DataStream::new(stream::StreamType::RemoteFile, "blah.txt"),
            stream::DataStream::new(stream::StreamType::LocalStdout, ""),
            node::OpAction::Run,
            node::ExecutionLocation::Client,
        ));
        assert_eq!(program, expected_program);
    }

}
