extern crate dash;

use dash::util::Result;
//use failure::bail;

use super::annotation_parser::parse_annotation_file;
use super::fileinfo::FileMap;
use super::grammar::*;
use super::parser::Parser;
use super::shell_interpreter;
use super::shell_parse;
use cmd::{CommandNode, NodeArg};
use dash::dag::{node, stream};
use dash::graph;
use dash::graph::{cmd, program, rapper, Location};
use graph::stream::{DashStream, FileStream};
use program::{Elem, NodeId, Program};
use rapper::Rapper;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

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

    pub fn parse_cmd_graph(&mut self, command: &str) -> Result<Program> {
        // make a shell split from the command
        let shellsplit = shell_parse::ShellSplit::new(command)?;

        // turn shell split into shell graph
        let shellgraph = shellsplit.convert_into_shell_graph()?;

        // turn this into node graph that can be fed into the annotation layer to be executed
        let mut program = shellgraph.convert_into_program()?;

        // apply the parser
        self.apply_parser(&mut program)?;

        // later: for all *cmd* nodes -- apply any graph substitutions to generate a parallel
        // version of the same command

        // for all FILESTREAMS -- try to apply the mount info to figure out if the file is remote
        self.resolve_filestreams(&mut program)?;
        // apply location algorithm
        self.assign_program_location(&mut program)?;
        Ok(program)
    }

    /// Resolves any filestreams in the nodes to take note of their possible local or remote
    /// location.
    /// Note that command nodes aren't allowed to have any stdin or stdout that are filestreams;
    /// those are all replaced with a corresponding read and write node.
    fn resolve_filestreams(&mut self, program: &mut Program) -> Result<()> {
        for (_, node) in program.get_mut_nodes_iter() {
            match node.get_mut_elem() {
                Elem::Cmd(ref mut command_node) => {
                    // iterate through op args
                    for node_arg in command_node.get_args_iter_mut() {
                        match node_arg {
                            NodeArg::Str(_) => {}
                            NodeArg::Stream(ref mut fs) => {
                                self.filemap.modify_stream_to_remote(fs)?;
                            }
                        }
                    }
                }
                Elem::Write(ref mut write_node) => {
                    // iterate through output streams
                    for dashstream in write_node.get_stdout_iter_mut() {
                        match dashstream {
                            DashStream::File(ref mut fs) => {
                                self.filemap.modify_stream_to_remote(fs)?;
                            }
                            _ => {}
                        }
                    }
                }
                Elem::Read(ref mut read_node) => {
                    // iterate through input streams
                    for dashstream in read_node.get_stdin_iter_mut() {
                        match dashstream {
                            DashStream::File(ref mut fs) => {
                                self.filemap.modify_stream_to_remote(fs)?;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn apply_parser(&mut self, program: &mut Program) -> Result<()> {
        // for each individual node, if it is a *cmd* node, apply the parser
        // TODO: figure out if this actually applies the parser :)
        for (_, node) in program.get_mut_nodes_iter() {
            let elem = node.get_mut_elem();
            if let Elem::Cmd(ref mut command_node) = elem {
                if command_node.args_len() == 0 {
                    continue;
                }
                // if a parser exists, try to parse the command into a parsed command
                if self.parsers.contains_key(&command_node.get_name()) {
                    let parser: &mut Parser =
                        self.parsers.get_mut(&command_node.get_name()).unwrap();
                    // use the parser to turn the String args into "types"
                    let args = command_node.get_string_args();
                    let typed_args = parser.parse_command(args)?;
                    // interpret typed arguments and add them to the op
                    self.interpret_cmd_types(typed_args, command_node)?;
                }
            }
        }
        Ok(())
    }

    // TODO: OLD FUNCTION
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

    fn interpret_cmd_types(&mut self, cmd: ParsedCommand, command: &mut CommandNode) -> Result<()> {
        command.clear_args();
        for arg in cmd.typed_args.iter() {
            match arg.1 {
                ArgType::Str => {
                    command.add_arg(NodeArg::Str(arg.0.clone()));
                }
                ArgType::InputFile | ArgType::OutputFile => {
                    command.add_arg(NodeArg::Stream(FileStream::new(
                        &arg.0.clone(),
                        Location::Client,
                    )));
                }
                ArgType::InputFileList | ArgType::OutputFileList => {
                    unimplemented!();
                }
            }
        }
        Ok(())
    }

    /// TODO: OLD FUNCTION
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

    /// Assigns a location to each node in the program,
    /// and modifies any pipes to be TCP streams when necessary.
    /// Current algorithm:
    /// read/write nodes must run on the machine where the input/output file stream is located.
    /// command nodes must run where any FileStream arguments are located.
    /// Otherwise, preserve locality: e.g. try to run where the last command ran.
    /// TODO: maybe we can add semantics about input > output? (to help with the decision).
    fn assign_program_location(&mut self, prog: &mut Program) -> Result<()> {
        // iterate through nodes and assign any mandatory locations
        let mandatory_location = |locs: Vec<Location>| -> Option<Location> {
            let mut set: HashSet<Location> = HashSet::from_iter(locs);
            match set.len() {
                0 => None,
                1 => {
                    let mut ret_val: Option<Location> = None;
                    for loc in set.drain() {
                        ret_val = Some(loc)
                    }
                    ret_val
                }
                _ => Some(Location::Client),
            }
        };
        let mut assigned: HashSet<NodeId> = HashSet::default();
        for (id, node) in prog.get_mut_nodes_iter() {
            match node.get_mut_elem() {
                Elem::Read(ref mut readnode) => {
                    let locations = readnode.get_input_locations();
                    match mandatory_location(locations) {
                        Some(loc) => {
                            readnode.set_loc(loc);
                            assigned.insert(*id);
                        }
                        None => {}
                    }
                }
                Elem::Write(ref mut writenode) => {
                    let locations = writenode.get_output_locations();
                    match mandatory_location(locations) {
                        Some(loc) => {
                            writenode.set_loc(loc);
                            assigned.insert(*id);
                        }
                        None => {}
                    }
                }
                Elem::Cmd(ref mut cmdnode) => {
                    let locations = cmdnode.arg_locations();
                    match mandatory_location(locations) {
                        Some(loc) => {
                            cmdnode.set_loc(loc);
                            assigned.insert(*id);
                        }
                        None => {}
                    }
                }
            }
        }

        // TODO: program in a DP to calculate the optimal location for each node
        // right now -- just fill in the nodes where *most* of the nodes are evaluating
        let mut location_count: HashMap<Location, u32> = HashMap::default();
        for (id, node) in prog.get_nodes_iter() {
            if assigned.contains(id) {
                if location_count.contains_key(&node.get_loc()) {
                    let mut_count = location_count.get_mut(&node.get_loc()).unwrap();
                    *mut_count += 1;
                } else {
                    location_count.insert(node.get_loc(), 1);
                }
            }
        }
        let mut max_counted_location = Location::default();
        let mut max_count: u32 = 0;
        for (loc, count) in location_count.iter() {
            if max_count == 0 || *count > max_count {
                max_count = *count;
                max_counted_location = loc.clone();
            }
        }

        for (id, node) in prog.get_mut_nodes_iter() {
            if !assigned.contains(id) {
                node.set_loc(max_counted_location.clone());
            }
        }

        Ok(())
    }

    /// TODO: OLD FUNCTION
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
