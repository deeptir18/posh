extern crate dash;

use dash::util::Result;
//use failure::bail;
use super::annotation_parser::parse_annotation_file;
use super::fileinfo::FileMap;
use super::grammar::*;
use super::parser::Parser;
use super::shell_interpreter;
use super::shell_parse;
use super::special_commands::parse_export_command;
use cmd::{CommandNode, NodeArg};
use dash::dag::{node, stream};
use dash::graph;
use dash::graph::{cmd, program, rapper, Location};
use failure::bail;
use graph::stream::{DashStream, FileStream};
use program::{Elem, Node, NodeId, Program};
use rapper::Rapper;
use std::collections::{HashMap, HashSet};
use std::convert::Into;
use std::env;
use std::iter::FromIterator;
use std::path::PathBuf;
pub struct Interpreter {
    pub parsers: HashMap<String, Parser>,
    pub filemap: FileMap,
    pub pwd: PathBuf,
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

        // Note: for correct use, should call set pwd after
        Ok(Interpreter {
            parsers: parser_map,
            filemap: folders,
            pwd: PathBuf::new(),
        })
    }

    pub fn set_pwd(&mut self, pwd: PathBuf) {
        self.pwd = pwd;
    }

    /// Command could contain some information needed by the shell, e.g. export variables,
    /// or may need to be handled separately, via xargs.
    /// TODO: this is a hacky way to handle this entire thing...
    pub fn parse(&mut self, command: &str) -> Result<Option<Program>> {
        if command.starts_with("export") {
            // set the underlying environment variable
            match parse_export_command(command) {
                Ok((var, value)) => {
                    // set an environment value
                    env::set_var(var, value);
                }
                Err(e) => {
                    bail!("Could not parse export command: {:?}", e);
                }
            }
            Ok(None)
        } else {
            let prog = self.parse_cmd_graph(command)?;
            Ok(Some(prog))
        }
    }

    pub fn parse_cmd_graph(&mut self, command: &str) -> Result<Program> {
        // make a shell split from the command
        let shellsplit = shell_parse::ShellSplit::new(command)?;
        // turn shell split into shell graph
        let shellgraph = shellsplit.convert_into_shell_graph()?;

        // turn this into node graph that can be fed into the annotation layer to be executed
        let mut program = shellgraph.convert_into_program()?;
        println!("prog: {:?}", program);

        // apply the parser
        // note: involves interpreting * for JUST parallelizable arguments
        self.apply_parser(&mut program)?;

        // in case any arguments correspond to environment vars -> resolve them
        self.resolve_env_vars(&mut program)?;

        // later: for all *cmd* nodes -- apply any graph substitutions to generate a parallel, if it is parallelizable
        self.parallelize_cmd_nodes(&mut program)?;

        // for all FILESTREAMS -- try to apply the mount info to figure out if the file is remote
        // This also resolves environment vars for files & *patterns
        self.resolve_filestreams(&mut program)?;
        // apply location algorithm
        self.assign_program_location(&mut program)?;
        Ok(program)
    }

    pub fn check_cmd_splittable(&self, cmd_name: &str) -> bool {
        for (name, parser) in self.parsers.iter() {
            if cmd_name == name {
                if parser.splittable_across_input() {
                    return true;
                }
            }
        }
        return false;
    }

    pub fn parallelize_cmd_nodes(&mut self, program: &mut Program) -> Result<()> {
        // iterate through nodes, and if any are splittable across input, update to replace the
        // relevant edges and streams and nodes
        let mut nodes_to_split: Vec<NodeId> = Vec::new();
        for (id, node) in program.get_nodes_iter() {
            match node.get_elem() {
                Elem::Cmd(cmdnode) => {
                    if self.check_cmd_splittable(&cmdnode.get_name()) {
                        nodes_to_split.push(*id);
                    }
                }
                _ => {}
            }
        }

        // for nodes to split, split across the stdin
        for node in nodes_to_split.iter() {
            program.split_across_input(*node)?;
        }
        Ok(())
    }

    pub fn resolve_env_vars(&mut self, program: &mut Program) -> Result<()> {
        for (_, node) in program.get_mut_nodes_iter() {
            match node.get_mut_elem() {
                Elem::Cmd(ref mut command_node) => {
                    // iterate over args trying to resolve any environment variables
                    for arg in command_node.get_args_iter_mut() {
                        match arg {
                            NodeArg::Str(ref mut arg) => {
                                if arg.starts_with("$") {
                                    let var_name = arg.split_at(1).1.to_string();
                                    match env::var(var_name) {
                                        Ok(val) => {
                                            arg.clear();
                                            arg.push_str(&val);
                                        }
                                        Err(e) => {
                                            println!("Couldn't resolve: {:?} -> {:?}", arg, e);
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    /// Resolves any filestreams in the nodes to take note of their possible local or remote
    /// location.
    /// Note that command nodes aren't allowed to have any stdin or stdout that are filestreams;
    /// those are all replaced with a corresponding read and write node.
    pub fn resolve_filestreams(&mut self, program: &mut Program) -> Result<()> {
        for (_, node) in program.get_mut_nodes_iter() {
            match node.get_mut_elem() {
                Elem::Cmd(ref mut command_node) => {
                    // iterate through op args
                    // keep track of args to resolve, and
                    let mut new_args: Vec<NodeArg> = Vec::new();
                    for node_arg in command_node.get_args_iter_mut() {
                        match node_arg {
                            NodeArg::Str(val) => {
                                new_args.push(NodeArg::Str(val.to_string()));
                            }
                            NodeArg::Stream(ref mut fs) => {
                                println!("---");
                                println!("Handling resolution of: {:?}", fs);
                                // first, split into multiple filestreams if there are any patterns
                                let mut replacements =
                                    self.filemap.resolve_filestream_with_pattern(fs)?;
                                println!("Replacements: {:?}", replacements);
                                // then, apply resolution
                                for fs in replacements.iter_mut() {
                                    self.filemap.resolve_filestream(fs, &self.pwd)?;
                                    println!("Fs after resolution: {:?}", fs);
                                }
                                for fs in replacements.into_iter() {
                                    new_args.push(NodeArg::Stream(fs));
                                }
                                println!("---");
                            }
                        }
                    }
                    println!("new args: {:?}", new_args);
                    command_node.set_args(new_args);
                }
                Elem::Write(ref mut write_node) => {
                    // iterate through output streams
                    for dashstream in write_node.get_stdout_iter_mut() {
                        match dashstream {
                            DashStream::File(ref mut fs) => {
                                self.filemap.resolve_filestream(fs, &self.pwd)?;
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
                                self.filemap.resolve_filestream(fs, &self.pwd)?;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn apply_parser(&mut self, program: &mut Program) -> Result<()> {
        // some nodes will be replaced by a vec of nodes, because of parallelization
        // maintain a map of nodes to nodes to replace with
        let mut node_repl_map: Vec<(NodeId, Vec<ParsedCommand>)> = Vec::new();
        for (id, node) in program.get_mut_nodes_iter() {
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
                    // creates a vec of parsed commands
                    let typed_args = parser.parse_command(args)?;
                    node_repl_map.push((*id, typed_args));
                }
            }
        }

        for (id, parsed_cmds) in node_repl_map.iter() {
            println!("{:?}: {:?}", id, parsed_cmds);
        }
        // iterate through node replacement map,
        // generate new command nodes to replace
        for (id, parsed_commands) in node_repl_map.into_iter() {
            let node = program.get_node(id).unwrap();

            let new_command_nodes = self.interpret_cmd_types(parsed_commands, node)?;
            for node in new_command_nodes.iter() {
                println!("replacement node: {:?}", node);
                println!("-------");
            }
            program.replace_node(id, new_command_nodes)?;
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
                let mut typed_args = parser.parse_command(args)?;
                // interpret typed arguments and add them to the op
                self.interpret_types(typed_args.pop().unwrap(), &mut op)?;
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

    fn interpret_cmd_types(&mut self, cmds: Vec<ParsedCommand>, node: Node) -> Result<Vec<Elem>> {
        let mut ret: Vec<Elem> = Vec::new();
        let command_opt: Option<CommandNode> = node.get_elem().into();
        let mut command = command_opt.unwrap();
        command.clear_args();
        for cmd in cmds.iter() {
            let mut new_node = command.clone();
            for arg in cmd.typed_args.iter() {
                match arg.1 {
                    ArgType::Str => {
                        new_node.add_arg(NodeArg::Str(arg.0.clone()));
                    }
                    ArgType::InputFile | ArgType::OutputFile => {
                        new_node.add_arg(NodeArg::Stream(FileStream::new(
                            &arg.0.clone(),
                            Location::Client,
                        )));
                    }
                    ArgType::InputFileList | ArgType::OutputFileList => {
                        unimplemented!();
                    }
                }
            }
            ret.push(Elem::Cmd(new_node));
        }
        Ok(ret)
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
                    match self.filemap.find_match_str(&arg.0, &self.pwd) {
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
    /// SOMEWHERE, NEED TO MODIFY THE NECESSARY PIPES TO BE TCP STREAMS!
    pub fn assign_program_location(&mut self, prog: &mut Program) -> Result<()> {
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
        // but ignore the write stderr nodes
        let mut location_count: HashMap<Location, u32> = HashMap::default();
        for (id, node) in prog.get_nodes_iter() {
            if assigned.contains(id) {
                let mut ignore = false;
                match node.get_elem() {
                    Elem::Write(writenode) => {
                        for stream in writenode.get_stdout() {
                            match stream {
                                DashStream::Stderr => {
                                    ignore = true;
                                }
                                DashStream::Stdout => {
                                    ignore = true;
                                }
                                _ => {
                                    ignore = true;
                                }
                            }
                        }
                    }
                    _ => {}
                }
                if ignore {
                    continue;
                }
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

        prog.make_pipes_networked()?;

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
    use super::super::examples::*;
    use super::*;

    #[test]
    fn test_sadjad_command() {
        let mut interpreter = get_test_interpreter();
        let program = interpreter
            .parse_cmd_graph(
                "cat /d/c/b/1.INFO | grep '[RAY]' | head -n1 | cut -c 7- > /d/c/b/rays.csv",
            )
            .unwrap();
        println!("expected program: {:?}", program);
    }

    #[test]
    fn test_thumbnail_cmd() {
        let mut interpreter = get_test_interpreter();
        let program = interpreter.parse_cmd_graph(
            "mogrify  -format gif -path thumbs_dir -thumbnail 100x100 data_dir/1.jpg data_dir/2.jpg"
        ).unwrap();
        println!("expected program: {:?}", program);
    }

    #[test]
    fn test_parse_command_remote() {
        let mut interpreter = get_test_interpreter();
        let program = interpreter.parse_command(
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
        let program = interpreter
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
