use super::{annotations2, config, scheduler, shellparser, Result};
use annotations2::{argument_matcher, grammar, parser};
use argument_matcher::{ArgMatch, RemoteAccessInfo};
use config::filecache::FileCache;
use config::filesize::FileSize;
use config::network::FileNetwork;
use dash::graph::filestream::{FifoMode, FifoStream, FileStream};
use dash::graph::info::Info;
use dash::graph::program::{Elem, NodeId, Program};
use dash::graph::stream::{DashStream, IOType, PipeStream};
use dash::graph::Location;
use failure::bail;
use grammar::{AccessType, ArgType};
use parser::Parser;
use scheduler::Scheduler;
use shellparser::shellparser::{parse_command, Command};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use tracing::debug;

pub struct Interpreter {
    /// Where interpreter keeps track of filesystem and link information for scheduling.
    config: FileNetwork,
    /// Cache for resolving relative paths to full paths
    filecache: FileCache,
    /// Parses command lines and matches with annotation information.
    parser: Parser,
    /// Scheduling object that implements the scheduling functionality.
    scheduler: Box<dyn Scheduler>,
    /// When parallelizing commands on a single machine, what is max way to split?
    splitting_factor: u32,
    /// Current working directory.
    pwd: PathBuf,
    /// Environment values.
    env: HashMap<String, String>,
}

impl Interpreter {
    /// Constructs a new interpreter given a  file with config information and a file with
    /// annotations.
    pub fn new(
        config_file: &str,
        annotations_file: &str,
        scheduler: Box<dyn Scheduler>,
    ) -> Result<Self> {
        let parser = Parser::new(annotations_file)?;
        let config = FileNetwork::new(config_file)?;
        Ok(Interpreter {
            config: config,
            filecache: FileCache::default(),
            parser: parser,
            scheduler: scheduler,
            splitting_factor: 1,
            pwd: Default::default(),
            env: Default::default(),
        })
    }

    pub fn construct(
        config: FileNetwork,
        parser: Parser,
        scheduler: Box<dyn Scheduler>,
        pwd: PathBuf,
        filesizemod: Box<dyn FileSize>,
    ) -> Interpreter {
        Interpreter {
            config: config,
            filecache: FileCache::new(filesizemod),
            parser: parser,
            scheduler: scheduler,
            splitting_factor: 1,
            pwd: pwd,
            env: Default::default(),
        }
    }
    pub fn set_splitting_factor(&mut self, factor: u32) {
        self.splitting_factor = factor;
    }

    pub fn set_pwd(&mut self, pwd: PathBuf) {
        self.pwd = pwd;
    }

    /// Takes a command line and returns a program, ready for execution.
    /// Handles parsing, scheduling, and implicit parallelization.
    pub fn parse_command_line(&mut self, command: &str) -> Result<Option<Program>> {
        // Shell parse pass
        let prog = parse_command(command)?;
        match prog {
            Command::EXPORT(var, value) => {
                // set an environment value
                env::set_var(var.clone(), value.clone());
                self.env.insert(var, value);
                Ok(None)
            }
            Command::PROGRAM(mut program) => {
                self.parse_program(&mut program)?;
                Ok(Some(program))
            }
        }
    }

    /// Runs parsing pipeline, which parses, parallelizes, and schedules programs.
    fn parse_program(&mut self, program: &mut Program) -> Result<()> {
        // run parser to produce arg matches for command nodes
        let mut match_map = self.run_parser(program)?;

        debug!("Finished parser");
        // run parallelization to split any nodes into multiple nodes
        self.parallelize_program(program, &mut match_map)?;

        debug!("Finished parallelization");
        // run scheduler
        let location_assignment = self.scheduler.schedule(
            program,
            &mut match_map,
            &self.config,
            &mut self.filecache,
            self.pwd.as_path(),
        )?;

        debug!("Finished scheduler");
        self.assign_locations(program, &mut match_map, location_assignment)?;
        debug!("Finished assigning locations");

        Ok(())
    }

    /// Takes the program and corresponding argmatch structure and splits it across any
    /// parallelizable arguments.
    fn parallelize_program(
        &mut self,
        program: &mut Program,
        match_map: &mut HashMap<NodeId, ArgMatch>,
    ) -> Result<()> {
        let mut replacement_map: Vec<(NodeId, (Vec<Elem>, Vec<ArgMatch>))> = Vec::new();
        for (id, node) in program.get_mut_nodes_iter() {
            if let Elem::Cmd(ref mut command_node) = node.get_mut_elem() {
                let arg_match = match_map.get_mut(&id).unwrap();
                command_node.clear_args();
                let replacement_matches = arg_match.split(self.splitting_factor, &self.config)?;

                if replacement_matches.len() > 0 {
                    let replacement_nodes: Vec<Elem> = replacement_matches
                        .iter()
                        .map(|_| Elem::Cmd(command_node.clone()))
                        .collect();
                    replacement_map.push((*id, (replacement_nodes, replacement_matches)));
                }
            }
        }
        for (id, (elems, mut argmatches)) in replacement_map.into_iter() {
            let new_ids = program.replace_node_parallel(id, elems, false)?;
            assert!(new_ids.len() == argmatches.len());
            let _ = match_map.remove(&id);
            // add in each part of split argmatch to the overall match map
            for new_id in new_ids.iter() {
                let argmatch = argmatches.remove(0);
                match_map.insert(*new_id, argmatch);
            }
        }

        self.parallelize_program_by_stdin(program, match_map)?;
        Ok(())
    }

    /// Parallelize program over stdin.
    fn parallelize_program_by_stdin(
        &mut self,
        program: &mut Program,
        match_map: &mut HashMap<NodeId, ArgMatch>,
    ) -> Result<()> {
        let mut nodes_to_split: Vec<NodeId> = Vec::new();
        for id in program.execution_order() {
            let node = program.get_node(id).unwrap();
            match node.get_elem() {
                Elem::Cmd(_cmdnode) => {
                    let argmatch = match_map.get(&id).unwrap();
                    if argmatch.get_splittable_across_input() {
                        nodes_to_split.push(id);
                    }
                }
                _ => {}
            }
        }
        for id in nodes_to_split.iter() {
            let new_node_ids = program.split_across_input(*id)?;
            if new_node_ids.len() <= 1 {
                continue;
            }
            // replace each argmatch
            let argmatch = match_map.get(id).unwrap().clone();
            for new_id in new_node_ids.iter() {
                match_map.insert(*new_id, argmatch.clone());
            }
            let _ = match_map.remove(id);
        }
        Ok(())
    }
    /// Finds annotation matches (if any) and resolves Strings in each node of program.
    fn run_parser(&mut self, program: &mut Program) -> Result<HashMap<NodeId, ArgMatch>> {
        let mut match_map: HashMap<NodeId, ArgMatch> = HashMap::default();
        // 1: produce annotation matches
        for (id, node) in program.get_mut_nodes_iter() {
            let elem = node.get_mut_elem();
            if let Elem::Cmd(ref mut command_node) = elem {
                if command_node.args_len() == 0 {
                    continue;
                } else {
                    // get an argmatch for the invocation and attach it to the command node
                    let arg_match = self.parser.match_invocation(
                        command_node.get_name().as_str(),
                        command_node.get_string_args(),
                    )?;
                    match_map.insert(*id, arg_match);
                }
            }
        }

        // Iterate through all nodes and:
        //      (1) Resolve any environment variables
        //      (2) Use glob to split any wildcard arguments for command nodes
        //      (3) Resolve each filestream to a full path. For scheduling at a later step.
        for (id, node) in program.get_mut_nodes_iter() {
            match node.get_mut_elem() {
                Elem::Read(ref mut read_node) => {
                    let filestream = read_node.get_stdin_mut();
                    filestream.resolve_env_var()?;
                    self.filecache.resolve_path(filestream, &self.pwd)?;
                }
                Elem::Write(ref mut write_node) => match write_node.get_stdout_mut() {
                    DashStream::File(ref mut filestream) => {
                        filestream.resolve_env_var()?;
                        self.filecache.resolve_path(filestream, &self.pwd)?;
                    }
                    _ => {}
                },
                Elem::Cmd(_) => {
                    // resolve all environment variables in associated arg match object
                    let arg_match = match_map.get_mut(&id).unwrap();
                    arg_match.resolve_file_paths(&mut self.filecache, &self.pwd.as_path())?;
                    arg_match.resolve_glob()?;
                    arg_match.resolve_env_vars()?;
                }
            }
        }
        Ok(match_map)
    }

    /// Modify program to reflect the assignments.
    /// Includes making any relevant pipes TCP connections,
    /// and modifying filestream prefixes if necessary.
    fn assign_locations(
        &mut self,
        prog: &mut Program,
        matches: &mut HashMap<NodeId, ArgMatch>,
        assignments: HashMap<NodeId, Location>,
    ) -> Result<()> {
        for (id, loc) in assignments.iter() {
            match prog.get_mut_node(*id) {
                Some(ref mut node) => {
                    node.set_loc(loc.clone());
                }
                None => {
                    bail!("Assignment map refers to node {:?} not in program", id);
                }
            }
        }

        // ensure each node can access files at given assigned location
        let mut remote_access_map: HashMap<NodeId, Vec<RemoteAccessInfo>> = HashMap::new();
        for (id, node) in prog.get_mut_nodes_iter() {
            match node.get_loc() {
                Location::Client => {}
                Location::Server(ip) => match node.get_mut_elem() {
                    Elem::Cmd(cmdnode) => {
                        let argmatch = matches.get_mut(id).unwrap();

                        // if the node requires pwd, assign pwd to the cmdnode itself
                        if argmatch.get_needs_current_dir() {
                            let mut fs = FileStream::new(&self.pwd.as_path(), Location::Client);
                            self.config.strip_file_path(
                                &mut fs,
                                &Location::Client,
                                &Location::Server(ip.clone()),
                            )?;
                            tracing::debug!(
                                "Setting cmdnode {:?} to have pwd of {:?}",
                                cmdnode,
                                fs
                            );
                            // set options to "needs current dir"
                            let mut options = cmdnode.get_options();
                            options.set_needs_current_dir(true);
                            cmdnode.set_options(options);
                            cmdnode.set_pwd(&fs.get_path());
                        }
                        // files to setup for remote access
                        let remote_access = argmatch.strip_file_paths(
                            Location::Client,
                            node.get_loc(),
                            &mut self.config,
                        )?;
                        remote_access_map.insert(*id, remote_access);
                    }
                    Elem::Write(ref mut writenode) => {
                        let loc = writenode.get_loc();
                        match writenode.get_stdout_mut() {
                            DashStream::File(ref mut fs) => {
                                self.config.strip_file_path(fs, &Location::Client, &loc)?;
                                fs.set_location(loc.clone());
                            }
                            _ => {}
                        }
                    }
                    Elem::Read(ref mut readnode) => {
                        let loc = readnode.get_loc();
                        let mut input = readnode.get_stdin_mut();
                        self.config
                            .strip_file_path(&mut input, &Location::Client, &loc)?;
                        input.set_location(loc.clone());
                    }
                },
            }
        }

        for (id, remote_access_vec) in remote_access_map.iter() {
            let mut argmatch = matches.get_mut(id).unwrap();
            for remote_access_info in remote_access_vec.iter() {
                self.setup_remote_access(prog, &mut argmatch, &mut remote_access_info.clone())?;
            }
        }

        // ensures all necessary pipestreams are converted to tcpstreams
        prog.make_pipes_networked()?;

        let mut readmap: HashMap<PipeStream, FileStream> = HashMap::new();
        for (_id, node) in prog.get_nodes_iter() {
            match node.get_elem() {
                Elem::Read(readnode) => {
                    if let Some(output) = readnode.get_stdout() {
                        match output {
                            DashStream::Pipe(ps) => {
                                assert_eq!(
                                    assignments.get(&ps.get_left()),
                                    assignments.get(&ps.get_right())
                                );
                                readmap.insert(ps.clone(), readnode.get_input_ref().clone());
                            }
                            _ => {}
                        }
                    } else {
                        bail!("Readnode parsed without output: {:?}", readnode);
                    }
                }
                _ => {}
            }
        }
        for (ps, fs) in readmap.iter() {
            // remove readnode
            prog.remove_node(ps.get_left())?;
            // replace stdin of right side of pipe with Dash filestream instead
            prog.replace_input_pipe(ps.get_left(), &ps, fs.clone())?;
        }

        // iterate through all cmdnodes and reconstruct final arguments
        for (id, node) in prog.get_mut_nodes_iter() {
            match node.get_mut_elem() {
                Elem::Cmd(ref mut cmdnode) => {
                    let argmatch = matches.get(id).unwrap();
                    let arguments = argmatch.reconstruct()?;
                    cmdnode.set_args(arguments);
                }
                _ => {}
            }
        }
        self.mark_pipes_bufferable(prog)?;
        Ok(())
    }

    /// Mark any pipes where buffering is necessary as bufferable.
    /// This is all tcp streams and pipes where the pipe feeds into stdin and it is 2nd or later in
    /// the list.
    fn mark_pipes_bufferable(&self, prog: &mut Program) -> Result<()> {
        let mut bufferable_map: HashMap<DashStream, (NodeId, NodeId)> = HashMap::default();
        for (_id, node) in prog.get_nodes_iter() {
            let stdin = node.get_stdin();
            let mut count = 0;
            for stream in stdin.iter() {
                match stream {
                    DashStream::Tcp(netstream) => {
                        if netstream.get_output_type() != IOType::Stderr {
                            bufferable_map.insert(
                                DashStream::Tcp(netstream.clone()),
                                (netstream.get_left(), netstream.get_right()),
                            );
                        }
                    }
                    DashStream::Pipe(pipestream) => {
                        if count >= 1 {
                            bufferable_map.insert(
                                DashStream::Pipe(pipestream.clone()),
                                (pipestream.get_left(), pipestream.get_right()),
                            );
                        }
                    }
                    _ => {}
                }
                count += 1;
            }
        }
        for (_id, node) in prog.get_mut_nodes_iter() {
            match node.get_mut_elem() {
                Elem::Cmd(ref mut cmdnode) => {
                    for stream in cmdnode.get_stdin_iter_mut() {
                        if bufferable_map.contains_key(&stream) {
                            stream.set_bufferable()?;
                        }
                    }
                    match cmdnode.get_stdout() {
                        Some(stream) => {
                            let mut copy = stream.clone();
                            if bufferable_map.contains_key(&stream) {
                                copy.set_bufferable()?;
                            }
                            cmdnode.set_stdout(copy.clone())?;
                        }
                        None => {}
                    }
                    match cmdnode.get_stderr() {
                        Some(stream) => {
                            let mut copy = stream.clone();
                            if bufferable_map.contains_key(&stream) {
                                copy.set_bufferable()?;
                            }
                            cmdnode.set_stderr(copy.clone())?;
                        }
                        None => {}
                    }
                }
                Elem::Write(ref mut writenode) => {
                    for stream in writenode.get_stdin_iter_mut() {
                        if bufferable_map.contains_key(&stream) {
                            stream.set_bufferable()?;
                        }
                    }
                }
                Elem::Read(ref mut readnode) => {
                    let stream = readnode.get_stdout_mut();
                    if bufferable_map.contains_key(&stream) {
                        stream.set_bufferable()?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Modifies the program to reflect that this filestream is read remotely.
    pub fn setup_remote_access(
        &self,
        prog: &mut Program,
        argmatch: &mut ArgMatch,
        remote_access_info: &mut RemoteAccessInfo,
    ) -> Result<()> {
        // transfer output file to correct location after job is done
        if remote_access_info.argtype == ArgType::OutputFile {
            unimplemented!()
        }
        // transfer input file to correct location before job starts
        if argmatch.get_access_type() != AccessType::Sequential {
            unimplemented!();
        }

        // add in a remote fifo read
        // need to ensure the left side's filestream is stripped to the filestream location
        // and also create a temporary var name on the right side for the fifo
        self.config.strip_file_path(
            &mut remote_access_info.filestream,
            &Location::Client,
            &remote_access_info.origin_location,
        )?;

        // query for a temp file in the new location
        let tmp_path = self.config.get_tmp(
            &remote_access_info.filestream.get_path().as_path(),
            &remote_access_info.access_location,
        )?;

        remote_access_info.set_tmp_name(FileStream::new(
            tmp_path.as_path(),
            remote_access_info.access_location.clone(),
        ));

        // ensure the argument gets changed
        argmatch.change_arg(&remote_access_info)?;

        // add into the the program
        let fifostream = FifoStream::new(
            tmp_path.as_path(),
            remote_access_info.access_location.clone(),
            FifoMode::READ,
        );
        prog.add_remote_fifo_read(
            &remote_access_info.origin_location,
            &remote_access_info.access_location,
            &remote_access_info.filestream,
            &fifostream,
        )?;
        Ok(())
    }
}
