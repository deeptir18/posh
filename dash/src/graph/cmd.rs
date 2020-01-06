use super::rapper::copy_wrapper as copy;
use super::rapper::iterating_redirect;
use super::rapper::{resolve_file_streams, stream_initiate_filter, InputStreamMetadata, Rapper};
use super::{program, stream, Location, Result};
use failure::bail;
use itertools::join;
use program::{Link, NodeId, ProgId};
use std::process::{ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::slice::IterMut;
use std::thread;
use stream::{
    DashStream, FileStream, HandleIdentifier, IOType, NetStream, OutputHandle, PipeStream,
    SharedPipeMap, SharedStreamMap,
};
use thread::{spawn, JoinHandle};
use which::which;

/// CommandNodes, which have args, are either file streams OR Strings.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum NodeArg {
    Str(String),
    Stream(FileStream),
}

/// Node that runs binaries with the provided arguments, at the given location.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct CommandNode {
    /// Id within the program.
    node_id: NodeId,
    /// Id of the program.
    prog_id: ProgId,
    /// Name of binary program.
    name: String,
    /// arguments to pass in to the binary
    args: Vec<NodeArg>,
    /// Vector of streams that stdin comes from, in serialized order.
    stdin: Vec<DashStream>,
    /// Vector of streams to send stdout of this program on.
    stdout: Vec<DashStream>,
    /// Vector of streams to send stderr of this program on.
    stderr: Vec<DashStream>,
    /// Execution location for the node.
    location: Location,
    /// Resolved args, as strings.
    resolved_args: Vec<String>,
    /// Extra information used for scheduling.
    options: CmdExtraInfo,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub struct CmdExtraInfo {
    pub splittable_across_input: bool,
    pub reduces_input: bool,
    pub needs_current_dir: bool,
}
impl Default for CmdExtraInfo {
    fn default() -> Self {
        CmdExtraInfo {
            splittable_across_input: false,
            reduces_input: false,
            needs_current_dir: false,
        }
    }
}

impl CmdExtraInfo {
    pub fn get_splittable_across_input(&self) -> bool {
        self.splittable_across_input
    }

    pub fn set_splittable_across_input(&mut self, val: bool) {
        self.splittable_across_input = val;
    }

    pub fn get_reduces_input(&self) -> bool {
        self.reduces_input
    }

    pub fn set_reduces_input(&mut self, val: bool) {
        self.reduces_input = val;
    }

    pub fn get_needs_current_dir(&self) -> bool {
        self.needs_current_dir
    }

    pub fn set_needs_current_dir(&mut self, val: bool) {
        self.needs_current_dir = val;
    }
}

impl Default for CommandNode {
    fn default() -> Self {
        CommandNode {
            node_id: Default::default(),
            prog_id: Default::default(),
            name: Default::default(),
            args: vec![],
            stdin: vec![],
            stdout: vec![],
            stderr: vec![],
            location: Default::default(),
            resolved_args: vec![],
            options: Default::default(),
        }
    }
}

impl CommandNode {
    pub fn get_options(&self) -> CmdExtraInfo {
        self.options
    }

    pub fn set_options(&mut self, options: CmdExtraInfo) {
        self.options = options;
    }

    pub fn clear_stdin(&mut self) {
        self.stdin.clear();
    }

    pub fn clear_stdout(&mut self) {
        self.stdout.clear();
    }

    pub fn clear_stderr(&mut self) {
        self.stderr.clear();
    }

    pub fn get_stdin_iter_mut(&mut self) -> IterMut<DashStream> {
        self.stdin.iter_mut()
    }

    pub fn get_stdout_iter_mut(&mut self) -> IterMut<DashStream> {
        self.stdout.iter_mut()
    }

    pub fn get_stderr_iter_mut(&mut self) -> IterMut<DashStream> {
        self.stderr.iter_mut()
    }

    pub fn new(cmd: &str, location: Location) -> Result<Self> {
        match which(cmd) {
            Ok(cmd_path) => match cmd_path.to_str() {
                Some(c) => Ok(CommandNode {
                    name: c.to_string(),
                    location: location,
                    ..Default::default()
                }),
                None => bail!("Could not turn binary to str"),
            },
            Err(e) => bail!("Could not find binary {} -> {:?}", cmd, e),
        }
    }

    /// Vector of locations for any file inputs.
    /// Used to decide location of where to execute a node.
    pub fn arg_locations(&self) -> Vec<Location> {
        let mut ret: Vec<Location> = Vec::new();
        for arg in self.args.iter() {
            match arg {
                NodeArg::Str(_) => {}
                NodeArg::Stream(fs) => ret.push(fs.get_location()),
            }
        }
        ret
    }

    pub fn args_len(&self) -> usize {
        self.args.len()
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_args_iter_mut(&mut self) -> IterMut<NodeArg> {
        self.args.iter_mut()
    }

    pub fn get_string_args(&self) -> Vec<String> {
        let mut ret: Vec<String> = Vec::new();
        for arg in self.args.iter() {
            match arg {
                NodeArg::Str(argument) => ret.push(argument.clone()),
                _ => {}
            }
        }
        ret
    }

    pub fn get_args(&self) -> Vec<NodeArg> {
        self.args.clone()
    }

    pub fn clear_args(&mut self) {
        self.args.clear();
    }

    pub fn set_args(&mut self, args: Vec<NodeArg>) {
        self.clear_args();
        self.args = args;
    }

    pub fn name_set(&self) -> bool {
        match self.name.as_ref() {
            "" => false,
            _ => true,
        }
    }

    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }

    pub fn initialize(&mut self) -> Result<()> {
        if !self.name_set() {
            bail!("Cmd could not be initialized; no name");
        }

        match which(self.name.clone()) {
            Ok(cmd_path) => match cmd_path.to_str() {
                Some(c) => {
                    self.name = c.to_string();
                }
                None => bail!("Could not turn binary to str"),
            },
            Err(e) => bail!("Could not find binary {} -> {:?}", self.name, e),
        }
        Ok(())
    }

    pub fn add_arg(&mut self, arg: NodeArg) {
        self.args.push(arg);
    }

    /// Returns the stream identifier for the stdout, stdin, and stderr handles for *this node*
    fn get_handle_identifier(&self, iotype: IOType) -> HandleIdentifier {
        HandleIdentifier::new(self.prog_id, self.node_id, iotype)
    }

    pub fn replace_stream(&mut self, edge: &Link, new_edge: &Link) -> Result<()> {
        let stream_repl =
            |stream: &mut DashStream, edge: &Link, new_edge: &Link, new_right: bool| -> bool {
                match stream {
                    DashStream::Pipe(ref mut pipestream) => {
                        if pipestream.get_left() == edge.get_left()
                            && pipestream.get_right() == edge.get_right()
                        {
                            if new_right {
                                pipestream.set_right(new_edge.get_right());
                            } else {
                                pipestream.set_left(new_edge.get_left());
                            }
                            return true;
                        } else {
                            return false;
                        }
                    }
                    DashStream::Tcp(ref mut netstream) => {
                        if netstream.get_left() == edge.get_left()
                            && netstream.get_right() == edge.get_right()
                        {
                            if new_right {
                                netstream.set_right(new_edge.get_right());
                            } else {
                                netstream.set_left(new_edge.get_left());
                            }
                            return true;
                        } else {
                            return false;
                        }
                    }
                    _ => {
                        unreachable!();
                    }
                }
            };
        if self.node_id != new_edge.get_left() && self.node_id != new_edge.get_right() {
            bail!("Trying to replace stream where neither left nor right is node ID, edge: {:?}, new_edge: {:?}, id: {:?}", edge, new_edge, self.node_id);
        } else {
            if self.node_id == new_edge.get_left() {
                // replace stream for one of outward edges
                let mut replaced = false;
                for stream in self.stdout.iter_mut() {
                    if replaced {
                        break;
                    }
                    replaced = stream_repl(stream, &edge, &new_edge, false);
                }
                for stream in self.stderr.iter_mut() {
                    if replaced {
                        break;
                    }
                    replaced = stream_repl(stream, &edge, &new_edge, false);
                }
            } else {
                // replace stream for one of the inward edges
                for stream in self.stdin.iter_mut() {
                    if stream_repl(stream, &edge, &new_edge, true) {
                        break;
                    }
                }
            }
            Ok(())
        }
    }

    /// Takes the args and converts the file stream args to strings to pass in.
    fn resolve_file_args(&mut self, parent_dir: &str) -> Result<Vec<String>> {
        let mut arg_iterator = self.args.iter_mut();
        let mut ret: Vec<String> = Vec::new();
        while let Some(arg) = arg_iterator.next() {
            match arg {
                NodeArg::Stream(fs) => {
                    fs.prepend_directory(parent_dir)?;
                    ret.push(fs.get_name());
                }
                NodeArg::Str(a) => {
                    ret.push(a.clone());
                }
            }
        }
        Ok(ret)
    }
}

impl Rapper for CommandNode {
    fn set_id(&mut self, id: NodeId) {
        self.node_id = id;
    }

    fn get_id(&self) -> NodeId {
        self.node_id
    }

    fn get_dot_label(&self) -> Result<String> {
        // get the command name and the args!
        let args: Vec<String> = self
            .args
            .iter()
            .map(|arg| match arg {
                NodeArg::Str(a) => format!("{:?}", a),
                NodeArg::Stream(fs) => fs.get_dot_label(),
            })
            .collect();
        Ok(format!(
            "{}:{}\nargs: {}\nloc: {:?}",
            self.node_id,
            self.name,
            join(args.clone(), ",\n"),
            self.location,
        ))
    }

    fn replace_stream_edges(&mut self, edge: Link, new_edges: Vec<Link>) -> Result<()> {
        // find all pipestreams with this edge as left and right
        // if this node is not in the left or right, don't do any searching
        if self.node_id != edge.get_left() && self.node_id != edge.get_right() {
            bail!("Calling replace stream edges where cmd node is neither left or right of edge to replace, id: {:?}, old_edge: {:?}", self.node_id, edge);
        } else {
            // outward edge, replace stdout and stderr pipes
            let mut streams_to_remove: Vec<DashStream> = Vec::new();
            let mut streams_to_add: Vec<(IOType, DashStream)> = Vec::new();
            // if edge to be replaced is an outward edge
            if self.get_id() == edge.get_left() {
                let mut streams = self.stdout.clone();
                streams.append(&mut self.stderr.clone());
                for stream in streams.iter() {
                    match stream {
                        DashStream::Pipe(pipestream) => {
                            if pipestream.get_left() == edge.get_left()
                                && pipestream.get_right() == edge.get_right()
                            {
                                streams_to_remove.push(DashStream::Pipe(pipestream.clone()));
                                for edge in new_edges.iter() {
                                    let mut new_pipestream = pipestream.clone();
                                    new_pipestream.set_right(edge.get_right());
                                    streams_to_add.push((
                                        new_pipestream.get_output_type(),
                                        DashStream::Pipe(new_pipestream),
                                    ));
                                }
                            }
                        }
                        DashStream::Tcp(tcpstream) => {
                            if tcpstream.get_left() == edge.get_left()
                                && tcpstream.get_right() == edge.get_right()
                            {
                                streams_to_remove.push(DashStream::Tcp(tcpstream.clone()));
                                for edge in new_edges.iter() {
                                    let mut new_tcpstream = tcpstream.clone();
                                    new_tcpstream.set_right(edge.get_right());
                                    streams_to_add.push((
                                        new_tcpstream.get_output_type(),
                                        DashStream::Tcp(new_tcpstream),
                                    ));
                                }
                            }
                        }
                        _ => {}
                    }
                }
            } else {
                let streams = self.stdin.clone();
                for stream in streams.iter() {
                    match stream {
                        DashStream::Pipe(pipestream) => {
                            if pipestream.get_right() == edge.get_right()
                                && pipestream.get_left() == edge.get_left()
                            {
                                streams_to_remove.push(DashStream::Pipe(pipestream.clone()));
                                for edge in new_edges.iter() {
                                    let mut new_pipestream = pipestream.clone();
                                    new_pipestream.set_left(edge.get_left());
                                    streams_to_add
                                        .push((IOType::Stdin, DashStream::Pipe(new_pipestream)));
                                }
                            }
                        }
                        DashStream::Tcp(tcpstream) => {
                            if tcpstream.get_right() == edge.get_right()
                                && tcpstream.get_left() == edge.get_left()
                            {
                                streams_to_remove.push(DashStream::Tcp(tcpstream.clone()));
                                for edge in new_edges.iter() {
                                    let mut new_tcpstream = tcpstream.clone();
                                    new_tcpstream.set_left(edge.get_left());
                                    streams_to_add
                                        .push((IOType::Stdin, DashStream::Tcp(new_tcpstream)));
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            // add and remove the streams
            if self.get_id() == edge.get_left() {
                self.stdout.retain(|x| !streams_to_remove.contains(x));
                self.stderr.retain(|x| !streams_to_remove.contains(x));
                for (iotype, stream) in streams_to_add.iter() {
                    match iotype {
                        IOType::Stdout => {
                            self.stdout.push(stream.clone());
                        }
                        IOType::Stderr => {
                            self.stderr.push(stream.clone());
                        }
                        _ => {}
                    }
                }
            } else {
                self.stdin.retain(|x| !streams_to_remove.contains(&x));
                for (_, stream) in streams_to_add.iter() {
                    self.stdin.push(stream.clone());
                }
            }
        }
        Ok(())
    }

    fn get_stdout_id(&self) -> Option<NodeId> {
        if self.stdout.len() > 1 {
            panic!("Calling get stdout id, but stdout is more than length 1");
        }

        if self.stdout.len() == 0 {
            return None;
        } else {
            let stream = &self.stdout[0];
            match stream {
                DashStream::Pipe(ps) => {
                    return Some(ps.get_right());
                }
                DashStream::Tcp(ns) => {
                    return Some(ns.get_right());
                }
                _ => {
                    unreachable!();
                }
            }
        }
    }

    fn replace_pipe_with_net(
        &mut self,
        pipe: PipeStream,
        net: NetStream,
        iotype: IOType,
    ) -> Result<()> {
        match iotype {
            IOType::Stdin => {
                let prev_len = self.stdin.len();
                self.stdin
                    .retain(|x| x.clone() != DashStream::Pipe(pipe.clone()));
                let new_len = self.stdin.len();
                assert!(new_len == prev_len - 1);
                self.add_stdin(DashStream::Tcp(net))?;
            }
            IOType::Stdout => {
                let prev_len = self.stdout.len();
                self.stdout
                    .retain(|x| x.clone() != DashStream::Pipe(pipe.clone()));
                let new_len = self.stdout.len();
                assert!(new_len == prev_len - 1);
                self.add_stdout(DashStream::Tcp(net))?;
            }
            IOType::Stderr => {
                let prev_len = self.stderr.len();
                self.stderr
                    .retain(|x| x.clone() != DashStream::Pipe(pipe.clone()));
                let new_len = self.stderr.len();
                assert!(new_len == prev_len - 1);
                self.add_stderr(DashStream::Tcp(net))?;
            }
        }
        Ok(())
    }

    fn set_loc(&mut self, loc: Location) {
        self.location = loc;
    }

    fn get_outward_streams(&self, iotype: IOType, is_server: bool) -> Vec<NetStream> {
        let streams: Vec<DashStream> = match iotype {
            IOType::Stdin => self
                .stdin
                .iter()
                .filter(|&s| stream_initiate_filter(s.clone(), self.node_id, is_server))
                .cloned()
                .collect(),
            IOType::Stdout => self
                .stdout
                .iter()
                .filter(|&s| stream_initiate_filter(s.clone(), self.node_id, is_server))
                .cloned()
                .collect(),
            IOType::Stderr => self
                .stderr
                .iter()
                .filter(|&s| stream_initiate_filter(s.clone(), self.node_id, is_server))
                .cloned()
                .collect(),
        };
        streams
            .iter()
            .map(|s| {
                let netstream_result: Option<NetStream> = s.clone().into();
                netstream_result.unwrap()
            })
            .collect()
    }

    fn get_stdin_len(&self) -> usize {
        self.stdin.len()
    }

    fn get_stdout_len(&self) -> usize {
        self.stdout.len()
    }

    fn get_stderr_len(&self) -> usize {
        self.stderr.len()
    }
    fn get_stdin(&self) -> Vec<DashStream> {
        self.stdin.clone()
    }

    fn get_stdout(&self) -> Vec<DashStream> {
        self.stdout.clone()
    }

    fn get_stderr(&self) -> Vec<DashStream> {
        self.stderr.clone()
    }
    fn add_stdin(&mut self, stream: DashStream) -> Result<()> {
        self.stdin.push(stream);
        Ok(())
    }
    fn add_stdout(&mut self, stream: DashStream) -> Result<()> {
        self.stdout.push(stream);
        Ok(())
    }

    fn add_stderr(&mut self, stream: DashStream) -> Result<()> {
        self.stderr.push(stream);
        Ok(())
    }

    fn execute(
        &mut self,
        mut pipes: SharedPipeMap,
        _network_connections: SharedStreamMap,
    ) -> Result<()> {
        let mut cmd = Command::new(self.name.clone());
        cmd.args(self.resolved_args.clone());

        if self.stdin.len() > 0 {
            println!("setting stdin for {:?} to be stdio::piped", self.node_id);
            cmd.stdin(Stdio::piped());
        }
        if self.stdout.len() > 0 {
            println!("setting stdout for {:?} to be stdio::piped", self.node_id);
            cmd.stdout(Stdio::piped());
        }
        if self.stderr.len() > 0 {
            println!("setting stderr for {:?} to be stdio::piped", self.node_id);
            cmd.stderr(Stdio::piped());
        }
        let child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                bail!("Failed to spawn child: {:?}", e);
            }
        };
        println!("spawned cmd: {:?}", cmd);

        if self.stdin.len() > 0 {
            let stdin_handle = match child.stdin {
                Some(h) => h,
                None => bail!("Could not get stdin handle for proc"),
            };
            println!("Inserting {:?}", self.get_handle_identifier(IOType::Stdin));
            pipes.insert(
                self.get_handle_identifier(IOType::Stdin),
                OutputHandle::Stdin(stdin_handle),
            )?;
        }

        if self.stdout.len() > 0 {
            let stdout_handle = match child.stdout {
                Some(h) => h,
                None => bail!("Could not get handle for child stdout"),
            };
            println!("Inserting {:?}", self.get_handle_identifier(IOType::Stdout));
            pipes.insert(
                self.get_handle_identifier(IOType::Stdout),
                OutputHandle::Stdout(stdout_handle),
            )?;
        }

        if self.stderr.len() > 0 {
            let stderr_handle = match child.stderr {
                Some(h) => h,
                None => bail!("Could not get handle for child stderr"),
            };

            println!("Inserting {:?}", self.get_handle_identifier(IOType::Stderr));
            pipes.insert(
                self.get_handle_identifier(IOType::Stderr),
                OutputHandle::Stderr(stderr_handle),
            )?;
        }
        Ok(())
    }

    fn run_redirection(
        &mut self,
        mut pipes: SharedPipeMap,
        network_connections: SharedStreamMap,
        tmp: String,
    ) -> Result<()> {
        let mut join_handles: Vec<(IOType, JoinHandle<Result<()>>)> = Vec::new();
        // spawn stdin thread -- this thread internally needs to handle reading and buffering from
        // different inputs
        if self.stdin.len() > 0 {
            let stdin_prog_id = self.prog_id;
            let stdin_handle = pipes.remove(&self.get_handle_identifier(IOType::Stdin))?;
            let stdin_streams = self.stdin.clone();
            let stdin_pipes = pipes.clone();
            let stdin_connections = network_connections.clone();
            let stdin_id = self.node_id;
            println!(
                "About to spawn thread for copying into stdin for node: {:?}",
                self.node_id
            );
            join_handles.push((
                IOType::Stdin,
                spawn(move || {
                    copy_into_stdin(
                        stdin_id,
                        stdin_prog_id,
                        stdin_handle,
                        stdin_streams,
                        stdin_pipes,
                        stdin_connections,
                        tmp,
                    )
                }),
            ));
        }

        // spawn stdout thread
        if self.stdout.len() > 0 {
            // if stdout is PIPED to another process on same machine, do not do this
            // TODO: figure out a solution that works with multiple output streams
            let mut piped_stdout = false;
            for stream in self.stdout.iter() {
                match stream {
                    DashStream::Pipe(_) => {
                        piped_stdout = true;
                    }
                    _ => {}
                }
            }
            if !piped_stdout {
                let stdout_prog_id = self.prog_id;
                let stdout_handle = pipes.remove(&self.get_handle_identifier(IOType::Stdout))?;
                let stdout_streams = self.stdout.clone();
                let stdout_connections = network_connections.clone();
                let stdout_id = self.node_id;
                join_handles.push((
                    IOType::Stdout,
                    spawn(move || {
                        copy_stdout(
                            stdout_id,
                            stdout_prog_id,
                            stdout_handle,
                            stdout_streams,
                            stdout_connections,
                        )
                    }),
                ));
            }
        }

        // spawn stderr threads
        if self.stderr.len() > 0 {
            let mut piped_stderr = false;
            for stream in self.stderr.iter() {
                match stream {
                    DashStream::Pipe(_) => {
                        piped_stderr = true;
                    }
                    _ => {}
                }
            }
            if !piped_stderr {
                let stderr_prog_id = self.prog_id;
                let stderr_handle = pipes.remove(&self.get_handle_identifier(IOType::Stderr))?;
                let stderr_streams = self.stderr.clone();
                let stderr_connections = network_connections.clone();
                let stderr_id = self.node_id;
                join_handles.push((
                    IOType::Stderr,
                    spawn(move || {
                        copy_stderr(
                            stderr_id,
                            stderr_prog_id,
                            stderr_handle,
                            stderr_streams,
                            stderr_connections,
                        )
                    }),
                ));
            }
        }

        for (iotype, thread) in join_handles {
            match thread.join() {
                Ok(res) => match res {
                    Ok(_) => {}
                    Err(e) => {
                        bail!("{:?} thread joined with error: {:?}", iotype, e);
                    }
                },
                Err(e) => {
                    bail!("{:?} thread could not join: {:?}", iotype, e);
                }
            }
        }
        Ok(())
    }

    fn get_loc(&self) -> Location {
        self.location.clone()
    }

    /// Resolves both arguments and any file streams.
    fn resolve_args(&mut self, parent_dir: &str) -> Result<()> {
        match self.resolve_file_args(parent_dir) {
            Ok(mut v) => {
                self.resolved_args.append(&mut v);
            }
            Err(e) => bail!("Failed to resolve args: {:?}", e),
        }
        resolve_file_streams(&mut self.stderr, parent_dir)?;
        resolve_file_streams(&mut self.stdout, parent_dir)?;
        resolve_file_streams(&mut self.stdin, parent_dir)?;
        Ok(())
    }
}

fn copy_into_stdin(
    node_id: NodeId,
    prog_id: ProgId,
    handle: OutputHandle,
    stdin_streams: Vec<DashStream>,
    mut pipes: SharedPipeMap,
    mut network_connections: SharedStreamMap,
    tmp_folder: String,
) -> Result<()> {
    let mut metadata = InputStreamMetadata::new(node_id, &tmp_folder, stdin_streams.len());
    println!("In function to copy into stdin for node: {:?}", node_id);
    let stdin_handle_option: Option<ChildStdin> = handle.into();
    let mut stdin_handle = stdin_handle_option.unwrap();
    let mut tmp_handles = metadata.open_files()?;

    for (idx, stream) in stdin_streams.iter().enumerate() {
        // optimization: the output of this stream has already been copied
        if metadata.current() > idx {
            continue;
        }
        println!("Dealing with copying stream: {:?}", stream);
        match stream {
            DashStream::Tcp(netstream) => {
                let mut tcpstream = match network_connections.remove(&netstream) {
                    Ok(s) => s,
                    Err(e) => bail!(
                        "Failed to find tcp stream with info {:?}: {:?}",
                        netstream,
                        e
                    ),
                };
                iterating_redirect(
                    &mut tcpstream,
                    &mut stdin_handle,
                    &mut metadata,
                    idx,
                    &mut tmp_handles,
                )?;
                // insert back into shared map for next iteration of the loop
                network_connections.insert(netstream.clone(), tcpstream)?;
            }
            DashStream::Pipe(pipestream) => {
                let handle_identifier = HandleIdentifier::new(
                    prog_id,
                    pipestream.get_left(),
                    pipestream.get_output_type(),
                );
                let prev_handle = match pipes.remove(&handle_identifier) {
                    Ok(s) => s,
                    Err(e) => bail!(
                        "Failed to find handle with info {:?}: {:?}",
                        handle_identifier,
                        e
                    ),
                };
                let prev_handle_copy = match pipestream.get_output_type() {
                    IOType::Stdout => {
                        let prev_stdout_handle_option: Option<ChildStdout> = prev_handle.into();
                        let mut prev_stdout_handle = prev_stdout_handle_option.unwrap();
                        iterating_redirect(
                            &mut prev_stdout_handle,
                            &mut stdin_handle,
                            &mut metadata,
                            idx,
                            &mut tmp_handles,
                        )?;
                        OutputHandle::Stdout(prev_stdout_handle)
                    }
                    IOType::Stderr => {
                        let prev_stderr_handle_option: Option<ChildStderr> = prev_handle.into();
                        let mut prev_stderr_handle = prev_stderr_handle_option.unwrap();
                        iterating_redirect(
                            &mut prev_stderr_handle,
                            &mut stdin_handle,
                            &mut metadata,
                            idx,
                            &mut tmp_handles,
                        )?;
                        OutputHandle::Stderr(prev_stderr_handle)
                    }
                    IOType::Stdin => {
                        bail!("Pipestream should not have type of Stdin: {:?}", pipestream);
                    }
                };
                pipes.insert(handle_identifier, prev_handle_copy)?;
            }
            _ => {
                bail!("Command stdin should not have stdout or file stream types in input stream list.");
            }
        }
    }
    Ok(())
}

/// Copies stdout from process into the correct stream location.
/// stdout_handle: OutputHandle of type ChildStdout
/// stdout_streams: List of streams the process needs to copy to.
/// network_connections: SharedStreamMap containing the shared tcp connections.
/// Should only be used when the stdout stream redirection is *not* a pipe on the same machine,
/// then the process that takes this process's output from stdin will claim the handle to stdout of
fn copy_stdout(
    node_id: NodeId,
    _prog_id: ProgId,
    stdout_handle: OutputHandle,
    stdout_streams: Vec<DashStream>,
    mut network_connections: SharedStreamMap,
) -> Result<()> {
    let stdout_handle_option: Option<ChildStdout> = stdout_handle.into();
    let mut stdout_handle = stdout_handle_option.unwrap();
    for stream in stdout_streams.iter() {
        match stream {
            DashStream::Tcp(netstream) => {
                let mut tcp_stream = match network_connections.remove(&netstream) {
                    Ok(s) => s,
                    Err(e) => bail!(
                        "Failed to find tcp stream with info {:?}: {:?}",
                        netstream,
                        e
                    ),
                };
                println!(
                    "copying stdout of cmd {:?} into tcp stream {:?}",
                    node_id, netstream
                );
                copy(&mut stdout_handle, &mut tcp_stream)?;
            }
            _ => {
                bail!(
                    "Should not be in copy stdout function unless stream type is TCP connection: {:?}", stream
                );
            }
        }
    }
    Ok(())
}

fn copy_stderr(
    _node_id: NodeId,
    _prog_id: ProgId,
    stderr_handle: OutputHandle,
    stderr_streams: Vec<DashStream>,
    mut network_connections: SharedStreamMap,
) -> Result<()> {
    let stderr_handle_option: Option<ChildStderr> = stderr_handle.into();
    let mut stderr_handle = stderr_handle_option.unwrap();
    for stream in stderr_streams.iter() {
        match stream {
            DashStream::Tcp(netstream) => {
                let mut tcp_stream = match network_connections.remove(&netstream) {
                    Ok(s) => s,
                    Err(e) => bail!(
                        "Failed to find tcp stream with info {:?}: {:?}",
                        netstream,
                        e
                    ),
                };
                copy(&mut stderr_handle, &mut tcp_stream)?;
            }
            _ => {
                bail!("Should not be in copy stderr function unless stream type is TCP connection: {:?}", stream);
            }
        }
    }
    Ok(())
}
