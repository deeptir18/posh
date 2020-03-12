use super::execute::Execute;
use super::filestream::FileStream;
use super::info::{resolve_file_stream_option, resolve_file_streams, Info};
use super::pipe::{
    create_and_insert_channels, create_buffer_file, get_channel_name, BufferedPipe, PipeMode,
    SharedChannelMap,
};
use super::rapper::copy_wrapper as copy;
use super::rapper::stream_initiate_filter;
use super::{program, stream, Location, Result};
use failure::bail;
use itertools::join;
use program::{Link, NodeId, ProgId};
use std::path::{Path, PathBuf};
use std::process::{ChildStdin, Command, Stdio};
use std::slice::IterMut;
use std::thread;
use stream::{
    DashStream, HandleIdentifier, IOType, NetStream, OutputHandle, PipeStream, SharedPipeMap,
    SharedStreamMap,
};
use thread::{spawn, JoinHandle};
use tracing::{debug, error};
use which::which;

/// CommandNodes, which have args, are either file streams OR Strings.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum NodeArg {
    Str(String),
    Stream(FileStream),
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

/// Node that runs binaries with the provided arguments at the given location.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default)]
pub struct CommandNode {
    /// Id within the program.
    node_id: NodeId,
    /// Id of the program.
    prog_id: ProgId,
    /// Name of the binary program.
    name: String,
    /// arguments to pass into the binary
    args: Vec<NodeArg>,
    /// resolved arguments
    resolved_args: Vec<String>,
    /// Input streams in serialized order.
    stdin: Vec<DashStream>,
    /// Optional output stream for stdout.
    stdout: Option<DashStream>,
    /// Optional output stream for stderr.
    stderr: Option<DashStream>,
    /// Execution location for the node.
    location: Location,
    /// Extra information relevant for scheduling
    options: CmdExtraInfo,
    /// PWD for executing the command.
    pwd: PathBuf,
}

impl CommandNode {
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

    pub fn get_pwd(&self) -> PathBuf {
        self.pwd.clone()
    }

    pub fn set_pwd(&mut self, path: &Path) {
        self.pwd = path.to_path_buf();
    }

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
        self.stdout = None;
    }

    pub fn clear_stderr(&mut self) {
        self.stderr = None;
    }

    pub fn get_stdin_iter_mut(&mut self) -> IterMut<DashStream> {
        self.stdin.iter_mut()
    }

    pub fn get_stdout_mut(&mut self) -> &mut Option<DashStream> {
        &mut self.stdout
    }

    pub fn get_stderr_mut(&mut self) -> &mut Option<DashStream> {
        &mut self.stderr
    }

    pub fn arg_locations(&self) -> Vec<Location> {
        self.args
            .clone()
            .iter()
            .filter_map(|arg| match arg {
                NodeArg::Str(_) => None,
                NodeArg::Stream(fs) => Some(fs.get_location()),
            })
            .collect()
    }

    pub fn get_string_args(&self) -> Vec<String> {
        self.args
            .clone()
            .iter()
            .filter_map(|arg| match arg {
                NodeArg::Str(s) => Some(s.clone()),
                NodeArg::Stream(_) => None,
            })
            .collect()
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

    /// Change stream to reflect how one of the edges has changed.
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
                if let Some(ref mut stream) = self.stdout {
                    replaced = stream_repl(stream, &edge, &new_edge, false);
                }
                if let Some(ref mut stream) = self.stderr {
                    if !replaced {
                        stream_repl(stream, &edge, &new_edge, false);
                    }
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

    /// TODO: edit this function to make it use pathbufs instead of strings
    /// E.g. change parent_dir to not be a protobuf
    /// Then it might not have to also even return a result?
    fn resolve_file_args(&mut self, parent_dir: &Path) -> Result<Vec<String>> {
        self.args
            .clone()
            .iter_mut()
            .map(|arg| match arg {
                NodeArg::Stream(ref mut fs) => {
                    fs.prepend_directory(parent_dir);
                    fs.get_name()
                }
                NodeArg::Str(a) => Ok(a.to_string()),
            })
            .collect()
    }
}

impl Info for CommandNode {
    fn set_id(&mut self, id: NodeId) {
        self.node_id = id;
    }

    fn get_id(&self) -> NodeId {
        self.node_id
    }

    fn get_loc(&self) -> Location {
        self.location.clone()
    }

    fn set_loc(&mut self, loc: Location) {
        self.location = loc;
    }

    fn get_stdin(&self) -> Vec<DashStream> {
        self.stdin.clone()
    }

    fn get_stdout(&self) -> Option<DashStream> {
        self.stdout.clone()
    }

    fn get_stdout_id(&self) -> Option<NodeId> {
        match &self.stdout {
            Some(dashstream) => match dashstream {
                DashStream::Pipe(ps) => Some(ps.get_left()),
                DashStream::Tcp(ts) => Some(ts.get_left()),
                _ => None,
            },
            None => None,
        }
    }

    fn get_stderr(&self) -> Option<DashStream> {
        self.stderr.clone()
    }

    fn get_stdin_len(&self) -> usize {
        self.stdin.len()
    }

    fn get_stdout_len(&self) -> usize {
        match self.stdout {
            Some(_) => 1,
            None => 0,
        }
    }

    fn get_stderr_len(&self) -> usize {
        match self.stderr {
            Some(_) => 1,
            None => 0,
        }
    }

    fn add_stdin(&mut self, stream: DashStream) -> Result<()> {
        match stream {
            DashStream::Pipe(_) => {}
            DashStream::Tcp(_) => {}
            _ => {
                bail!(
                    "Cannot have stream of type {:?} as input to command node",
                    stream
                );
            }
        }
        self.stdin.push(stream);
        Ok(())
    }

    fn set_stdout(&mut self, stream: DashStream) -> Result<()> {
        match stream {
            DashStream::Pipe(_) => {}
            DashStream::Tcp(_) => {}
            _ => {
                bail!(
                    "Cannot have stream of type {:?} as stdout to command node",
                    stream
                );
            }
        }
        if let Some(current) = &self.stdout {
            debug!(
                "Setting stdout on {:?} as {:?}, but stdout is already set to: {:?}",
                self.node_id, stream, current
            );
        }
        self.stdout = Some(stream);
        Ok(())
    }

    fn set_stderr(&mut self, stream: DashStream) -> Result<()> {
        match stream {
            DashStream::Pipe(_) => {}
            DashStream::Tcp(_) => {}
            _ => {
                bail!(
                    "Cannot have stream of type {:?} as stderr to command node",
                    stream
                );
            }
        }
        if let Some(current) = &self.stderr {
            debug!(
                "Setting stderr on {:?} as {:?}, but stderr is already set to: {:?}",
                self.node_id, stream, current
            );
        }
        self.stderr = Some(stream);
        Ok(())
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

    fn resolve_args(&mut self, parent_dir: PathBuf) -> Result<()> {
        if self.options.get_needs_current_dir() {
            let parent = Path::new(&parent_dir).join(self.pwd.as_path());
            self.pwd = parent.to_path_buf();
        }

        match self.resolve_file_args(parent_dir.as_path()) {
            Ok(mut v) => {
                self.resolved_args.append(&mut v);
            }
            Err(e) => {
                bail!("Failed to resolve args: {:?}", e);
            }
        }
        resolve_file_streams(&mut self.stdin, parent_dir.as_path());
        resolve_file_stream_option(&mut self.stdout, parent_dir.as_path());
        resolve_file_stream_option(&mut self.stderr, parent_dir.as_path());
        Ok(())
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
                let mut streams: Vec<DashStream> = Vec::new();
                match &self.stdout {
                    Some(stdout) => streams.push(stdout.clone()),
                    None => {}
                }
                match &self.stderr {
                    Some(stderr) => streams.push(stderr.clone()),
                    None => {}
                }
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
                for (iotype, stream) in streams_to_add.iter() {
                    match iotype {
                        IOType::Stdout => {
                            self.stdout = Some(stream.clone());
                        }
                        IOType::Stderr => {
                            self.stderr = Some(stream.clone());
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
    /// Modify the pipe to be a netstream.
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
                self.stdin.push(DashStream::Tcp(net));
            }
            IOType::Stdout => {
                if let Some(_stream) = &self.stdout {
                    self.stdout = Some(DashStream::Tcp(net));
                } else {
                    bail!("Trying to replace stdout pipestream with net but node has no stdout");
                }
            }
            IOType::Stderr => {
                if let Some(_stream) = &self.stderr {
                    self.stderr = Some(DashStream::Tcp(net));
                } else {
                    bail!("Trying to replace stderr pipestream with net but node has no stderr");
                }
            }
        }
        Ok(())
    }

    fn get_outward_streams(&self, iotype: IOType, is_server: bool) -> Vec<NetStream> {
        let streams: Vec<DashStream> = match iotype {
            IOType::Stdin => self
                .stdin
                .iter()
                .filter(|&s| stream_initiate_filter(s.clone(), self.node_id, is_server))
                .cloned()
                .collect(),
            IOType::Stdout => match &self.stdout {
                Some(stream) => {
                    if stream_initiate_filter(stream.clone(), self.node_id, is_server) {
                        vec![stream.clone()]
                    } else {
                        vec![]
                    }
                }
                None => vec![],
            },
            IOType::Stderr => match &self.stderr {
                Some(stream) => {
                    if stream_initiate_filter(stream.clone(), self.node_id, is_server) {
                        vec![stream.clone()]
                    } else {
                        vec![]
                    }
                }
                None => vec![],
            },
        };
        streams
            .iter()
            .map(|s| {
                let netstream_result: Option<NetStream> = s.clone().into();
                netstream_result.unwrap()
            })
            .collect()
    }
}

impl Execute for CommandNode {
    fn spawn(
        &mut self,
        mut pipes: SharedPipeMap,
        _network_connections: SharedStreamMap,
        mut channels: SharedChannelMap,
        tmp_folder: PathBuf,
    ) -> Result<()> {
        let mut cmd = Command::new(self.name.clone());
        cmd.args(self.resolved_args.clone());

        if self.stdin.len() > 0 {
            debug!(
                "Setting stdin for node {:?} to be Stdio::Piped",
                self.node_id
            );
            cmd.stdin(Stdio::piped());
        }

        if let Some(stdout) = &self.stdout {
            // need to create the buffer file pipe for the reader and writer to access later
            debug!(
                "Setting stdout for node {:?} to be Stdio::Piped",
                self.node_id
            );
            match stdout {
                DashStream::Tcp(netstream) => {
                    if netstream.get_bufferable() {
                        create_buffer_file(tmp_folder.as_path(), self.node_id, IOType::Stdout)?;
                        create_and_insert_channels(self.node_id, IOType::Stdout, &mut channels)?;
                    }
                    cmd.stdout(Stdio::piped());
                }
                DashStream::Pipe(pipestream) => {
                    if pipestream.get_bufferable() {
                        create_buffer_file(tmp_folder.as_path(), self.node_id, IOType::Stdout)?;
                        create_and_insert_channels(self.node_id, IOType::Stdout, &mut channels)?;
                    }
                    cmd.stdout(Stdio::piped());
                }
                _ => {
                    cmd.stdout(Stdio::piped());
                }
            }
        }

        if let Some(stream) = &self.stderr {
            debug!("Setting stderr for {:?} to be Stdio::Piped", self.node_id);
            match stream {
                DashStream::Tcp(netstream) => {
                    if netstream.get_bufferable() {
                        create_buffer_file(tmp_folder.as_path(), self.node_id, IOType::Stderr)?;
                        create_and_insert_channels(self.node_id, IOType::Stderr, &mut channels)?;
                    }
                    cmd.stderr(Stdio::piped());
                }
                DashStream::Pipe(pipestream) => {
                    if pipestream.get_bufferable() {
                        create_buffer_file(tmp_folder.as_path(), self.node_id, IOType::Stderr)?;
                        create_and_insert_channels(self.node_id, IOType::Stderr, &mut channels)?;
                    }
                    cmd.stderr(Stdio::piped());
                }
                _ => {
                    cmd.stderr(Stdio::piped());
                }
            }
        }

        let child = cmd.spawn().expect("Failed to spawn child");
        let stdin_handle = child.stdin.expect("Could not get stdin handle for proc");
        pipes.insert(
            self.get_handle_identifier(IOType::Stdin),
            OutputHandle::Stdin(stdin_handle),
        )?;
        let stdout_handle = child.stdout.expect("Could not get stdout handle for proc");
        pipes.insert(
            self.get_handle_identifier(IOType::Stdout),
            OutputHandle::Stdout(stdout_handle),
        )?;
        let stderr_handle = child.stderr.expect("Could not get stderr handle for proc");
        pipes.insert(
            self.get_handle_identifier(IOType::Stderr),
            OutputHandle::Stderr(stderr_handle),
        )?;

        Ok(())
    }

    fn redirect(
        &mut self,
        mut pipes: SharedPipeMap,
        network_connections: SharedStreamMap,
        channels: SharedChannelMap,
        tmp_folder: PathBuf,
    ) -> Result<()> {
        let mut threads: Vec<(IOType, JoinHandle<Result<()>>)> = Vec::new();

        // spawn a stdin thread to handle the input
        if self.stdin.len() > 0 {
            let stdin_prog_id = self.prog_id;
            let stdin_handle = pipes.remove(&self.get_handle_identifier(IOType::Stdin))?;
            let stdin_streams = self.stdin.clone();
            let stdin_id = self.node_id;
            let pipes_clone = pipes.clone();
            let network_connections_clone = network_connections.clone();
            let channels_clone = channels.clone();
            let tmp_folder_clone = tmp_folder.clone();
            debug!(
                "Spawning thread for copying stdin into node {:?}",
                self.node_id
            );
            threads.push((
                IOType::Stdin,
                spawn(move || {
                    redirect_stdin(
                        stdin_id,
                        stdin_prog_id,
                        stdin_handle,
                        stdin_streams,
                        pipes_clone,
                        network_connections_clone,
                        channels_clone,
                        tmp_folder_clone,
                    )
                }),
            ));
        }

        // spawn a stdout thread to handle sending the stdout
        if let Some(stream) = &self.stdout {
            let stdout_id = self.node_id;
            let stdout_prog_id = self.prog_id;
            let stream_clone = stream.clone();
            let pipes_clone = pipes.clone();
            let network_connections_clone = network_connections.clone();
            let channels_clone = channels.clone();
            let tmp_folder_clone = tmp_folder.clone();
            debug!(
                "Spawning thread to copy stdout from a node {:?}",
                self.node_id
            );
            threads.push((
                IOType::Stdout,
                spawn(move || {
                    redirect_output(
                        stdout_id,
                        stdout_prog_id,
                        stream_clone,
                        pipes_clone,
                        network_connections_clone,
                        channels_clone,
                        tmp_folder_clone,
                        IOType::Stdout,
                    )
                }),
            ));
        }

        // spawn a stderr thread to handle sending the stderr
        if let Some(stream) = &self.stderr {
            let id = self.node_id;
            let prog = self.prog_id;
            let stream_clone = stream.clone();
            let pipes_clone = pipes.clone();
            let network_connections_clone = network_connections.clone();
            let channels_clone = channels.clone();
            let tmp_folder_clone = tmp_folder.clone();
            debug!(
                "Spawning thread to copy stderr from a node {:?}",
                self.node_id
            );
            threads.push((
                IOType::Stderr,
                spawn(move || {
                    redirect_output(
                        id,
                        prog,
                        stream_clone,
                        pipes_clone,
                        network_connections_clone,
                        channels_clone,
                        tmp_folder_clone,
                        IOType::Stderr,
                    )
                }),
            ));
        }

        // join all the threads
        for (iotype, thread) in threads {
            match thread.join() {
                Ok(res) => match res {
                    Ok(_) => {}
                    Err(e) => {
                        error!(
                            "Node {:?} {:?} thread joined with an error: {:?}",
                            self.node_id, iotype, e
                        );
                        bail!(
                            "Node {:?} {:?} thread joined with an error: {:?}",
                            self.node_id,
                            iotype,
                            e
                        );
                    }
                },
                Err(e) => {
                    error!(
                        "Node {:?} {:?} thread could not join with an error: {:?}",
                        self.node_id, iotype, e
                    );
                    bail!(
                        "Node {:?} {:?} thread could not join with an error: {:?}",
                        self.node_id,
                        iotype,
                        e
                    );
                }
            }
        }

        Ok(())
    }
}

fn redirect_stdin(
    node_id: NodeId,
    prog_id: ProgId,
    stdin_handle: OutputHandle,
    stdin_streams: Vec<DashStream>,
    mut pipes: SharedPipeMap,
    mut network_connections: SharedStreamMap,
    mut channels: SharedChannelMap,
    tmp_folder: PathBuf,
) -> Result<()> {
    let stdin_handle_option: Option<ChildStdin> = stdin_handle.into();
    let mut stdin = stdin_handle_option.unwrap();
    for input_stream in stdin_streams.iter() {
        match input_stream {
            DashStream::Tcp(netstream) => {
                let mut tcpstream = network_connections.remove(&netstream)?;
                copy(&mut tcpstream, &mut stdin)?;
            }
            DashStream::Pipe(pipestream) => {
                if pipestream.get_bufferable() {
                    let channel_end = channels.remove(&get_channel_name(
                        node_id,
                        PipeMode::Read,
                        pipestream.get_output_type(),
                    ))?;
                    // copy from the buffer file, not the process
                    let mut buffered_pipe = BufferedPipe::new(
                        node_id,
                        pipestream.get_output_type(),
                        tmp_folder.as_path(),
                        PipeMode::Read,
                        channel_end,
                    )?;
                    copy(&mut buffered_pipe, &mut stdin)?;
                } else {
                    // just copy from the process directly as normal
                    let handle_identifier = HandleIdentifier::new(
                        prog_id,
                        pipestream.get_left(),
                        pipestream.get_output_type(),
                    );
                    let mut prev_handle = pipes.remove(&handle_identifier)?;
                    copy(&mut prev_handle, &mut stdin)?;
                }
            }
            _ => {
                bail!("Command node should not see input from file, stdout, or stderr stream handle: {:?}", input_stream);
            }
        }
    }
    Ok(())
}

fn redirect_output(
    node_id: NodeId,
    prog_id: ProgId,
    stream: DashStream,
    mut pipes: SharedPipeMap,
    mut network_connections: SharedStreamMap,
    mut channels: SharedChannelMap,
    tmp_folder: PathBuf,
    iotype: IOType,
) -> Result<()> {
    // todo: would be better to copy directly into the buffered pipe file, and just have a separate
    // process waiting on knowing that the writing is done
    match stream.clone() {
        DashStream::Tcp(netstream) => {
            let mut tcp_stream = match network_connections.remove(&netstream) {
                Ok(s) => s,
                Err(e) => {
                    bail!(
                        "Failed to find tcp stream with info {:?}: {:?}",
                        netstream,
                        e
                    );
                }
            };
            if netstream.get_bufferable() {
                // spawn the write thread
                let left_channel =
                    channels.remove(&get_channel_name(node_id, PipeMode::Write, iotype))?;
                let right_channel =
                    channels.remove(&get_channel_name(node_id, PipeMode::Read, iotype))?;
                let mut left_pipe =
                    BufferedPipe::new(node_id, iotype, &tmp_folder, PipeMode::Write, left_channel)?;
                let mut right_pipe =
                    BufferedPipe::new(node_id, iotype, &tmp_folder, PipeMode::Read, right_channel)?;
                let mut handle = pipes.remove(&HandleIdentifier::new(prog_id, node_id, iotype))?;
                let copy_thread: JoinHandle<Result<()>> = spawn(move || {
                    copy(&mut handle, &mut left_pipe)?;
                    left_pipe.set_write_done()?;
                    Ok(())
                });

                // spawn the copy into tcp connection thread
                let send_thread: JoinHandle<Result<()>> = spawn(move || {
                    copy(&mut right_pipe, &mut tcp_stream)?;
                    Ok(())
                });

                match copy_thread.join() {
                    Ok(res) => match res {
                        Ok(_) => {}
                        Err(e) => {
                            bail!("Thread to copy {:?} handle {:?} into left side of buffer failed: {:?}", iotype, node_id, e);
                        }
                    },
                    Err(e) => {
                        bail!("thread to copy {:?} handle {:?} into the buffered pipe failed to join: {:?}", iotype, node_id, e);
                    }
                }

                match send_thread.join() {
                    Ok(res) => match res {
                        Ok(_) => {}
                        Err(e) => {
                            bail!("Thread to send {:?} handle for node {:?} into tcp stream failed: {:?}", iotype, node_id, e);
                        }
                    },
                    Err(e) => {
                        bail!("thread to send {:?} handle into tcp stream for node {:?} failed to join: {:?}", iotype, node_id, e);
                    }
                }
            } else {
                // directly copy the stdout into the connection without any intermediate buffering
                let mut handle = pipes.remove(&HandleIdentifier::new(prog_id, node_id, iotype))?;
                copy(&mut handle, &mut tcp_stream)?;
            }
        }
        DashStream::Pipe(pipestream) => {
            if pipestream.get_bufferable() {
                // need to copy stdout of the command into the buffered pipe
                let mut stdout_handle =
                    pipes.remove(&HandleIdentifier::new(prog_id, node_id, iotype))?;
                let channel_end =
                    channels.remove(&get_channel_name(node_id, PipeMode::Write, iotype))?;
                let mut buffered_pipe = BufferedPipe::new(
                    node_id,
                    iotype,
                    tmp_folder.as_path(),
                    PipeMode::Write,
                    channel_end,
                )?;
                copy(&mut stdout_handle, &mut buffered_pipe)?;
                buffered_pipe.set_write_done()?;
            }
        }
        _ => {}
    }
    Ok(())
}
