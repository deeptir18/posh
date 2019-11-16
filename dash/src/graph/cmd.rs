use super::rapper::{resolve_file_streams, stream_initiate_filter, Rapper};
use super::{program, stream, Location, Result};
use failure::bail;
use program::ProgId;
use std::io::copy;
use std::process::{Command, Stdio};
use stream::{IOType, SharedPipeMap, SharedStreamMap, Stream, StreamIdentifier, StreamType};
use which::which;
use std::mem::drop;

/// CommandNodes, which have args, are either file streams OR Strings.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum NodeArg {
    Str(String),
    Stream(Stream),
}

/// Node that runs binaries with the provided arguments, at the given location.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct CommandNode {
    /// Name of binary program.
    name: String,
    /// arguments to pass in to the binary
    args: Vec<NodeArg>,
    /// Vector of streams that stdin comes from, in serialized order.
    stdin: Vec<Stream>,
    /// Vector of streams to send stdout of this program on.
    stdout: Vec<Stream>,
    /// Vector of streams to send stderr of this program on.
    stderr: Vec<Stream>,
    /// Execution location for the node.
    location: Location,
    /// Resolved args, as strings.
    resolved_args: Vec<String>,
}

impl Default for CommandNode {
    fn default() -> Self {
        CommandNode {
            name: Default::default(),
            args: vec![],
            stdin: vec![],
            stdout: vec![],
            stderr: vec![],
            location: Default::default(),
            resolved_args: vec![],
        }
    }
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

    /// Takes the args and converts the file stream args to strings to pass in.
    fn resolve_file_args(&mut self, parent_dir: &str) -> Result<Vec<String>> {
        let mut arg_iterator = self.args.iter_mut();
        let mut ret: Vec<String> = Vec::new();
        while let Some(arg) = arg_iterator.next() {
            match arg {
                NodeArg::Stream(s) => match s.get_type() {
                    StreamType::File(_) => {
                        let resolved_file = s.prepend_directory(parent_dir)?;
                        ret.push(resolved_file);
                    }
                    _ => {
                        unimplemented!();
                    }
                },
                NodeArg::Str(a) => {
                    ret.push(a.clone());
                }
            }
        }
        Ok(ret)
    }
}

impl Rapper for CommandNode {
    fn get_outward_streams(
        &self,
        prog_id: ProgId,
        iotype: IOType,
        is_server: bool,
    ) -> Vec<(Location, StreamIdentifier)> {
        let streams: Vec<Stream> = match iotype {
            IOType::Stdin => self
                .stdin
                .iter()
                .filter(|&s| stream_initiate_filter(s.clone(), is_server))
                .cloned()
                .collect(),
            IOType::Stdout => self
                .stdout
                .iter()
                .filter(|&s| stream_initiate_filter(s.clone(), is_server))
                .cloned()
                .collect(),
            IOType::Stderr => self
                .stderr
                .iter()
                .filter(|&s| stream_initiate_filter(s.clone(), is_server))
                .cloned()
                .collect(),
        };
        streams
            .iter()
            .map(|s| {
                let loc = s.get_network_connection().unwrap();
                (loc, StreamIdentifier::new(prog_id, s.clone(), iotype))
            })
            .collect()
    }
    fn get_stdin(&self) -> Vec<stream::Stream> {
        self.stdin.clone()
    }

    fn get_stdout(&self) -> Vec<stream::Stream> {
        self.stdout.clone()
    }

    fn get_stderr(&self) -> Vec<stream::Stream> {
        self.stderr.clone()
    }
    fn add_stdin(&mut self, stream: Stream) -> Result<()> {
        self.stdin.push(stream);
        Ok(())
    }
    fn add_stdout(&mut self, stream: Stream) -> Result<()> {
        self.stdout.push(stream);
        Ok(())
    }

    fn add_stderr(&mut self, stream: Stream) -> Result<()> {
        self.stderr.push(stream);
        Ok(())
    }

    fn execute(
        &mut self,
        pipes: SharedPipeMap,
        network_connections: SharedStreamMap,
        prog_id: ProgId,
    ) -> Result<()> {
        let mut cmd = Command::new(self.name.clone()).args(self.resolved_args.clone());
        // TODO: can we just define the command with everything piped?
        if self.stdin.len() > 0 {
            cmd = cmd.stdin(Stdio::piped());
        }
        if self.stdout.len() > 0 {
            cmd = cmd.stdout(Stdio::piped());
        }
        if self.stderr.len() > 0 {
            cmd = cmd.stderr(Stdio::piped());
        }
        let mut stdin_handle = match cmd.stdin {
            Some(h) => h,
            Err
        }
        // 2: copy all stdin from the correct processes in the process map
        for stream in self.stdin.iter() {
            match stream.get_type() {
                StreamType::TcpConnection(loc) => {
                    let stream_identifier = StreamIdentifier::new(prog_id, stream, IOType::Stdout);
                    let map = match network_connections.0.lock() {
                        Ok(m) => m,
                        Err(e) => bail!("Lock is poisoned: {:?}", e);
                    };
                    let tcp_stream = match map.get_mut(stream_identifier) {
                        Some(t) => t,
                        None => { bail!("No shared stream for stream identifier {:?}", stream_identifier); }
                    };
                    let mut tcp_stream_copy = tcp_stream.try_clone()?;
                    drop(map);
                    copy(&mut tcp_stream_copy, &mut stdin_handle);
                }
                StreamType::Piped => {}
                _ => {
                    println!("Stdin should not have stdout or file stream types");
                }
            }
        }
        // 3: copy stderr and stdout to either:
        // (a) the correct tcp stream from the SharedStreamMap
        // (b) copy the child process *completely* into the SharedStreamMap
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
