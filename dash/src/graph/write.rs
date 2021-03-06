use super::rapper::iterating_redirect;
use super::rapper::{resolve_file_streams, stream_initiate_filter, InputStreamMetadata, Rapper};
use super::{program, stream, Location, Result};
use failure::bail;
use itertools::join;
use program::{Link, NodeId, ProgId};
use std::collections::HashMap;
use std::io::{stderr, stdout};
use std::mem::drop;
use std::net::TcpStream;
use std::path::Path;
use std::slice::IterMut;
use stream::{
    DashStream, HandleIdentifier, IOType, NetStream, OutputHandle, PipeStream, SharedPipeMap,
    SharedStreamMap,
};
use tracing::debug;
/// Node that writes stdin to a specified file.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default)]
pub struct WriteNode {
    /// Id within the program.
    node_id: NodeId,
    /// Id of the program.
    prog_id: ProgId,
    /// Input streams to write node.
    stdin: Vec<DashStream>,
    /// Output streams (note: must be file streams).
    output: Vec<DashStream>,
    /// Execution location of the read node.
    location: Location,
}

impl WriteNode {
    pub fn get_stdout_iter_mut(&mut self) -> IterMut<DashStream> {
        self.output.iter_mut()
    }
    pub fn get_stdin_iter_mut(&mut self) -> IterMut<DashStream> {
        self.stdin.iter_mut()
    }
    pub fn get_output_locations(&self) -> Vec<Location> {
        let mut ret: Vec<Location> = Vec::new();
        for stream in self.output.iter() {
            match stream {
                DashStream::File(fs) => ret.push(fs.get_location()),
                DashStream::Stdout => ret.push(Location::Client),
                DashStream::Stderr => ret.push(Location::Client),
                _ => {}
            }
        }
        ret
    }
}
impl Rapper for WriteNode {
    fn set_id(&mut self, id: NodeId) {
        self.node_id = id;
    }

    fn get_id(&self) -> NodeId {
        self.node_id
    }

    fn get_dot_label(&self) -> Result<String> {
        let outputs: Result<Vec<String>> = self
            .output
            .iter()
            .map(|stream| stream.get_dot_label())
            .collect();
        match outputs {
            Ok(o) => Ok(format!(
                "{}: {}\nloc: {:?}",
                self.node_id,
                join(o, "\n\n"),
                self.location
            )),
            Err(e) => bail!("{:?}", e),
        }
    }

    fn replace_stream_edges(&mut self, edge: Link, new_edges: Vec<Link>) -> Result<()> {
        if self.node_id != edge.get_right() {
            bail!("Calling replace stream edges on write node where edge right is NOT node id, id: {:?}, edge: {:?}", self.node_id, edge);
        } else {
            let mut streams_to_remove: Vec<DashStream> = Vec::new();
            let mut streams_to_add: Vec<DashStream> = Vec::new();
            for stream in self.stdin.iter() {
                match stream {
                    DashStream::Pipe(pipestream) => {
                        if pipestream.get_right() == edge.get_right() {
                            streams_to_remove.push(DashStream::Pipe(pipestream.clone()));
                            for new_edge in new_edges.iter() {
                                let mut new_pipestream = pipestream.clone();
                                new_pipestream.set_left(new_edge.get_left());
                                streams_to_add.push(DashStream::Pipe(new_pipestream));
                            }
                        }
                    }
                    DashStream::Tcp(netstream) => {
                        if netstream.get_right() == edge.get_right() {
                            streams_to_remove.push(DashStream::Tcp(netstream.clone()));
                            for new_edge in new_edges.iter() {
                                let mut new_netstream = netstream.clone();
                                new_netstream.set_left(new_edge.get_left());
                                streams_to_add.push(DashStream::Tcp(new_netstream));
                            }
                        }
                    }
                    _ => {
                        unreachable!();
                    }
                }
            }
            assert!(streams_to_remove.len() == 1);
            self.stdin.retain(|x| !streams_to_remove.contains(x));
            self.stdin.append(&mut streams_to_add);
        }

        Ok(())
    }

    /// Write nodes have no "stdouts" - should always be a sink.
    fn get_stdout_id(&self) -> Option<NodeId> {
        return None;
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
                bail!("No pipe stdout for write");
            }
            IOType::Stderr => {
                bail!("No pipe stdout for write");
            }
        }
        Ok(())
    }

    fn set_loc(&mut self, loc: Location) {
        self.location = loc;
    }

    fn get_outward_streams(&self, iotype: IOType, is_server: bool) -> Vec<NetStream> {
        // Only look at stdin streams; output MUST be a file on the same machine.
        let streams: Vec<DashStream> = match iotype {
            IOType::Stdin => self
                .stdin
                .clone()
                .iter()
                .filter(|&s| stream_initiate_filter(s.clone(), self.node_id, is_server))
                .cloned()
                .collect(),
            _ => Vec::new(),
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
        self.output.len()
    }

    fn get_stderr_len(&self) -> usize {
        0
    }
    fn get_stdin(&self) -> Vec<DashStream> {
        self.stdin.clone()
    }

    fn get_stdout(&self) -> Vec<DashStream> {
        self.output.clone()
    }

    fn get_stderr(&self) -> Vec<DashStream> {
        vec![]
    }

    fn add_stdin(&mut self, stream: DashStream) -> Result<()> {
        self.stdin.push(stream);
        Ok(())
    }
    fn add_stdout(&mut self, stream: DashStream) -> Result<()> {
        match stream {
            DashStream::File(fs) => {
                self.output.push(DashStream::File(fs));
            }
            DashStream::Stdout => {
                self.output.push(DashStream::Stdout);
            }
            DashStream::Stderr => {
                self.output.push(DashStream::Stderr);
            }
            _ => bail!(
                "Adding stdout to write node that is not a file stream: {:?},",
                stream
            ),
        }
        Ok(())
    }

    fn add_stderr(&mut self, stream: DashStream) -> Result<()> {
        // TODO: is this okay?
        self.output.push(stream);
        Ok(())
    }

    fn run_redirection(
        &mut self,
        mut pipes: SharedPipeMap,
        mut network_connections: SharedStreamMap,
        tmp_folder: String,
    ) -> Result<()> {
        let mut metadata = InputStreamMetadata::new(self.node_id, &tmp_folder, self.stdin.len());
        let mut tmp_handles = metadata.open_files()?;
        // now -> we don't do broadcast so there is only 1 output stream per write node
        let output_stream = match self.output.pop() {
            Some(os) => os,
            None => {
                bail!(
                    "Write node {:?} does not have an output stream",
                    self.node_id
                );
            }
        };
        match output_stream.clone() {
            DashStream::File(filestream) => {
                let f = filestream.open()?;
                drop(f);
            }
            _ => {}
        }

        // pop all the individual streams so we don't need to access the shared hashmap again
        let mut input_pipestreams: HashMap<usize, OutputHandle> = HashMap::default();
        let mut input_tcpstreams: HashMap<usize, TcpStream> = HashMap::default();
        for (idx, input_stream) in self.stdin.iter().enumerate() {
            match input_stream {
                DashStream::Tcp(netstream) => {
                    let tcpstream = network_connections.remove(&netstream)?;
                    input_tcpstreams.insert(idx, tcpstream);
                }
                DashStream::Pipe(pipestream) => {
                    let handle_identifier = HandleIdentifier::new(
                        self.prog_id,
                        pipestream.get_left(),
                        pipestream.get_output_type(),
                    );
                    let output_handle = pipes.remove(&handle_identifier)?;
                    input_pipestreams.insert(idx, output_handle);
                }
                _ => {
                    bail!("Write node should not see input from a file, stdout, or stderr handle: {:?}", input_stream);
                }
            }
        }
        while metadata.current() < self.stdin.len() {
            for (idx, stream) in self.stdin.iter().enumerate() {
                // optimization: the output of this stream has already been copied
                if metadata.current() > idx {
                    continue;
                }

                match stream {
                    DashStream::Tcp(_netstream) => {
                        let mut tcpstream = input_tcpstreams.get_mut(&idx).unwrap();
                        match output_stream.clone() {
                            DashStream::File(filestream) => {
                                // right now, hack. Fix this properly later by passing an option to
                                // iterating_redirect
                                if self.stdin.len() == 1 {
                                    debug!("setting up non blocking FALSE");
                                    tcpstream.set_nonblocking(false)?;
                                }
                                // need to open this in APPEND mode
                                let mut file_handle = filestream.open_with_append()?;
                                iterating_redirect(
                                    &mut tcpstream,
                                    &mut file_handle,
                                    &mut metadata,
                                    idx,
                                    &mut tmp_handles,
                                    self.node_id,
                                )?;
                                drop(file_handle);
                            }
                            DashStream::Stdout => {
                                // right now, hack. Fix this properly later by passing an option to
                                // iterating_redirect
                                if self.stdin.len() == 1 {
                                    debug!("setting non blocking");
                                    tcpstream.set_nonblocking(false)?;
                                }
                                iterating_redirect(
                                    &mut tcpstream,
                                    &mut stdout(),
                                    &mut metadata,
                                    idx,
                                    &mut tmp_handles,
                                    self.node_id,
                                )?;
                            }
                            DashStream::Stderr => {
                                // right now, hack. Fix this properly later by passing an option to
                                // iterating_redirect
                                if self.stdin.len() == 1 {
                                    debug!("setting non blocking");
                                    tcpstream.set_nonblocking(false)?;
                                }
                                iterating_redirect(
                                    &mut tcpstream,
                                    &mut stderr(),
                                    &mut metadata,
                                    idx,
                                    &mut tmp_handles,
                                    self.node_id,
                                )?;
                            }
                            _ => {
                                bail!("Output stream is not of type file, stdout or stderr handle: {:?}", output_stream);
                            }
                        }
                    }
                    DashStream::Pipe(pipestream) => {
                        let mut output_handle = input_pipestreams.get_mut(&idx).unwrap();
                        if self.stdin.len() == 1 {
                            debug!(
                                "Running write stdin function for node {:?} for pipe {:?}",
                                self.node_id, pipestream
                            );
                        }
                        match output_stream.clone() {
                            DashStream::File(filestream) => {
                                let mut file_handle = filestream.open_with_append()?;
                                iterating_redirect(
                                    &mut output_handle,
                                    &mut file_handle,
                                    &mut metadata,
                                    idx,
                                    &mut tmp_handles,
                                    self.node_id,
                                )?;
                                drop(file_handle);
                            }
                            DashStream::Stdout => {
                                iterating_redirect(
                                    &mut output_handle,
                                    &mut stdout(),
                                    &mut metadata,
                                    idx,
                                    &mut tmp_handles,
                                    self.node_id,
                                )?;
                            }
                            DashStream::Stderr => {
                                iterating_redirect(
                                    &mut output_handle,
                                    &mut stderr(),
                                    &mut metadata,
                                    idx,
                                    &mut tmp_handles,
                                    self.node_id,
                                )?;
                            }
                            _ => {
                                bail!("Output stream is not of type file, stdout or stderr handle: {:?}", output_stream);
                            }
                        }
                    }
                    _ => {
                        bail!("Write node should not see input from a file, stdout, or stderr handle: {:?}", stream);
                    }
                }
            }
        }
        metadata.remove_files()?;
        Ok(())
    }

    fn execute(
        &mut self,
        _pipes: SharedPipeMap,
        _network_connections: SharedStreamMap,
    ) -> Result<()> {
        // Noop: a write node just writes the output of streams into files
        // Nothing needs to be spawned beforehand.
        Ok(())
    }

    fn get_loc(&self) -> Location {
        self.location.clone()
    }

    fn resolve_args(&mut self, parent_dir: &str) -> Result<()> {
        resolve_file_streams(&mut self.stdin, &Path::new(parent_dir));
        resolve_file_streams(&mut self.output, &Path::new(parent_dir));
        Ok(())
    }
}
