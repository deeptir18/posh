use super::execute::Execute;
use super::filestream::FileStream;
use super::info::Info;
use super::pipe::SharedChannelMap;
use super::rapper::copy_wrapper as copy;
use super::rapper::stream_initiate_filter;
use super::{program, stream, Location, Result};
use failure::bail;
use program::{Link, NodeId, ProgId};
use std::path::PathBuf;
use stream::{DashStream, IOType, NetStream, PipeStream, SharedPipeMap, SharedStreamMap};
use tracing::error;

/// Node that reads from files and sends the output to the specified outputs.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default)]
pub struct ReadNode {
    /// Id within the program.
    node_id: NodeId,
    /// Id of the program.
    prog_id: ProgId,
    /// Input streams to the read node (note: must be file streams)
    input: FileStream,
    /// Output stream for the read node
    stdout: DashStream,
    /// Execution location of read node.
    location: Location,
}

impl ReadNode {
    pub fn get_stdout_mut(&mut self) -> &mut DashStream {
        &mut self.stdout
    }

    pub fn get_stdin_mut(&mut self) -> &mut FileStream {
        &mut self.input
    }

    pub fn get_input_ref(&self) -> &FileStream {
        &self.input
    }

    pub fn get_input_location(&self) -> Result<Location> {
        Ok(self.input.get_location())
    }
}

impl Info for ReadNode {
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
        vec![DashStream::File(self.input.clone())]
    }

    fn get_stdout(&self) -> Option<DashStream> {
        Some(self.stdout.clone())
    }

    fn get_stdout_id(&self) -> Option<NodeId> {
        match &self.stdout {
            DashStream::Pipe(ps) => Some(ps.get_right()),
            DashStream::Tcp(ns) => Some(ns.get_right()),
            _ => None,
        }
    }

    fn get_stderr(&self) -> Option<DashStream> {
        None
    }

    fn get_stdin_len(&self) -> usize {
        1
    }

    fn get_stdout_len(&self) -> usize {
        1
    }

    fn get_stderr_len(&self) -> usize {
        0
    }

    fn add_stdin(&mut self, stream: DashStream) -> Result<()> {
        match stream {
            DashStream::File(fs) => {
                self.input = fs;
                Ok(())
            }
            _ => bail!(
                "Setting stdin on filestream to be a non-file stream: {:?}",
                stream
            ),
        }
    }

    fn set_stdout(&mut self, stream: DashStream) -> Result<()> {
        match stream {
            DashStream::Pipe(_) => {}
            DashStream::Tcp(_) => {}
            _ => bail!(
                "Cannot have stream of type {:?} as output for read node",
                stream
            ),
        }
        self.stdout = stream;
        Ok(())
    }

    fn set_stderr(&mut self, _stream: DashStream) -> Result<()> {
        unimplemented!()
    }

    fn get_dot_label(&self) -> Result<String> {
        Ok(format!(
            "{}: {:?}\nloc: {:?}",
            self.node_id, self.input, self.location
        ))
    }

    fn resolve_args(&mut self, parent_dir: PathBuf) -> Result<()> {
        // resolve the location of the input filestream
        self.input.prepend_directory(parent_dir.as_path());
        Ok(())
    }

    fn replace_stream_edges(&mut self, edge: Link, new_edges: Vec<Link>) -> Result<()> {
        assert!(new_edges.len() == 1);
        if self.node_id != edge.get_left() {
            bail!("Calling replace stream edges on read node where edge left is NOT node id, id: {:?}, edge: {:?}", self.node_id, edge);
        } else {
            let mut stream_to_add: Option<DashStream> = None;
            match &self.stdout {
                DashStream::Pipe(pipestream) => {
                    if pipestream.get_right() == edge.get_right() {
                        for new_edge in new_edges.iter() {
                            let mut new_pipestream = pipestream.clone();
                            new_pipestream.set_right(new_edge.get_right());
                            stream_to_add = Some(DashStream::Pipe(new_pipestream));
                        }
                    }
                }
                DashStream::Tcp(netstream) => {
                    if netstream.get_right() == edge.get_right() {
                        for new_edge in new_edges.iter() {
                            let mut new_netstream = netstream.clone();
                            new_netstream.set_right(new_edge.get_right());
                            stream_to_add = Some(DashStream::Tcp(new_netstream));
                        }
                    }
                }
                _ => {
                    unreachable!();
                }
            }
            match stream_to_add {
                Some(ds) => {
                    self.stdout = ds;
                    Ok(())
                }
                None => {
                    bail!("Couldn't find stream for edge {:?} to replace", edge);
                }
            }
        }
    }

    /// Modify the pipe to be a netstream.
    fn replace_pipe_with_ds(
        &mut self,
        pipe: PipeStream,
        repl: DashStream,
        iotype: IOType,
    ) -> Result<()> {
        match iotype {
            IOType::Stdout => {
                match &self.stdout {
                    DashStream::Pipe(ps) => {
                        if *ps == pipe {
                            self.stdout = repl.clone();
                            Ok(())
                        } else {
                            error!("In replace_pipe_with_ds, pipe {:?} doesn't exist to replace with net {:?}", pipe, repl);
                            bail!("Pipe doesn't exist in replace_pipe_with_ds");
                        }
                    }
                    _ => {
                        error!("In replace_pipe_with_ds, pipe {:?} doesn't exist to replace with net {:?}", pipe, repl);
                        bail!("Pipe doesn't exist in replace_pipe_with_ds");
                    }
                }
            }
            _ => Ok(()),
        }
    }
    fn get_outward_streams(&self, iotype: IOType, is_server: bool) -> Vec<NetStream> {
        let mut ret: Vec<NetStream> = Vec::new();
        match iotype {
            IOType::Stdout => match &self.stdout {
                DashStream::Tcp(netstream) => {
                    if stream_initiate_filter(
                        DashStream::Tcp(netstream.clone()),
                        self.node_id,
                        is_server,
                    ) {
                        ret.push(netstream.clone());
                    }
                }
                _ => {}
            },
            _ => {}
        }
        ret
    }
}

impl Execute for ReadNode {
    fn spawn(
        &mut self,
        _pipes: SharedPipeMap,
        _network_connections: SharedStreamMap,
        _channels: SharedChannelMap,
        _tmp_folder: PathBuf,
    ) -> Result<()> {
        Ok(())
    }
    fn redirect(
        &mut self,
        _pipes: SharedPipeMap,
        mut network_connections: SharedStreamMap,
        _channels: SharedChannelMap,
        _tmp_folder: PathBuf,
    ) -> Result<()> {
        let mut file_handle = self.input.open()?;
        match &self.stdout {
            DashStream::Tcp(netstream) => {
                let mut tcpstream = network_connections.remove(&netstream)?;
                // hopefully this will immediately block until the next process is ready
                copy(&mut file_handle, &mut tcpstream)?;
            }
            DashStream::Pipe(pipe) => {
                error!("Read node should not send output to a pipe: {:?}", pipe);
                bail!("Read node should not send output over a pipe: {:?}", pipe);
            }
            _ => {
                error!(
                    "Read node should not send output to a file, stdout, or stderr handle: {:?}",
                    self.stdout
                );
                bail!(
                    "Read node should not send output to a file, stdout, or stderr handle: {:?}",
                    self.stdout
                );
            }
        }
        Ok(())
    }
}
