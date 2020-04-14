use super::execute::Execute;
use super::info::{resolve_file_streams, Info};
use super::pipe::{get_channel_name, BufferedPipe, PipeMode, SharedChannelMap};
use super::rapper::copy_wrapper as copy;
use super::rapper::stream_initiate_filter;
use super::{program, stream, Location, Result};
use failure::bail;
use program::{Link, NodeId, ProgId};
use std::mem::drop;
use std::path::PathBuf;
use std::slice::IterMut;
use stream::{
    DashStream, HandleIdentifier, IOType, NetStream, PipeStream, SharedPipeMap, SharedStreamMap,
};
use tracing::{debug, error};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default)]
pub struct WriteNode {
    /// Id within the program.
    node_id: NodeId,
    /// Id of the program.
    prog_id: ProgId,
    /// Input streams to write node.
    stdin: Vec<DashStream>,
    /// Output streams (note: must be file streams).
    output: DashStream,
    /// Execution location of the read node.
    location: Location,
}

impl WriteNode {
    pub fn get_stdout_mut(&mut self) -> &mut DashStream {
        &mut self.output
    }

    pub fn get_output_ref(&self) -> &DashStream {
        &self.output
    }

    pub fn get_stdin_iter_mut(&mut self) -> IterMut<DashStream> {
        self.stdin.iter_mut()
    }

    pub fn get_output_location(&self) -> Result<Location> {
        match &self.output {
            DashStream::File(fs) => Ok(fs.get_location()),
            DashStream::Fifo(fs) => Ok(fs.get_location()),
            DashStream::Stdout => Ok(Location::Client),
            DashStream::Stderr => Ok(Location::Client),
            _ => {
                error!(
                    "WriteNode seems to have non-file, stdout or stderr output stream: {:?}",
                    self.output
                );
                bail!(
                    "WriteNode seems to have non-file, stdout or stderr output stream: {:?}",
                    self.output
                );
            }
        }
    }
}

impl Info for WriteNode {
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
        Some(self.output.clone())
    }

    fn get_stdout_id(&self) -> Option<NodeId> {
        None
    }

    fn get_stderr(&self) -> Option<DashStream> {
        None
    }

    fn get_stdin_len(&self) -> usize {
        self.stdin.len()
    }

    fn get_stdout_len(&self) -> usize {
        1
    }

    fn get_stderr_len(&self) -> usize {
        unimplemented!()
    }

    fn add_stdin(&mut self, stream: DashStream) -> Result<()> {
        match &stream {
            DashStream::Pipe(_ps) => {}
            DashStream::Tcp(_ns) => {}
            _ => {
                bail!(
                    "Cannot have stream of type {:?} as input to write node {}",
                    stream,
                    self.node_id
                );
            }
        }
        self.stdin.push(stream);
        Ok(())
    }

    fn set_stdout(&mut self, stream: DashStream) -> Result<()> {
        match stream {
            DashStream::File(_) => {}
            DashStream::Fifo(_) => {}
            DashStream::Stdout => {}
            DashStream::Stderr => {}
            _ => {
                bail!("Cannot have stream {:?} as output to write node {:?}");
            }
        }
        self.output = stream;
        Ok(())
    }

    fn set_stderr(&mut self, _stream: DashStream) -> Result<()> {
        unimplemented!()
    }

    fn get_dot_label(&self) -> Result<String> {
        Ok(format!(
            "{}: {:?}\nloc: {:?}",
            self.node_id, self.output, self.location
        ))
    }

    fn resolve_args(&mut self, parent_dir: PathBuf) -> Result<()> {
        resolve_file_streams(&mut self.stdin, parent_dir.as_path());
        match self.output {
            DashStream::File(ref mut fs) => {
                fs.prepend_directory(&parent_dir.as_path());
            }
            _ => {}
        }
        Ok(())
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

    /// Modify the pipe to be a netstream.
    fn replace_pipe_with_ds(
        &mut self,
        pipe: PipeStream,
        repl: DashStream,
        iotype: IOType,
    ) -> Result<()> {
        match iotype {
            IOType::Stdin => {
                let mut replaced = false;
                for stream in self.stdin.iter_mut() {
                    match stream {
                        DashStream::Pipe(ps) => {
                            if *ps == pipe {
                                std::mem::replace(stream, repl.clone());
                                replaced = true;
                                break;
                            } else {
                                continue;
                            }
                        }
                        _ => {}
                    }
                }
                if !replaced {
                    error!(
                        "In replace_pipe_with_ds, pipe {:?} doesn't exist to replace with ds {:?}",
                        pipe, repl
                    );
                    bail!("Pipe doesn't exist in replace_pipe_with_ds");
                } else {
                    Ok(())
                }
            }
            IOType::Stdout => {
                error!("Calling replace_pipe_with_ds for iotype STDOUT in write node");
                bail!("No pipe stdout for write node");
            }
            IOType::Stderr => {
                error!("Calling replace_pipe_with_ds for iotype STDERR in write node");
                bail!("No pipe stderr for write node");
            }
        }
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
}

impl Execute for WriteNode {
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
        mut pipes: SharedPipeMap,
        mut network_connections: SharedStreamMap,
        mut channels: SharedChannelMap,
        tmp_folder: PathBuf,
    ) -> Result<()> {
        debug!(
            "Spawning thread for copying stdin into node {:?}",
            self.node_id
        );

        // open a file for appending
        match &self.output {
            DashStream::File(filestream) => {
                let f = filestream.open()?;
                drop(f);
            }
            DashStream::Fifo(fifostream) => {
                // first, create the fifo
                fifostream.create()?;
            }
            _ => {}
        }
        // open the file for appending
        for input_stream in self.stdin.iter() {
            match &input_stream {
                DashStream::Tcp(netstream) => {
                    let mut tcpstream = network_connections.remove(&netstream)?;
                    match &self.output {
                        DashStream::File(filestream) => {
                            let mut f = filestream.open_with_append()?;
                            copy(&mut tcpstream, &mut f)?;
                        }
                        DashStream::Fifo(fifostream) => {
                            let mut f = fifostream.open()?;
                            copy(&mut tcpstream, &mut f)?;
                        }
                        DashStream::Stdout => {
                            copy(&mut tcpstream, &mut std::io::stdout())?;
                        }
                        DashStream::Stderr => {
                            copy(&mut tcpstream, &mut std::io::stderr())?;
                        }
                        _ => {
                            error!(
                                "Cannot have stream of type {:?} as output of write node",
                                self.output
                            );
                            bail!(
                                "Cannot have stream of type {:?} as output of write node",
                                self.output
                            );
                        }
                    }
                }
                DashStream::Pipe(pipestream) => {
                    match pipestream.get_bufferable() {
                        true => {
                            let channel_end = channels.remove(&get_channel_name(
                                pipestream.get_left(),
                                PipeMode::Read,
                                pipestream.get_output_type(),
                            ))?;
                            let mut handle = BufferedPipe::new(
                                pipestream.get_left(),
                                pipestream.get_output_type(),
                                tmp_folder.as_path(),
                                PipeMode::Read,
                                channel_end,
                            )?;
                            match &self.output {
                                DashStream::File(filestream) => {
                                    let mut f = filestream.open_with_append()?;
                                    copy(&mut handle, &mut f)?;
                                }
                                DashStream::Stdout => {
                                    copy(&mut handle, &mut std::io::stdout())?;
                                }
                                DashStream::Stderr => {
                                    copy(&mut handle, &mut std::io::stderr())?;
                                }
                                _ => {
                                    error!(
                                        "Cannot have stream of type {:?} as output of write node for a pipestream",
                                        self.output
                                    );
                                    bail!(
                                        "Cannot have stream of type {:?} as output of write node for a pipestream",
                                        self.output
                                    );
                                }
                            }
                        }
                        false => {
                            let identifier = HandleIdentifier::new(
                                self.prog_id,
                                pipestream.get_left(),
                                pipestream.get_output_type(),
                            );
                            let mut handle = pipes.remove(&identifier)?;
                            match &self.output {
                                DashStream::File(filestream) => {
                                    let mut f = filestream.open_with_append()?;
                                    copy(&mut handle, &mut f)?;
                                }
                                DashStream::Stdout => {
                                    copy(&mut handle, &mut std::io::stdout())?;
                                }
                                DashStream::Stderr => {
                                    copy(&mut handle, &mut std::io::stderr())?;
                                }
                                _ => {
                                    error!(
                                        "Cannot have stream of type {:?} as output of write node for a pipestream",
                                        self.output
                                    );
                                    bail!(
                                        "Cannot have stream of type {:?} as output of write node for a pipestream",
                                        self.output
                                    );
                                }
                            }
                        }
                    };
                }
                _ => {
                    error!(
                        "Cannot have stream of type {:?} as output of write node",
                        input_stream
                    );
                    bail!(
                        "Cannot have stream of type {:?} as output of write node",
                        input_stream
                    );
                }
            }
        }

        Ok(())
    }
}
