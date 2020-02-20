use super::execute::Execute;
use super::info::{resolve_file_streams, Info};
use super::pipe::{get_channel_name, BufferedPipe, PipeMode, SharedChannelMap};
use super::rapper::copy_wrapper as copy;
use super::{program, stream, Location, Result};
use failure::bail;
use program::{NodeId, ProgId};
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

    pub fn get_stdin_iter_mut(&mut self) -> IterMut<DashStream> {
        self.stdin.iter_mut()
    }

    pub fn get_output_location(&self) -> Result<Location> {
        match &self.output {
            DashStream::File(fs) => Ok(fs.get_location()),
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

    fn get_stderr(&self) -> Option<DashStream> {
        unimplemented!();
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

    fn add_stdin(&mut self, stream: DashStream) {
        self.stdin.push(stream);
    }

    fn set_stdout(&mut self, stream: DashStream) {
        self.output = stream;
    }

    fn set_stderr(&mut self, _stream: DashStream) {
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

    /// Modify the pipe to be a netstream.
    fn replace_pipe_with_net(
        &mut self,
        pipe: PipeStream,
        net: NetStream,
        iotype: IOType,
    ) -> Result<()> {
        match iotype {
            IOType::Stdin => {
                let mut replaced = false;
                for stream in self.stdin.iter_mut() {
                    match stream {
                        DashStream::Pipe(ps) => {
                            if *ps == pipe {
                                std::mem::replace(stream, DashStream::Tcp(net.clone()));
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
                    error!("In replace_pipe_with_net, pipe {:?} doesn't exist to replace with net {:?}", pipe, net);
                    bail!("Pipe doesn't exist in replace_pipe_with_net");
                } else {
                    Ok(())
                }
            }
            IOType::Stdout => {
                error!("Calling replace_pipe_with_net for iotype STDOUT in write node");
                bail!("No pipe stdout for write node");
            }
            IOType::Stderr => {
                error!("Calling replace_pipe_with_net for iotype STDERR in write node");
                bail!("No pipe stderr for write node");
            }
        }
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
                        false => {
                            let mut handle = pipes.remove(&HandleIdentifier::new(
                                self.prog_id,
                                pipestream.get_left(),
                                pipestream.get_output_type(),
                            ))?;
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
