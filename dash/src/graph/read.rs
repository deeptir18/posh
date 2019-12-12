use super::rapper::copy_wrapper as copy;
use super::rapper::{resolve_file_streams, stream_initiate_filter, Rapper};
use super::{program, stream, Location, Result};
use failure::bail;
use itertools::join;
use program::{NodeId, ProgId};
use std::fs::OpenOptions;
use std::slice::IterMut;
use stream::{
    DashStream, HandleIdentifier, IOType, NetStream, PipeStream, SharedPipeMap, SharedStreamMap,
};

/// Node that reads from files and sends the output to the specified outputs.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default)]
pub struct ReadNode {
    /// Id within the program.
    node_id: NodeId,
    /// Id of the program.
    prog_id: ProgId,
    /// Input streams to the read node (note: must be file streams).
    input: Vec<DashStream>,
    /// Output streams (where to copy output to).
    stdout: Vec<DashStream>,
    /// Execution location of read node.
    location: Location,
}

impl ReadNode {
    pub fn get_stdin_iter_mut(&mut self) -> IterMut<DashStream> {
        self.input.iter_mut()
    }
    pub fn get_stdout_iter_mut(&mut self) -> IterMut<DashStream> {
        self.stdout.iter_mut()
    }

    pub fn get_input_locations(&self) -> Vec<Location> {
        let mut ret: Vec<Location> = Vec::new();
        for stream in self.input.iter() {
            match stream {
                DashStream::File(fs) => ret.push(fs.get_location()),
                _ => {}
            }
        }
        ret
    }
}

impl Rapper for ReadNode {
    fn set_id(&mut self, id: NodeId) {
        self.node_id = id;
    }

    fn get_id(&self) -> NodeId {
        self.node_id
    }

    fn get_dot_label(&self) -> Result<String> {
        let inputs: Result<Vec<String>> = self
            .input
            .iter()
            .map(|stream| stream.get_dot_label())
            .collect();
        match inputs {
            Ok(o) => Ok(format!(
                "{}: {}\nloc: {:?}",
                self.node_id,
                join(o, "\n\n"),
                self.location
            )),
            Err(e) => bail!("{:?}", e),
        }
    }

    fn set_loc(&mut self, loc: Location) {
        self.location = loc;
    }

    fn get_outward_streams(&self, iotype: IOType, is_server: bool) -> Vec<NetStream> {
        // only could be in output streams; input streams must be filestreams
        let streams: Vec<DashStream> = match iotype {
            IOType::Stdout => self
                .stdout
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
        self.input.len()
    }

    fn get_stdout_len(&self) -> usize {
        self.stdout.len()
    }

    fn get_stderr_len(&self) -> usize {
        0
    }

    fn get_stdin(&self) -> Vec<DashStream> {
        self.input.clone()
    }

    fn get_stdout(&self) -> Vec<DashStream> {
        self.stdout.clone()
    }

    fn get_stderr(&self) -> Vec<DashStream> {
        vec![]
    }

    fn add_stdin(&mut self, stream: DashStream) -> Result<()> {
        match stream {
            DashStream::File(fs) => {
                self.input.push(DashStream::File(fs));
            }
            _ => bail!("Adding stdin to read node that is not a file stream."),
        }
        Ok(())
    }
    fn add_stdout(&mut self, stream: DashStream) -> Result<()> {
        self.stdout.push(stream);
        Ok(())
    }

    fn add_stderr(&mut self, _stream: DashStream) -> Result<()> {
        bail!("No stderr for read node");
    }
    fn run_redirection(
        &mut self,
        mut pipes: SharedPipeMap,
        mut network_connections: SharedStreamMap,
    ) -> Result<()> {
        for input_stream in self.input.iter() {
            for stream in self.stdout.iter() {
                match stream {
                    DashStream::Tcp(netstream) => {
                        let mut tcpstream = network_connections.remove(&netstream)?;
                        match input_stream {
                            DashStream::File(filestream) => {
                                let mut file_handle = OpenOptions::new()
                                    .write(true)
                                    .create(true)
                                    .open(filestream.get_name())?;
                                copy(&mut file_handle, &mut tcpstream)?;
                            }
                            _ => {
                                bail!(
                                    "Input stream for read node MUST be of type File: {:?}",
                                    input_stream
                                );
                            }
                        }
                    }
                    DashStream::Pipe(pipestream) => {
                        let handle_identifier = HandleIdentifier::new(
                            self.prog_id,
                            self.node_id,
                            // NOTE: pipe between read node and cmd node will be of type input
                            pipestream.get_output_type(),
                        );
                        let mut input_handle = pipes.remove(&handle_identifier)?;

                        match input_stream {
                            DashStream::File(filestream) => {
                                let mut file_handle = OpenOptions::new()
                                    .write(true)
                                    .create(true)
                                    .open(filestream.get_name())?;
                                copy(&mut file_handle, &mut input_handle)?;
                            }
                            _ => {
                                bail!(
                                    "Input file for read node MUST be of type file: {:?}",
                                    input_stream
                                );
                            }
                        }
                    }
                    _ => {
                        bail!("Read node should not send output from a file, stdout, or stderr handle: {:?}", stream);
                    }
                }
            }
        }
        Ok(())
    }

    fn execute(
        &mut self,
        _pipes: SharedPipeMap,
        _network_connections: SharedStreamMap,
    ) -> Result<()> {
        Ok(())
    }

    fn get_loc(&self) -> Location {
        self.location.clone()
    }

    fn resolve_args(&mut self, parent_dir: &str) -> Result<()> {
        resolve_file_streams(&mut self.input, parent_dir)?;
        resolve_file_streams(&mut self.stdout, parent_dir)?;
        Ok(())
    }
    fn replace_pipe_with_net(
        &mut self,
        pipe: PipeStream,
        net: NetStream,
        iotype: IOType,
    ) -> Result<()> {
        match iotype {
            IOType::Stdin => {
                bail!("Stdin for read node should not be pipe!");
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
                bail!("No stderr out of read node");
            }
        }
        Ok(())
    }
}
