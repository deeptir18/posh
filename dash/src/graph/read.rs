use super::rapper::{resolve_file_streams, stream_initiate_filter, Rapper};
use super::{program, stream, Location, Result};
use failure::bail;
use program::ProgId;
use stream::{IOType, SharedPipeMap, SharedStreamMap, Stream, StreamIdentifier, StreamType};

/// Node that reads from files and sends the output to the specified outputs.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ReadNode {
    /// Input streams to the read node (note: must be file streams).
    input: Vec<Stream>,
    /// Output streams (where to copy output to).
    stdout: Vec<Stream>,
    /// Execution location of read node.
    location: Location,
}

impl Rapper for ReadNode {
    fn get_outward_streams(
        &self,
        prog_id: ProgId,
        iotype: IOType,
        is_server: bool,
    ) -> Vec<(Location, StreamIdentifier)> {
        let streams: Vec<Stream> = match iotype {
            IOType::Stdin => self
                .input
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
            IOType::Stderr => {
                panic!("Read node does not have stderr");
            }
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
        self.input.clone()
    }

    fn get_stdout(&self) -> Vec<stream::Stream> {
        self.stdout.clone()
    }

    fn get_stderr(&self) -> Vec<stream::Stream> {
        unimplemented!();
    }

    fn add_stdin(&mut self, stream: Stream) -> Result<()> {
        match stream.get_type() {
            StreamType::File(_) => {}
            _ => bail!("Adding stdin to read node that is not a file stream."),
        }
        self.input.push(stream);
        Ok(())
    }
    fn add_stdout(&mut self, stream: Stream) -> Result<()> {
        self.stdout.push(stream);
        Ok(())
    }

    fn add_stderr(&mut self, _stream: Stream) -> Result<()> {
        bail!("No stderr for read node");
    }
    fn execute(
        &mut self,
        _pipes: SharedPipeMap,
        _network_connections: SharedStreamMap,
        _prog_id: ProgId,
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
}
