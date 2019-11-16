use super::rapper::{resolve_file_streams, stream_initiate_filter, Rapper};
use super::{program, stream, Location, Result};
use failure::bail;
use program::ProgId;
use stream::{IOType, SharedPipeMap, SharedStreamMap, Stream, StreamIdentifier, StreamType};
/// Node that writes stdin to a specified file.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct WriteNode {
    /// Input streams to write node.
    stdin: Vec<Stream>,
    /// Output streams (note: must be file streams).
    output: Vec<Stream>,
    /// Execution location of the read node.
    location: Location,
}

impl Rapper for WriteNode {
    fn get_outward_streams(
        &self,
        prog_id: ProgId,
        iotype: IOType,
        is_server: bool,
    ) -> Vec<(Location, StreamIdentifier)> {
        let streams: Vec<Stream> = match iotype {
            IOType::Stdin => self
                .stdin
                .clone()
                .iter()
                .filter(|&s| stream_initiate_filter(s.clone(), is_server))
                .cloned()
                .collect(),
            IOType::Stdout => self
                .output
                .iter()
                .filter(|&s| stream_initiate_filter(s.clone(), is_server))
                .cloned()
                .collect(),
            IOType::Stderr => {
                panic!("Write node does not have stderr!");
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
        self.stdin.clone()
    }

    fn get_stdout(&self) -> Vec<stream::Stream> {
        self.output.clone()
    }

    fn get_stderr(&self) -> Vec<stream::Stream> {
        unimplemented!();
    }

    fn add_stdin(&mut self, stream: Stream) -> Result<()> {
        self.stdin.push(stream);
        Ok(())
    }
    fn add_stdout(&mut self, stream: Stream) -> Result<()> {
        match stream.get_type() {
            StreamType::File(_) => {}
            _ => bail!("Adding stdout to write node that is not a file stream."),
        }
        self.output.push(stream);
        Ok(())
    }

    fn add_stderr(&mut self, _stream: Stream) -> Result<()> {
        bail!("No stderr for write node");
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
        resolve_file_streams(&mut self.stdin, parent_dir)?;
        resolve_file_streams(&mut self.output, parent_dir)?;
        Ok(())
    }
}
