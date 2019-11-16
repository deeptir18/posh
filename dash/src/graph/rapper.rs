use super::program::ProgId;
use super::stream;
use super::Location;
use super::Result;
use stream::{IOType, SharedPipeMap, SharedStreamMap, Stream, StreamIdentifier, StreamType};

pub fn stream_initiate_filter(s: Stream, is_server: bool) -> bool {
    match is_server {
        // if client, only filter for network connection streams
        false => match s.get_network_connection() {
            Some(_) => true,
            None => false,
        },
        // if server, filter out streams where the location is the client
        true => match s.get_network_connection() {
            Some(loc) => match loc {
                Location::Client => false,
                Location::Server(_) => true,
            },
            None => false,
        },
    }
}

/// Resolves a file stream to point to the correct path on the given server
pub fn resolve_file_streams(streams: &mut Vec<Stream>, parent_dir: &str) -> Result<()> {
    for s in streams.iter_mut() {
        match s.get_type() {
            StreamType::File(_) => {
                s.prepend_directory(parent_dir)?;
            }
            _ => {}
        }
    }
    Ok(())
}
/// Defines the set of functionality necessary to execute a node on any machine.
/// All types of nodes implement this trait.
pub trait Rapper {
    /// Returns all streams this node would need to initiate.
    fn get_outward_streams(
        &self,
        prog_id: ProgId,
        iotype: IOType,
        is_server: bool,
    ) -> Vec<(Location, StreamIdentifier)>;
    fn get_stdin(&self) -> Vec<stream::Stream>;

    fn get_stdout(&self) -> Vec<stream::Stream>;

    fn get_stderr(&self) -> Vec<stream::Stream>;

    fn add_stdin(&mut self, stream: stream::Stream) -> Result<()>;

    fn add_stdout(&mut self, stream: stream::Stream) -> Result<()>;

    fn add_stderr(&mut self, stream: stream::Stream) -> Result<()>;

    fn execute(
        &mut self,
        pipes: SharedPipeMap,
        network_connections: SharedStreamMap,
        prog_id: ProgId,
    ) -> Result<()>;

    fn get_loc(&self) -> Location;

    fn resolve_args(&mut self, parent_dir: &str) -> Result<()>;
}
