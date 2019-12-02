use super::program::NodeId;
use super::stream;
use super::Location;
use super::Result;
use stream::{DashStream, IOType, NetStream, SharedPipeMap, SharedStreamMap, PipeStream};

/// Checks if this is a stream that represents a TCP connection that should be initiated by this
/// nodeid.
pub fn stream_initiate_filter(s: DashStream, node_id: NodeId, is_server: bool) -> bool {
    match s {
        DashStream::Tcp(netstream) => match is_server {
            // if not server (e.g. client), always initiate stream
            false => true,
            // if it is the server, check that the other connection is NOT the client, and this is
            // the left side of the connection
            true => {
                let loc = match netstream.get_connection(node_id) {
                    Some(l) => l,
                    None => {
                        // TODO: add some debugging here?
                        return false;
                    }
                };
                match loc {
                    Location::Client => {
                        return false;
                    }
                    _ => {}
                }
                if netstream.get_left() == node_id {
                    return false;
                } else {
                    return true;
                }
            }
        },
        _ => false,
    }
}

/// Resolves a file stream to point to the correct path on the given server
pub fn resolve_file_streams(streams: &mut Vec<DashStream>, parent_dir: &str) -> Result<()> {
    for s in streams.iter_mut() {
        match s {
            DashStream::File(filestream) => {
                // Mutates the underlying filestream object.
                filestream.prepend_directory(parent_dir)?;
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
    fn get_outward_streams(&self, iotype: IOType, is_server: bool) -> Vec<NetStream>;

    fn get_stdin(&self) -> Vec<DashStream>;

    fn get_stdout(&self) -> Vec<DashStream>;

    fn get_stderr(&self) -> Vec<DashStream>;

    fn get_stdin_len(&self) -> usize;

    fn get_stdout_len(&self) -> usize;

    fn get_stderr_len(&self) -> usize;

    fn add_stdin(&mut self, stream: DashStream) -> Result<()>;

    fn add_stdout(&mut self, stream: DashStream) -> Result<()>;

    fn add_stderr(&mut self, stream: DashStream) -> Result<()>;

    /// Starts processes that *execute* any commands.
    fn execute(&mut self, pipes: SharedPipeMap, network_connections: SharedStreamMap)
        -> Result<()>;

    /// Spawns threads that run redirection of I/O for any commands.
    fn run_redirection(
        &mut self,
        pipes: SharedPipeMap,
        network_connections: SharedStreamMap,
    ) -> Result<()>;

    fn get_loc(&self) -> Location;

    fn set_loc(&mut self, loc: Location);

    fn resolve_args(&mut self, parent_dir: &str) -> Result<()>;

    fn replace_pipe_with_net(&mut self, pipe: PipeStream, net: NetStream, iotype: IOType) -> Result<()>;
}
