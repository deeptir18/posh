use super::program::NodeId;
use super::stream::{DashStream, IOType, NetStream, PipeStream};
use super::{Location, Result};
use std::path::{Path, PathBuf};
/// Set of functionality related to modifying
/// and querying information about a node.
/// Includes functions that modify nodes while parsing and scheduling the program.
pub trait Info {
    fn set_id(&mut self, id: NodeId);

    fn get_id(&self) -> NodeId;

    fn get_loc(&self) -> Location;

    fn set_loc(&mut self, loc: Location);

    fn get_stdin(&self) -> Vec<DashStream>;

    fn get_stdout(&self) -> Option<DashStream>;

    fn get_stderr(&self) -> Option<DashStream>;

    fn get_stdin_len(&self) -> usize;

    fn get_stdout_len(&self) -> usize;

    fn get_stderr_len(&self) -> usize;

    fn add_stdin(&mut self, stream: DashStream);

    fn set_stdout(&mut self, stream: DashStream);

    fn set_stderr(&mut self, stream: DashStream);

    fn get_dot_label(&self) -> Result<String>;

    fn resolve_args(&mut self, parent_dir: PathBuf) -> Result<()>;

    /// Modify the pipe to be a netstream.
    fn replace_pipe_with_net(
        &mut self,
        pipe: PipeStream,
        net: NetStream,
        iotype: IOType,
    ) -> Result<()>;
}

pub fn resolve_file_streams(streams: &mut Vec<DashStream>, parent_dir: &Path) {
    for s in streams.iter_mut() {
        match s {
            DashStream::File(ref mut fs) => {
                fs.prepend_directory(parent_dir);
            }
            _ => {}
        }
    }
}

pub fn resolve_file_stream_option(stream: &mut Option<DashStream>, parent_dir: &Path) {
    if let Some(ref mut dashstream) = stream {
        match dashstream {
            DashStream::File(ref mut fs) => {
                fs.prepend_directory(parent_dir);
            }
            _ => {}
        }
    }
}
