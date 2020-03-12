use super::pipe::SharedChannelMap;
use super::stream::{SharedPipeMap, SharedStreamMap};
use super::Result;
use std::path::PathBuf;
/// Functions to enable executing nodes on any machine.
pub trait Execute {
    /// Spawns the node to do the necessary work.
    fn spawn(
        &mut self,
        pipes: SharedPipeMap,
        network_connections: SharedStreamMap,
        channels: SharedChannelMap,
        tmp_folder: PathBuf,
    ) -> Result<()>;

    /// Redirects input and output of node to the correct places based on where the stdin, stdout
    /// and stderr go to.
    fn redirect(
        &mut self,
        pipes: SharedPipeMap,
        network_connections: SharedStreamMap,
        channels: SharedChannelMap,
        tmp_folder: PathBuf,
    ) -> Result<()>;
}
