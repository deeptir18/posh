//! Pipe object that temporarily buffers output into a local temporary file, if necessary.
//! Input:
//!     A command node's child output handle (for either stdout ot stderr).
//! Output
//!     Can be a TCP connection.
//!     Can also be another process on the same machine.
//! What it does:
//! Things that need to happen:
//! 1. If there are multiple cmd nodes -> 1 cmd nodes, where the multiple command nodes are from
//!    different machines or from the client, need to make sure the output from each cmd node is
//!    copied into the stdin of 1 command, when buffering any temporary output in a file at the
//!    producer side
//! 2. If there are multiple cmd nodes -> 1 write node, where some of the connections are different
//!    machines, or all the same machine even, need to make sure all the output gets there at the
//!    correct time.
//! 3. The output needs to be copied from the producer command node to a consumer node even if it's
//!    being buffered somewhere
//! 4. The complicated case is when a command node is copying to another command node, but it is
//!    not the only one:
//!     At some point -> the call copy(node1 pipe, node2.stdin) needs to happen
//!     - Maybe this can happen on the input side?
//!             This way, there needs to be no "condition variables"
//!     - If the pipe object implements "read"
//! Then for the TCP connection case:
//!      Need to copy from the pipe into the connection when the connection is ready
//!      This would need to be done on the PRODUCER SIDE, in a thread (so handled in the "output"
//!      case)
use super::program::NodeId;
use super::stream::IOType;
use super::Result;
use super::SharedMap;
use crossbeam::channel::{bounded, select, Receiver, Sender};
use failure::bail;
use nix::sys::stat;
use nix::unistd;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tracing::error;

pub enum ChannelEnd {
    Sender(Sender<u32>),
    Receiver(Receiver<u32>),
}

pub type SharedChannelMap = SharedMap<String, ChannelEnd>;

pub fn create_and_insert_channels(
    node_id: NodeId,
    iotype: IOType,
    shared_map: &mut SharedChannelMap,
) -> Result<()> {
    let (s, r) = get_channel_ends();
    shared_map.insert(get_channel_name(node_id, PipeMode::Write, iotype), s)?;
    shared_map.insert(get_channel_name(node_id, PipeMode::Read, iotype), r)?;
    Ok(())
}

fn get_channel_ends() -> (ChannelEnd, ChannelEnd) {
    let (s, r) = bounded(1);
    (ChannelEnd::Sender(s), ChannelEnd::Receiver(r))
}

/// Unique naming scheme for pipe channels.
pub fn get_channel_name(node_id: NodeId, mode: PipeMode, iotype: IOType) -> String {
    format!("{}_{:?}_{:?}", node_id, iotype, mode)
}

fn buffer_name(tmp: &Path, id: NodeId, iotype: IOType) -> PathBuf {
    let mut ret = tmp.to_path_buf();
    ret.push(&format!("{:?}_{:?}", id, iotype));
    ret
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub enum PipeMode {
    Write,
    Read,
}
pub struct BufferedPipe {
    /// Filepath
    filepath: PathBuf,
    /// Is this the write or the read end of the pipe?
    mode: PipeMode,
    /// Handle to the filepath
    handle: File,
    /// Channel end
    channel: ChannelEnd,
    /// finished writing?
    finished_writing: bool,
}

/// Creates a new FIFO with read, write permissions for the owner.
pub fn create_fifo_file(tmp: &Path, id: NodeId, iotype: IOType) -> Result<()> {
    let mut mode = stat::Mode::S_IRUSR;
    mode.insert(stat::Mode::S_IWUSR);
    match unistd::mkfifo(&buffer_name(tmp, id, iotype), mode) {
        Ok(_) => Ok(()),
        Err(e) => bail!(
            "Failed to create buffer fifo path {:?}: {:?}",
            &buffer_name(tmp, id, iotype),
            e
        ),
    }
}

/// Creates a new FIFO with read, write permissions for the owner.
pub fn create_buffer_file(tmp: &Path, id: NodeId, iotype: IOType) -> Result<()> {
    let _ = OpenOptions::new()
        .create(true)
        .open(buffer_name(tmp, id, iotype))?;
    Ok(())
}

impl BufferedPipe {
    pub fn new(
        id: NodeId,
        iotype: IOType,
        tmp: &Path,
        mode: PipeMode,
        channel: ChannelEnd,
    ) -> Result<Self> {
        let handle = match mode {
            PipeMode::Write => {
                match channel {
                    ChannelEnd::Receiver(_) => {
                        bail!("Cannot pass in Write Mode with a Receiver");
                    }
                    _ => {}
                }
                OpenOptions::new()
                    .write(true)
                    .open(buffer_name(tmp, id, iotype))?
            }
            PipeMode::Read => {
                match channel {
                    ChannelEnd::Sender(_) => {
                        bail!("Cannot pass in Read Mode with a Sender");
                    }
                    _ => {}
                }
                OpenOptions::new()
                    .read(true)
                    .open(buffer_name(tmp, id, iotype))?
            }
        };
        Ok(BufferedPipe {
            filepath: buffer_name(tmp, id, iotype),
            mode: mode,
            handle: handle,
            channel: channel,
            finished_writing: false,
        })
    }

    pub fn get_handle(&self) -> Result<File> {
        let handle = match self.mode {
            PipeMode::Write => OpenOptions::new()
                .write(true)
                .create(true)
                .open(self.filepath.as_path()),
            PipeMode::Read => OpenOptions::new().read(true).open(self.filepath.as_path()),
        };
        match handle {
            Ok(h) => Ok(h),
            Err(e) => bail!("{:?}", e),
        }
    }

    /// On finishing the copy from process into the file, need to "set write done" on the buffered
    /// pipe so the receiving thread will know the write is done.
    pub fn set_write_done(&mut self) -> Result<()> {
        match &mut self.channel {
            ChannelEnd::Sender(ref mut sender) => {
               sender.send(1)?;
               Ok(())
            }
            ChannelEnd::Receiver(_) => {
                bail!("Cannot call set_write_done with a receiver channel; should only have a writer channel")
            }
        }
    }
}

impl Write for BufferedPipe {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.handle.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.handle.flush()
    }
}

impl Read for BufferedPipe {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let receiver = match &mut self.channel {
            ChannelEnd::Receiver(ref mut r) => r,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Channel must be a receiver",
                ));
            }
        };
        loop {
            select! {
                recv(receiver) -> msg => {
                    match msg {
                        Ok(num) => {
                            assert_eq!(num, 1);
                            self.finished_writing = true;
                        }
                        Err(e) => {
                            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Didn't receive msg on the channel: {:?}", e)));
                        }
                    }
                }
                default => {
                    // try reading from the file
                    match self.handle.read(buf) {
                        Ok(size) => {
                            if size == 0 {
                                if self.finished_writing {
                                    return Ok(0);
                                } else {
                                    continue;
                                }
                            } else {
                                return Ok(size);
                            }
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }


                }

            }
        }
    }
}

/// Turns BufferedPipe object into an Stdio.
/// Allows commands to *directly* write output into the BufferedPipe object without an extra copy.
/// TODO: we might not be able to use this because we need some way of knowing that the thread is
/// done.
impl From<BufferedPipe> for Stdio {
    fn from(buffered_pipe: BufferedPipe) -> Stdio {
        let handle = match buffered_pipe.get_handle() {
            Ok(h) => h,
            Err(e) => {
                error!("Could not get a file handled for buffered pipe: {:?}", e);
                panic!("Could not get a file handled for buffered pipe: {:?}", e);
            }
        };
        Stdio::from(handle)
    }
}
