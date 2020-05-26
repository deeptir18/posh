use super::program::NodeId;
use super::stream::IOType;
use super::Result;
use super::SharedMap;
use crossbeam::channel::{bounded, select, Receiver, Sender};
use failure::bail;
use std::fs::{remove_file, File, OpenOptions};
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
pub fn create_buffer_file(tmp: &Path, id: NodeId, iotype: IOType) -> Result<()> {
    let mut open_options = OpenOptions::new();
    open_options.write(true).create(true).read(true);
    open_options.open(buffer_name(tmp, id, iotype))?;
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

    pub fn drop_file(&mut self) -> Result<()> {
        remove_file(self.filepath.as_path())?;
        Ok(())
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
        let normal_read = |handle: &mut File, buffer: &mut [u8]| -> std::io::Result<usize> {
            match handle.read(buffer) {
                Ok(size) => {
                    return Ok(size);
                }
                Err(e) => {
                    println!("Error reading: {:?}", e);
                    return Err(e);
                }
            }
        };
        // if already finished writing, do read as normal
        if self.finished_writing {
            return normal_read(&mut self.handle, buf);
        }
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
                            // now do a normal read
                            return normal_read(&mut self.handle, buf);
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
