use super::filestream::FileStream;
use super::program::{NodeId, ProgId};
use super::{Location, Result, SharedMap};
use failure::bail;
use serde::{Deserialize, Serialize};
use std::convert::Into;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{ChildStderr, ChildStdin, ChildStdout};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq)]
pub struct PipeStream {
    /// Left node that emits the pipe
    left: NodeId,
    /// Right node that receives the pipe
    right: NodeId,
    /// Stdout or stderr of the left node?
    output_type: IOType,
    /// Buffer intermediate output?
    buffer_into_file: bool,
}

impl PipeStream {
    pub fn new(left: NodeId, right: NodeId, output_type: IOType) -> Result<Self> {
        match output_type {
            IOType::Stdin => bail!("Cannot construct PipeStream with Stdin as the output_type"),
            _ => {}
        }
        Ok(PipeStream {
            left: left,
            right: right,
            output_type: output_type,
            buffer_into_file: false,
        })
    }

    /// Returns string to display on a pipe stream node
    /// Mainly used for debugging purposes.
    pub fn get_dot_label(&self) -> String {
        format!(
            "PIPE:\nleft: {:?}\nright: {:?}\ntype: {:?}",
            self.left, self.right, self.output_type
        )
    }

    pub fn get_left(&self) -> NodeId {
        self.left
    }

    pub fn get_right(&self) -> NodeId {
        self.right
    }

    pub fn set_left(&mut self, id: NodeId) {
        self.left = id;
    }

    pub fn set_right(&mut self, id: NodeId) {
        self.right = id;
    }
    pub fn get_output_type(&self) -> IOType {
        self.output_type
    }

    pub fn set_bufferable(&mut self) {
        self.buffer_into_file = true;
    }

    pub fn get_bufferable(&self) -> bool {
        self.buffer_into_file
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq)]
pub struct NetStream {
    /// Left node that emits stream
    left: NodeId,
    /// Right node that emits strea,
    right: NodeId,
    /// Stdout or stderr of left node?
    output_type: IOType,
    /// Location of left side of connection
    left_location: Location,
    /// Location of right side of connection
    right_location: Location,
    /// Should we buffer into a file
    buffer_into_file: bool,
}

impl Default for NetStream {
    fn default() -> Self {
        NetStream {
            left: 0,
            right: 0,
            output_type: IOType::Stdout,
            left_location: Location::Client,
            right_location: Location::Client,
            buffer_into_file: false,
        }
    }
}

impl NetStream {
    pub fn new(
        left: NodeId,
        right: NodeId,
        output_type: IOType,
        left_location: Location,
        right_location: Location,
    ) -> Result<Self> {
        match output_type {
            IOType::Stdin => bail!("Cannot construct PipeStream with Stdin as the output_type"),
            _ => {}
        }
        Ok(NetStream {
            left: left,
            right: right,
            output_type: output_type,
            left_location: left_location,
            right_location: right_location,
            buffer_into_file: false,
        })
    }

    /// Returns string to display on a pipe stream node
    /// Mainly used for debugging purposes.
    pub fn get_dot_label(&self) -> String {
        format!(
            "NETPIPE:\nleft: {:?},{:?}\nright: {:?},{:?}\ntype: {:?}",
            self.left, self.left_location, self.right, self.right_location, self.output_type
        )
    }

    pub fn set_left(&mut self, id: NodeId) {
        self.left = id;
    }

    pub fn set_right(&mut self, id: NodeId) {
        self.right = id;
    }

    pub fn get_left(&self) -> NodeId {
        self.left
    }

    pub fn get_right(&self) -> NodeId {
        self.right
    }

    pub fn get_output_type(&self) -> IOType {
        self.output_type
    }

    pub fn get_right_location(&self) -> Location {
        self.right_location.clone()
    }

    pub fn get_left_location(&self) -> Location {
        self.left_location.clone()
    }

    pub fn get_sending_side(&self) -> Location {
        if self.left_location == Location::Client {
            return self.left_location.clone();
        } else if self.right_location == Location::Client {
            return self.right_location.clone();
        } else {
            return self.left_location.clone();
        }
    }

    pub fn get_receiving_side(&self) -> Location {
        if self.left_location == Location::Client {
            return self.right_location.clone();
        } else if self.right_location == Location::Client {
            return self.left_location.clone();
        } else {
            return self.right_location.clone();
        }
    }

    /// Gets other side of the Tcp Connection as the node_id
    /// Assumes this stream is in a vector of streams on either node end.
    pub fn get_connection(&self, id: NodeId) -> Option<Location> {
        if id == self.left {
            return Some(self.right_location.clone());
        } else if id == self.right {
            return Some(self.left_location.clone());
        }
        return None;
    }

    pub fn get_bufferable(&self) -> bool {
        self.buffer_into_file
    }

    pub fn set_bufferable(&mut self) {
        self.buffer_into_file = true;
    }
}

/// Kinds of inputs and outputs for node
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq)]
pub enum DashStream {
    /// File on a certain machine
    File(FileStream),
    /// Pipe between two local processes
    Pipe(PipeStream),
    /// Networked pipe between two nodes across machines
    Tcp(NetStream),
    /// Stdout on the client
    Stdout,
    /// Stderr on the client
    Stderr,
}

impl Default for DashStream {
    fn default() -> Self {
        DashStream::Stdout
    }
}

impl DashStream {
    pub fn get_dot_label(&self) -> Result<String> {
        match self {
            DashStream::File(fs) => Ok(fs.get_dot_label()),
            DashStream::Pipe(ps) => Ok(ps.get_dot_label()),
            DashStream::Tcp(ns) => Ok(ns.get_dot_label()),
            DashStream::Stdout => Ok("STDOUT".to_string()),
            DashStream::Stderr => Ok("STDERR".to_string()),
        }
    }
}
impl Into<Option<FileStream>> for DashStream {
    fn into(self) -> Option<FileStream> {
        match self {
            DashStream::File(stream) => Some(stream),
            _ => None,
        }
    }
}

impl Into<Option<PipeStream>> for DashStream {
    fn into(self) -> Option<PipeStream> {
        match self {
            DashStream::Pipe(stream) => Some(stream),
            _ => None,
        }
    }
}

impl Into<Option<NetStream>> for DashStream {
    fn into(self) -> Option<NetStream> {
        match self {
            DashStream::Tcp(stream) => Some(stream),
            _ => None,
        }
    }
}

/// Used to uniquely identify handles in the SharedPipeMap.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq)]
pub struct HandleIdentifier {
    /// Program Id
    pub prog_id: ProgId,
    /// NodeId that created this handle
    pub node_id: NodeId,
    /// IOtype: Stdin, stdout or stderr?
    pub iotype: IOType,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq, Copy)]
pub enum IOType {
    Stdin,
    Stdout,
    Stderr,
}

impl Default for HandleIdentifier {
    fn default() -> Self {
        HandleIdentifier {
            prog_id: 0,
            node_id: 0,
            iotype: IOType::Stdout,
        }
    }
}

impl HandleIdentifier {
    pub fn new(prog_id: ProgId, node_id: NodeId, iotype: IOType) -> Self {
        HandleIdentifier {
            prog_id: prog_id,
            node_id: node_id,
            iotype: iotype,
        }
    }
}

pub enum OutputHandle {
    Stdin(ChildStdin),
    Stdout(ChildStdout),
    Stderr(ChildStderr),
}

impl Read for OutputHandle {
    fn read(&mut self, buf: &mut [u8]) -> std::result::Result<usize, std::io::Error> {
        match self {
            OutputHandle::Stdout(handle) => handle.read(buf),
            OutputHandle::Stderr(handle) => handle.read(buf),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "child stdin does not implement read!",
            )),
        }
    }
}

impl Write for OutputHandle {
    fn write(&mut self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
        match self {
            OutputHandle::Stdin(handle) => handle.write(buf),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "child stdout or stderr does not implement write!",
            )),
        }
    }

    fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        match self {
            OutputHandle::Stdin(handle) => handle.flush(),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "child stdout or stderr does not implement write!",
            )),
        }
    }
}
impl Into<Option<ChildStdin>> for OutputHandle {
    fn into(self) -> Option<ChildStdin> {
        match self {
            OutputHandle::Stdin(c) => Some(c),
            _ => None,
        }
    }
}

impl Into<Option<ChildStdout>> for OutputHandle {
    fn into(self) -> Option<ChildStdout> {
        match self {
            OutputHandle::Stdout(c) => Some(c),
            _ => None,
        }
    }
}

impl Into<Option<ChildStderr>> for OutputHandle {
    fn into(self) -> Option<ChildStderr> {
        match self {
            OutputHandle::Stderr(c) => Some(c),
            _ => None,
        }
    }
}
/// Used to manage pipes on processes in the same machine.
pub type SharedPipeMap = SharedMap<HandleIdentifier, OutputHandle>;

/// Used to manage tcp connections when executing nodes.
pub type SharedStreamMap = SharedMap<NetStream, TcpStream>;
