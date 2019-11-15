use super::program::ProgId;
use super::{Location, Result};
use failure::bail;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::TcpStream;
use std::path::Path;
use std::process::Child;
use std::sync::{Arc, Mutex};

/// Kinds of inputs and outputs for node
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq)]
pub enum StreamType {
    /// File associated with a location and a filename
    File(Location),
    /// Piped to a process on the same machine
    Piped,
    /// Piped over the network somewhere
    TcpConnection(Location),
    /// Standard out; should only be used in processes on the client
    Stdout,
}

impl Default for StreamType {
    fn default() -> Self {
        StreamType::Stdout
    }
}

/// Input and output for nodes.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default, Hash, Eq)]
pub struct Stream {
    /// The type of the stream
    stream_type: StreamType,
    /// The unique name of the stream (must be unique within the program).
    name: String,
}

impl Stream {
    /// Constructs a new Stream.
    pub fn new(stream_type: StreamType, name: &str) -> Self {
        Stream {
            stream_type: stream_type,
            name: name.to_string(),
        }
    }

    /// Constructs a new Stream.
    /// Strips the mount file from the full path.
    pub fn remote_file_stream(location: Location, full_path: &str, mount: &str) -> Result<Self> {
        let mut path = Path::new(full_path);
        path = path.strip_prefix(mount)?;
        let loc = match path.to_str() {
            Some(p) => p,
            None => bail!("Failed to strip prefix {} from {}", mount, full_path),
        };
        Ok(Stream {
            stream_type: StreamType::File(location),
            name: loc.to_string(),
        })
    }

    /// Modifies the Stream to prepend the directory to the name.
    pub fn prepend_directory(&self, directory: &str) -> Result<String> {
        match Path::new(directory)
            .join(self.name.clone())
            .as_path()
            .to_str()
        {
            Some(s) => Ok(s.to_string()),
            None => bail!("Could not prepend directory {} to {}", directory, self.name),
        }
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_type(&self) -> StreamType {
        self.stream_type.clone()
    }

    pub fn set_type(&mut self, stream_type: StreamType) {
        self.stream_type = stream_type;
    }

    /// Checks if the stream is a tcp connection, that goes over the network.
    /// Returns location if so.
    pub fn get_network_connection(&self) -> Option<Location> {
        match &self.stream_type {
            StreamType::TcpConnection(loc) => Some(loc.clone()),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq, Copy)]
pub enum IOType {
    Stdin,
    Stdout,
    Stderr,
}

impl Default for IOType {
    fn default() -> Self {
        IOType::Stdout
    }
}

/// Associates stream object, program id, and iotype, for stream setup.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default, Hash, Eq)]
pub struct StreamIdentifier {
    pub prog_id: ProgId,
    pub stream: Stream,
    pub iotype: IOType,
}

impl StreamIdentifier {
    pub fn new(id: ProgId, stream: Stream, iotype: IOType) -> Self {
        StreamIdentifier {
            iotype: iotype,
            prog_id: id,
            stream: stream,
        }
    }
}

pub struct SharedPipeMap(pub Arc<Mutex<HashMap<Stream, Child>>>);
impl SharedPipeMap {
    pub fn new() -> Self {
        let map: HashMap<Stream, Child> = HashMap::default();
        SharedPipeMap(Arc::new(Mutex::new(map)))
    }

    pub fn clone(&self) -> Self {
        SharedPipeMap(self.0.clone())
    }
}
type StreamMap = HashMap<StreamIdentifier, TcpStream>;
pub struct SharedStreamMap(pub Arc<Mutex<StreamMap>>);
impl SharedStreamMap {
    pub fn new() -> Self {
        let map: HashMap<StreamIdentifier, TcpStream> = HashMap::default();
        SharedStreamMap(Arc::new(Mutex::new(map)))
    }

    pub fn clone(&self) -> Self {
        SharedStreamMap(self.0.clone())
    }
}
