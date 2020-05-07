use super::{program, stream, Location};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
#[derive(Serialize, Deserialize, PartialEq, Debug, Copy, Clone)]
pub enum ClientReturnCode {
    Success,
    Failure,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Copy, Clone)]
pub enum ClientLoadStatus {
    TooBusy,
    ResourcesAvailable,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Copy, Clone)]
pub enum ExecutionLocation {
    Server,
    Client,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RequestReturnInfo {
    pub status: ClientReturnCode,    // success or failure
    pub location: ExecutionLocation, // executed on client or server?
    pub server_load: Vec<f32>,       // load on each processor when request happened
    pub server_timestamp: u128,      // when the server processed this request
    pub client_time: u128,           // time client takes to finish the request
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct StreamSetupMsg {
    pub stdout_port: u16,
    pub stderr_port: u16,
}

/// Used to initiate over the network streams.
/// For stdout and stderr, the receiver for the stdin process sends a request to the sender.
/// For stdin, the sender of the stream sends a request to the receiver.
/// For streams involving the client, the client initiates the request.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct NetworkStreamInfo {
    /// Who is initiating the connection
    pub loc: Location,
    /// port for initiation
    pub port: String,
    /// Program Id
    pub prog_id: program::ProgId,
    /// Stream object: type and unique name
    pub netstream: stream::NetStream,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SizeRequest {
    /// (file, is_dir) bools
    pub files: Vec<PathBuf>,
    /// sizes of each file (only used in return)
    pub sizes: Vec<(PathBuf, u64)>,
    /// did the request fail?
    pub failed: bool,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum MessageType {
    /// Request to execute a set of nodes
    ProgramExecution,
    /// Request to setup a pipe.
    Pipe,
    /// Control message (e.g., success or failure).
    Control,
    /// Initial control message to setup a stream.
    SetupStreams,
    /// Request size for files
    SizeRequest,
}
impl MessageType {
    pub fn from_u32(value: u32) -> MessageType {
        match value {
            1 => MessageType::ProgramExecution,
            2 => MessageType::Pipe,
            3 => MessageType::Control,
            4 => MessageType::SetupStreams,
            5 => MessageType::SizeRequest,
            _ => panic!("Passing in unknown message type to constructor: {}", value),
        }
    }

    pub fn to_u32(&self) -> u32 {
        match *self {
            MessageType::ProgramExecution => 1,
            MessageType::Pipe => 2,
            MessageType::Control => 3,
            MessageType::SetupStreams => 4,
            MessageType::SizeRequest => 5,
        }
    }
}
