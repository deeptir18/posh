use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, PartialEq, Debug, Copy, Clone)]
pub enum ClientReturnCode {
    TooBusy,
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
