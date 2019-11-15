use super::util::Result;
pub mod cmd;
pub mod program;
pub mod rapper;
pub mod read;
pub mod stream;
pub mod write;

/// Represents where a computation should take place, or where a stream leads to.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq)]
pub enum Location {
    /// Client/orchestration machine
    Client,
    /// Address of this particular server
    Server(String),
}

impl Default for Location {
    fn default() -> Self {
        Location::Client
    }
}
