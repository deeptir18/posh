use super::Result;
use failure::bail;
use std::path::Path;
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]

pub enum StreamType {
    RemoteFile,  // file lives on the server
    LocalFile,   // file lives on the client
    Pipe,        // intermediate computation,
    LocalStdout, // local standard out
    NoStream,    // empty stream (used for default stdin arg)
}

impl Default for StreamType {
    fn default() -> Self {
        StreamType::NoStream
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default)]
pub struct DataStream {
    pub stream_type: StreamType,
    pub name: String,
}

impl DataStream {
    // TODO: feels like the right design is to have a wrapper around the command class -- this way
    // eventually you can stop using rust's command library
    pub fn new(stream_type: StreamType, name: &str) -> Self {
        DataStream {
            stream_type: stream_type,
            name: name.to_string(),
        }
    }

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
        self.stream_type
    }
}
