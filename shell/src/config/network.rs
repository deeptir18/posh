use dash::util::Result;
use failure::bail;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct FileNetwork {
    /// map of local mounted paths to IP addresses
    path_to_addr: HashMap<PathBuf, ServerKey>,
    /// cache of relative paths
    cache: HashMap<PathBuf, CacheEntry>,
    /// information about other server
    server_info: Vec<ServerInfo>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq, Default)]
pub struct ServerKey {
    ip: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq, Default)]
pub struct ServerInfo {
    /// Mounts this server can access via NFS
    mounted_directories: HashMap<PathBuf, ServerKey>,
    /// Outward network connections
    connections: HashMap<ServerKey, u32>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq, Default)]
pub struct CacheEntry {
    relative: PathBuf,
    full: PathBuf,
    server: ServerKey,
    mount: PathBuf,
}

impl CacheEntry {
    pub fn new(rel_path: &Path, server: ServerKey, full_path: &Path, mount: &Path) -> Self {
        CacheEntry {
            relative: rel_path.to_path_buf(),
            full: full_path.to_path_buf(),
            server: server.clone(),
            mount: mount.to_path_buf(),
        }
    }

    /// Convert a relative filepath to be prepended with this mount instead.
    pub fn convert(&self, path: &Path) -> Result<PathBuf, ServerKey> {}
}

impl FileNetwork {
    pub fn new(mount_file: &str) -> Result<Self> {
        unimplemented!();
    }

    pub fn construct(map: HashMap<String, String>) -> Self {
        unimplemented!();
    }

    fn cached_location(&self, filename: &Path) -> Option<CacheEntry> {
        unimplemented!();
    }

    pub fn network_speed(&self, machine1: ServerKey, machine2: ServerKey) -> Option<u32> {
        unimplemented!();
    }

    pub fn find_match(
        &self,
        filestream: &mut FileStream,
        pwd: &PathBuf,
    ) -> Option<(PathBuf, String)> {
        unimplemented!();
    }
}
