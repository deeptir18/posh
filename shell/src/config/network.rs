extern crate nom;
extern crate yaml_rust;
use dash::graph::filestream::FileStream;
use dash::graph::Location;
use dash::util::Result;
use failure::bail;
use nom::types::CompleteByteSlice;
use nom::*;
use std::collections::HashMap;
use std::fs::read_to_string;
use std::path::{Path, PathBuf};
use std::str::from_utf8;
use yaml_rust::YamlLoader;
named_complete!(
    parse_client<Location>,
    map!(tag!("client"), |_| { Location::Client })
);

named_complete!(
    parse_server<Location>,
    map!(many1!(alt!(digit | tag!("."))), |elts: Vec<
        CompleteByteSlice,
    >| {
        let mut name = "".to_string();
        for elt in elts.iter() {
            let str_repr = from_utf8(elt.0).unwrap();
            name.push_str(str_repr);
        }
        Location::Server(name)
    })
);
named_complete!(
    parse_pair<(Location, Location)>,
    do_parse!(
        first_loc: alt!(parse_client | parse_server)
            >> tag!(",")
            >> second_loc: alt!(parse_client | parse_server)
            >> (first_loc, second_loc)
    )
);

named_complete!(
    parse_link_key<(Location, Location)>,
    delimited!(tag!("("), parse_pair, tag!(")"))
);

#[derive(PartialEq, Debug, Clone, Eq, Default)]
pub struct FileNetwork {
    /// map of local mounted paths to IP addresses
    path_to_addr: HashMap<PathBuf, ServerKey>,
    /// information about other servers (when they have NFS access)
    server_info: HashMap<ServerKey, ServerInfo>,
    /// Link speed information (topology information)
    links: HashMap<(Location, Location), u32>,
    /// list of servers
    locations: Vec<Location>,
}

#[derive(PartialEq, Debug, Clone, Hash, Eq, Default)]
pub struct ServerKey {
    pub ip: String,
}

/// Description of which servers can access which other mounts.
#[derive(PartialEq, Debug, Clone, Hash, Eq, Default)]
pub struct ServerInfo {
    /// Mounts this server can access via NFS. Assumes each server has 1 mount they can expose to
    /// others.
    /// TODO: this model is not exactly scalable.
    pub other_mounted_directories: Vec<(PathBuf, ServerKey)>,
    /// tmp directory,
    pub tmp_directory: PathBuf,
}

impl FileNetwork {
    pub fn new(mount_file: &str) -> Result<Self> {
        let mut path_to_addr: HashMap<PathBuf, ServerKey> = HashMap::default();
        let mut server_info: HashMap<ServerKey, ServerInfo> = HashMap::default();
        let mut links: HashMap<(Location, Location), u32> = HashMap::default();
        let file_str = read_to_string(Path::new(&mount_file))?;
        let yamls = match YamlLoader::load_from_str(&file_str) {
            Ok(docs) => docs,
            Err(e) => {
                bail!("Could not parse yaml config: {:?}", e);
            }
        };
        let yaml = &yamls[0];
        match yaml["mounts"].as_hash() {
            Some(map) => {
                for (key, value) in map.iter() {
                    let mount = Path::new(&value.as_str().unwrap()).to_path_buf();
                    let ip = ServerKey {
                        ip: key.as_str().unwrap().to_string(),
                    };
                    path_to_addr.insert(mount, ip);
                }
            }
            None => {
                bail!("Config file contains no info under mounts");
            }
        }

        match yaml["links"].as_hash() {
            Some(map) => {
                for (key, value) in map.iter() {
                    let link_key =
                        parse_link_key(CompleteByteSlice(key.as_str().unwrap().as_bytes()))
                            .unwrap()
                            .1;
                    let speed: u32 = value.as_i64().unwrap() as u32;
                    links.insert(link_key, speed);
                }
            }
            None => {
                bail!("Config file contains no info under links");
            }
        }

        // TODO: add in parsing options for servers accessing other machines via NFS
        match yaml["tmp_directory"].as_hash() {
            Some(map) => {
                for (key, value) in map.iter() {
                    let ip = ServerKey {
                        ip: key.as_str().unwrap().to_string(),
                    };
                    let directory = Path::new(&value.as_str().unwrap()).to_path_buf();
                    let info = ServerInfo {
                        tmp_directory: directory,
                        other_mounted_directories: Vec::new(),
                    };
                    server_info.insert(ip, info);
                }
            }
            None => {
                bail!("Config file contains no tmp directory info");
            }
        }

        let mut servers: Vec<Location> = path_to_addr
            .iter()
            .map(|(_mt, server)| Location::Server(server.ip.clone()))
            .collect();
        servers.push(Location::Client);
        Ok(FileNetwork {
            path_to_addr: path_to_addr,
            server_info: server_info,
            links: links,
            locations: servers,
        })
    }

    pub fn construct(
        path_to_addr: HashMap<PathBuf, ServerKey>,
        links: HashMap<(Location, Location), u32>,
        server_info: HashMap<ServerKey, ServerInfo>,
    ) -> Self {
        let mut servers: Vec<Location> = path_to_addr
            .iter()
            .map(|(_mt, server)| Location::Server(server.ip.clone()))
            .collect();
        servers.push(Location::Client);
        FileNetwork {
            path_to_addr: path_to_addr,
            server_info: server_info,
            links: links,
            locations: servers,
        }
    }

    pub fn get_location_list(&self) -> Vec<Location> {
        self.locations.clone()
    }

    /// Queries for speed of link from machine1 to machine2
    pub fn network_speed(&self, machine1: &Location, machine2: &Location) -> Option<f64> {
        if machine1 == machine2 {
            return Some(std::f64::INFINITY);
        }
        match self.links.get(&(machine1.clone(), machine2.clone())) {
            Some(speed) => Some(*speed as f64),
            None => None,
        }
    }

    pub fn get_path_location(&self, path: PathBuf) -> Location {
        for (mount, serverkey) in self.path_to_addr.iter() {
            if path.starts_with(mount.as_path()) {
                return Location::Server(serverkey.ip.clone());
            }
        }
        return Location::Client;
    }

    /// Queries for where a certain file lives (origin filesystem).
    pub fn get_location(&self, filestream: &FileStream) -> Location {
        self.get_path_location(filestream.get_path())
    }

    pub fn stripped_path(
        &self,
        path: &Path,
        origin_location: &Location,
        new_location: &Location,
    ) -> Result<PathBuf> {
        let mut fs = FileStream::new(path, origin_location.clone());
        self.strip_file_path(&mut fs, origin_location, new_location)?;
        Ok(fs.get_path())
    }

    /// Strips the filestream of the correct path when serialized.
    pub fn strip_file_path(
        &self,
        filestream: &mut FileStream,
        origin_location: &Location,
        new_location: &Location,
    ) -> Result<()> {
        match origin_location {
            Location::Client => {
                // should be default currently, to call this with client
                for (mount, serverkey) in self.path_to_addr.iter() {
                    if filestream.get_path().starts_with(mount.as_path()) {
                        match new_location {
                            Location::Client => unreachable!(),
                            Location::Server(ip) => {
                                if *ip != serverkey.ip {
                                    bail!("New location {:?} of file, passed in, is not the prefix mount location {:?}", new_location, ip);
                                }
                                filestream.strip_prefix(mount.as_path())?;
                                return Ok(());
                            }
                        }
                    }
                }
                bail!(
                    "No prefix found in {:?} for {:?}",
                    origin_location,
                    new_location
                );
            }
            Location::Server(ip) => {
                // if another server has NFS access to the second server, can also check here
                match self.server_info.get(&ServerKey { ip: ip.clone() }) {
                    Some(info) => {
                        for (pathbuf, other_ip) in info.other_mounted_directories.iter() {
                            if filestream.get_path().starts_with(pathbuf.as_path()) {
                                match new_location {
                                    Location::Client => unreachable!(),
                                    Location::Server(ip) => {
                                        if *ip != other_ip.ip {
                                            bail!("New location {:?} of file, passed in, is not the prefix mount location {:?}", new_location, ip);
                                        }
                                        filestream.strip_prefix(pathbuf.as_path())?;
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        bail!(
                            "No prefix found in {:?} for {:?}",
                            origin_location,
                            new_location
                        );
                    }
                    None => {
                        bail!("Location {:?} has no mount info", origin_location);
                    }
                }
            }
        }
    }

    /// Gets a new tmp file in the desired location with that filestem.
    /// TODO: better naming scheme
    pub fn get_tmp(&self, stem: &Path, location: &Location) -> Result<PathBuf> {
        match location {
            Location::Client => unreachable!(),
            Location::Server(ip) => match self.server_info.get(&ServerKey { ip: ip.clone() }) {
                Some(info) => {
                    let mut pathbuf = info.tmp_directory.clone();
                    pathbuf.push(stem);
                    return Ok(pathbuf);
                }
                None => {
                    bail!("Server {:?} has no tmp directory", ip);
                }
            },
        }
    }
}
