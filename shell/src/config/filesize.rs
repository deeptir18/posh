extern crate walkdir;
use super::network::FileNetwork;
use dash::graph::Location;
use dash::runtime::new_client::ShellClient;
use dash::util::Result;
use failure::bail;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// Actually query the system for file size related information.
pub struct QueryFileSize;

/// Query for file sizes offloaded.
pub struct OffloadQueryFileSize {
    client: ShellClient,
    config: FileNetwork,
}

impl OffloadQueryFileSize {
    pub fn new(client: ShellClient, config: FileNetwork) -> Self {
        OffloadQueryFileSize {
            client: client,
            config: config,
        }
    }
}

pub trait FileSize {
    fn file_size(&self, path: &Path) -> Result<u64>;
    fn is_dir(&self, path: &Path) -> bool;
    fn dir_size(&self, path: &Path) -> Result<u64>;
    fn query_file_list(&self, paths: &Vec<PathBuf>) -> Result<HashMap<PathBuf, u64>> {
        let mut ret: HashMap<PathBuf, u64> = HashMap::default();
        for path in paths.iter() {
            let size = match self.is_dir(path.as_path()) {
                true => self.dir_size(path.as_path())?,
                false => self.file_size(path.as_path())?,
            };
            ret.insert(path.clone(), size);
        }
        Ok(ret)
    }
}

impl FileSize for QueryFileSize {
    fn file_size(&self, path: &Path) -> Result<u64> {
        let metadata = path.metadata()?;
        Ok(metadata.len())
    }

    fn is_dir(&self, path: &Path) -> bool {
        path.is_dir()
    }
    fn dir_size(&self, path: &Path) -> Result<u64> {
        let total_size = WalkDir::new(path)
            .min_depth(1)
            .max_depth(10)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.metadata().ok())
            .filter(|metadata| metadata.is_file())
            .fold(0, |acc, m| acc + m.len());
        Ok(total_size)
    }
}

impl FileSize for OffloadQueryFileSize {
    fn file_size(&self, path: &Path) -> Result<u64> {
        let metadata = path.metadata()?;
        Ok(metadata.len())
    }

    fn is_dir(&self, path: &Path) -> bool {
        path.is_dir()
    }
    fn dir_size(&self, path: &Path) -> Result<u64> {
        let total_size = WalkDir::new(path)
            .min_depth(1)
            .max_depth(10)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.metadata().ok())
            .filter(|metadata| metadata.is_file())
            .fold(0, |acc, m| acc + m.len());
        Ok(total_size)
    }

    fn query_file_list(&self, paths: &Vec<PathBuf>) -> Result<HashMap<PathBuf, u64>> {
        let mut ret: HashMap<PathBuf, u64> = HashMap::default();
        let mut requests: HashMap<Location, Vec<PathBuf>> = HashMap::default();
        let mut dedup_paths: HashMap<(Location, PathBuf), PathBuf> = HashMap::default();
        for path in paths.iter() {
            let location = self.config.get_path_location(path.clone());
            match location {
                Location::Client => {
                    // find the size locally and add to the map
                    let size = match self.is_dir(path.as_path()) {
                        true => self.dir_size(path.as_path())?,
                        false => self.file_size(path.as_path())?,
                    };
                    ret.insert(path.clone(), size);
                    continue;
                }
                _ => {}
            }
            let stripped_path =
                self.config
                    .stripped_path(path.as_path(), &Location::Client, &location)?;
            dedup_paths.insert((location.clone(), stripped_path.clone()), path.clone());
            if requests.contains_key(&location) {
                let vec = requests.get_mut(&location).unwrap();
                vec.push(stripped_path);
            } else {
                let vec: Vec<PathBuf> = vec![stripped_path];
                requests.insert(location.clone(), vec);
            }
        }
        // use shell client to query the mounts for each file
        let sizes = self.client.stat_files(requests)?;
        for (location, size_request) in sizes.iter() {
            for (path, size) in size_request.sizes.iter() {
                let original_path = match dedup_paths.get(&(location.clone(), path.clone())) {
                    Some(p) => p,
                    None => {
                        bail!("Filesize mod: doesn't contain backwards mapping for (loc {:?}, path {:?})", location, path);
                    }
                };
                ret.insert(original_path.clone(), *size);
            }
        }

        Ok(ret)
    }
}
