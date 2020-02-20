extern crate dash;
use dash::graph::filestream::FileStream;
use dash::graph::Location;
use dash::util::Result;
use failure::bail;
use glob::glob;
use nom::types::CompleteByteSlice;
use nom::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::path::Path;
use std::path::PathBuf;
use std::*;
use tracing::debug;
/// Map of mount to IP addresses
/// TODO: it might be that multiple mounts are on multiple IPs? need better way of encoding
/// configuration information
pub struct FileMap {
    map: HashMap<PathBuf, String>,
    cache: HashMap<PathBuf, CacheEntry>, // caches the location of certain directories + full path
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq, Default)]
pub struct CacheEntry {
    pub rel_path: PathBuf,
    pub ip: String,
    pub full_path: PathBuf,
    pub mount: PathBuf,
}
impl CacheEntry {
    pub fn new(rel_path: &Path, ip: String, full_path: &Path, mount: &Path) -> Self {
        CacheEntry {
            rel_path: rel_path.to_path_buf(),
            ip: ip.clone(),
            full_path: full_path.to_path_buf(),
            mount: mount.to_path_buf(),
        }
    }
}

named_complete!(
    parse_file_info<(&str, &str)>,
    do_parse!(
        folder: map!(take_until!(":"), |n: CompleteByteSlice| {
            str::from_utf8(n.0).unwrap()
        }) >> tag!(":")
            >> ip: map!(rest, |n: CompleteByteSlice| {
                str::from_utf8(n.0).unwrap()
            })
            >> (folder, ip)
    )
);

fn in_mount(filename: &Path, mount: &Path) -> bool {
    filename.starts_with(mount)
}

impl FileMap {
    pub fn cached_location(&self, filename: PathBuf) -> Option<CacheEntry> {
        // ideally stores the relative filepaths.
        for (dir, entry) in self.cache.iter() {
            if filename.as_path().starts_with(Path::new(dir)) {
                return Some(entry.clone());
            }
        }
        return None;
    }

    // lol why is this string
    pub fn construct(map: HashMap<String, String>) -> Self {
        let mut filemap: HashMap<PathBuf, String> = HashMap::new();
        for (mount, ip) in map.iter() {
            filemap.insert(Path::new(mount.as_str()).to_path_buf(), ip.clone());
        }
        FileMap {
            map: filemap,
            cache: HashMap::default(),
        }
    }

    pub fn new(mount_info: &str) -> Result<Self> {
        let mut ret: HashMap<PathBuf, String> = HashMap::default();
        let file = File::open(mount_info)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line_src = line?;
            let (file, ip) = match parse_file_info(CompleteByteSlice(line_src.as_ref())) {
                Ok(b) => b.1,
                Err(e) => {
                    bail!("line {:?} failed with {:?}", line_src, e.to_string());
                }
            };
            ret.insert(Path::new(file).to_path_buf(), ip.to_string());
        }
        Ok(FileMap {
            map: ret,
            cache: HashMap::default(),
        })
    }

    /// TODO: here for backwards compatibility
    /// Can remove eventually
    pub fn find_match_str(&self, filename: &Path) -> Option<(PathBuf, String)> {
        // first, canonicalize the path
        for (mount, ip) in self.map.iter() {
            if in_mount(filename, mount) {
                return Some((mount.clone(), ip.clone()));
            }
        }
        None
    }

    /// Checks which mount the filename resolves to (if any)
    pub fn find_match(
        &mut self,
        filestream: &mut FileStream,
        pwd: &PathBuf,
    ) -> Option<(PathBuf, String)> {
        // first, check if path is cached anywhere
        match self.cached_location(filestream.get_path()) {
            Some(entry) => {
                // for now, we want to replace the string for that substring in the path we have,
                // and replace it with the full path saved for this sub folder
                let cached_full = self.cache.get(&entry.rel_path).unwrap();
                let rel_path = Path::new(&entry.rel_path);
                let cur_loc = filestream.get_path();
                let cur_relative = cur_loc
                    .as_path()
                    .strip_prefix(rel_path)
                    .expect("Not a prefix");
                let mut result = Path::new(&cached_full.full_path).to_path_buf();
                result.push(cur_relative);
                filestream.set_path(&result);
                let mount = entry.mount.clone();
                let ip = entry.ip.clone();
                return Some((mount, ip));
            }
            None => {}
        }
        let mut new_cache_entry = CacheEntry::default();
        // store the name of the old relative path passed in
        let mut old_path = filestream.get_path();
        if !old_path.as_path().is_dir() {
            old_path.pop();
        }
        new_cache_entry.rel_path = old_path.to_path_buf();
        // first, canonicalize the path
        match filestream.dash_cannonicalize(pwd) {
            Ok(_) => {}
            Err(e) => {
                debug!("Could not cannonicalize path: {:?}", e);
            }
        }
        let mut new_path = filestream.get_path();
        if !new_path.as_path().is_dir() {
            new_path.pop();
        }
        new_cache_entry.full_path = new_path;

        for (mount, ip) in self.map.iter() {
            if in_mount(&filestream.get_path(), &mount) {
                // add this into the cache
                new_cache_entry.mount = mount.clone();
                new_cache_entry.ip = ip.clone();
                self.cache
                    .insert(new_cache_entry.rel_path.clone(), new_cache_entry);
                return Some((mount.clone(), ip.clone()));
            }
        }
        None
    }

    /// Check which mount the pwd resolves to, if any
    pub fn find_current_dir_match(&self, pwd: &PathBuf) -> Option<(PathBuf, String)> {
        for (mount, ip) in self.map.iter() {
            if Path::new(pwd).starts_with(Path::new(mount)) {
                return Some((mount.clone(), ip.clone()));
            }
        }
        None
    }

    pub fn get_mount(&self, ip: &str) -> Option<PathBuf> {
        for (mount, ip_addr) in self.map.iter() {
            if ip == ip_addr {
                return Some(mount.clone());
            }
        }
        return None;
    }

    /// Used to resolve any filestream arguments that might contain a pattern the resulting list of
    /// multiple files.
    /// TODO: This *only* resolves the pattern with local or absolute paths -- so to do this
    /// correctly, the pwd must be set to the correct thing.
    /// TODO: In addition, this won't handle if the pattern includes an environment variable is
    /// included. Or maybe it will? Who knows?
    /// TODO: This function assumes that the client sees a filesystem view of the underlying files
    ///  -- but if later, Dash wanted to work with a *ssh* backend, this would need to change.
    pub fn resolve_filestream_with_pattern(
        &self,
        filestream: &mut FileStream,
    ) -> Result<Vec<FileStream>> {
        let mut res: Vec<FileStream> = Vec::new();
        let filename = filestream.get_name()?;
        for entry in glob(&filename).expect("Failed to read glob pattern") {
            match entry {
                Ok(path) => {
                    let name = match path.to_str() {
                        Some(n) => Path::new(n).to_path_buf(),
                        None => bail!("Could not turn path: {:?} to string", path),
                    };
                    res.push(FileStream::new_with_mode(
                        name,
                        filestream.get_mode(),
                        filestream.get_location(),
                    ));
                }
                Err(e) => {
                    bail!("One of the paths is an error: {:?}", e);
                }
            }
        }
        // TODO: does no pattern return no replacements?
        if res.len() == 0 {
            res.push(filestream.clone());
        }
        Ok(res)
    }

    /// Modifies the filestream to be remote if necessary.
    pub fn resolve_filestream(&mut self, filestream: &mut FileStream, pwd: &PathBuf) -> Result<()> {
        // first, see if there's an environment variable
        // Note: won't work on filestreams with patterns
        let name = filestream.get_name()?;
        if name.starts_with("$") {
            let var_name = name.split_at(1).1.to_string();
            match env::var(var_name) {
                Ok(val) => {
                    filestream.set_path(&Path::new(&val).to_path_buf());
                }
                Err(e) => {
                    bail!(
                        "Couldn't find environment variable {:?}: {:?}",
                        filestream.get_name(),
                        e
                    );
                }
            }
        }
        match filestream.get_location() {
            Location::Client => match self.find_match(filestream, pwd) {
                Some((mount, ip)) => {
                    filestream.set_location(Location::Server(ip));
                    filestream.strip_prefix(&mount)?;
                    Ok(())
                }
                None => Ok(()),
            },
            Location::Server(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // This could be a thing to ask Sadjad about
    #[test]
    fn test_parse_mount() {
        let (_, tup): (CompleteByteSlice, (&str, &str)) =
            parse_file_info(CompleteByteSlice(b"/mod/foo:127.0.0.1")).unwrap();

        assert_eq!(String::from(tup.0), String::from("/mod/foo"));
        assert_eq!(String::from(tup.1), String::from("127.0.0.1"));
    }

    #[test]
    fn test_in_mount_basic() {
        assert_eq!(in_mount(Path::new("/d/c/b/a"), Path::new("/d/c")), true);
        assert_eq!(in_mount(Path::new("/d/c/b/a"), Path::new("/f/e")), false);
    }

    #[test]
    fn test_find_match_basic() {
        let mut map: HashMap<PathBuf, String> = HashMap::default();
        map.insert(PathBuf::from("/d/c/b/a"), "127.0.0.1".to_string());
        map.insert(PathBuf::from("/e/c/b/a"), "127.0.0.2".to_string());
        let filemap = FileMap {
            map: map,
            cache: HashMap::default(),
        };

        assert_eq!(
            filemap.find_match_str(Path::new("/d/c/b/a/0")),
            Some((
                PathBuf::from("/d/c/b/a".to_string()),
                "127.0.0.1".to_string()
            ))
        );
        assert_eq!(
            filemap.find_match_str(Path::new("/e/c/b/a/0")),
            Some((
                PathBuf::from("/e/c/b/a".to_string()),
                "127.0.0.2".to_string()
            ))
        );
        assert_eq!(filemap.find_match_str(Path::new("/f/c/b/a/0")), None);
    }
}
