extern crate dash;
use dash::graph::{stream, Location};
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
use stream::FileStream;
use tracing::debug;
/// Map of mount to IP addresses
pub struct FileMap {
    map: HashMap<String, String>,
    cache: HashMap<String, CacheEntry>, // caches the location of certain directories + full path
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq, Default)]
pub struct CacheEntry {
    pub rel_path: String,
    pub ip: String,
    pub full_path: String,
    pub mount: String,
}
impl CacheEntry {
    pub fn new(rel_path: &str, ip: String, full_path: &str, mount: &str) -> Self {
        CacheEntry {
            rel_path: rel_path.to_string(),
            ip: ip.clone(),
            full_path: full_path.to_string(),
            mount: mount.to_string(),
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

// Run FS Metadata to see if is_dir or is_file => and then cannonicalize
// If it can't be cannonicalized:
//  Mkdir -p the thing
//  Then run FS Metadata
//  Then run "pop" to get the TOP level dir and remove that -> it should be safe to remove this.
fn in_mount(filename: &str, mount: &str, _pwd: &PathBuf) -> bool {
    // path should already be cannonicalized
    return Path::new(filename).starts_with(Path::new(mount));
}

impl FileMap {
    pub fn cached_location(&self, filename: &str, _pwd: &PathBuf) -> Option<CacheEntry> {
        // ideally stores the relative filepaths.
        for (dir, entry) in self.cache.iter() {
            if Path::new(filename).starts_with(Path::new(dir)) {
                return Some(entry.clone());
            }
        }
        return None;
    }

    pub fn construct(map: HashMap<String, String>) -> Self {
        FileMap {
            map: map,
            cache: HashMap::default(),
        }
    }

    pub fn new(mount_info: &str) -> Result<Self> {
        let mut ret: HashMap<String, String> = HashMap::default();
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
            ret.insert(file.to_string(), ip.to_string());
        }
        Ok(FileMap {
            map: ret,
            cache: HashMap::default(),
        })
    }

    /// TODO: here for backwards compatibility
    /// Can remove eventually
    pub fn find_match_str(&self, filename: &str, pwd: &PathBuf) -> Option<(String, String)> {
        // first, canonicalize the path
        for (mount, ip) in self.map.iter() {
            if in_mount(&filename, &mount, pwd) {
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
    ) -> Option<(String, String)> {
        // first, check if path is cached anywhere
        //pub rel_path: String,
        //pub loc: Location,
        //pub full_path: String,
        //pub mount: String,
        // #[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq, Default)]
        match self.cached_location(&filestream.get_name(), pwd) {
            Some(entry) => {
                // for now, we want to replace the string for that substring in the path we have,
                // and replace it with the full path saved for this sub folder
                let cached_full = self.cache.get(&entry.rel_path).unwrap();
                let rel_path = Path::new(&entry.rel_path);
                let cur_loc = Path::new(&filestream.get_name()).to_path_buf();
                let cur_relative = cur_loc
                    .as_path()
                    .strip_prefix(rel_path)
                    .expect("Not a prefix");
                let mut result = Path::new(&cached_full.full_path).to_path_buf();
                result.push(cur_relative);
                filestream.set_name(result.to_str().unwrap());
                let mount = entry.mount.clone();
                let ip = entry.ip.clone();
                return Some((mount, ip));
            }
            None => {}
        }
        let mut new_cache_entry = CacheEntry::default();
        // store the name of the old relative path passed in
        let mut old_path = Path::new(&filestream.get_name()).to_path_buf();
        if !old_path.as_path().is_dir() {
            old_path.pop();
        }
        new_cache_entry.rel_path = old_path.to_str().unwrap().to_string();
        // first, canonicalize the path
        match filestream.dash_cannonicalize(pwd) {
            Ok(_) => {}
            Err(e) => {
                debug!("Could not cannonicalize path: {:?}", e);
            }
        }
        let mut new_path = Path::new(&filestream.get_name()).to_path_buf();
        if !new_path.as_path().is_dir() {
            new_path.pop();
        }
        new_cache_entry.full_path = new_path.to_str().unwrap().to_string();

        for (mount, ip) in self.map.iter() {
            if in_mount(&filestream.get_name(), &mount, pwd) {
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
    pub fn find_current_dir_match(&self, pwd: &PathBuf) -> Option<(String, String)> {
        for (mount, ip) in self.map.iter() {
            if Path::new(pwd).starts_with(Path::new(mount)) {
                return Some((mount.clone(), ip.clone()));
            }
        }
        None
    }

    pub fn get_mount(&self, ip: &str) -> Option<String> {
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
        for entry in glob(&filestream.get_name()).expect("Failed to read glob pattern") {
            match entry {
                Ok(path) => {
                    let name = match path.to_str() {
                        Some(n) => n.to_string(),
                        None => bail!("Could not turn path: {:?} to string", path),
                    };
                    res.push(FileStream::new_exact(
                        name,
                        filestream.get_location(),
                        filestream.get_mode(),
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
        if filestream.get_name().starts_with("$") {
            let var_name = filestream.get_name().split_at(1).1.to_string();
            match env::var(var_name) {
                Ok(val) => {
                    filestream.set_name(&val);
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
        assert_eq!(in_mount("/d/c/b/a", "/d/c"), true);
        assert_eq!(in_mount("/d/c/b/a", "/f/e"), false);
    }

    #[test]
    fn test_find_match_basic() {
        let mut map: HashMap<String, String> = HashMap::default();
        map.insert("/d/c/b/a".to_string(), "127.0.0.1".to_string());
        map.insert("/e/c/b/a".to_string(), "127.0.0.2".to_string());
        let filemap = FileMap { map: map };

        assert_eq!(
            filemap.find_match("/d/c/b/a/0"),
            Some(("/d/c/b/a".to_string(), "127.0.0.1".to_string()))
        );
        assert_eq!(
            filemap.find_match("/e/c/b/a/0"),
            Some(("/e/c/b/a".to_string(), "127.0.0.2".to_string()))
        );
        assert_eq!(filemap.find_match("/f/c/b/a/0"), None);
    }
}
